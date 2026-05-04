using IntegrationTests;
using JasperFx;
using JasperFx.Core;
using JasperFx.Resources;
using Marten;
using Marten.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Wolverine;
using Wolverine.ErrorHandling;
using Wolverine.Marten;
using Wolverine.Runtime;
using Wolverine.Runtime.Handlers;
using Xunit;
using Xunit.Abstractions;

namespace MartenTests.Saga;

/// <summary>
/// Reproduction for a saga state-loss scenario observed in production:
///
/// When a single saga instance receives N tick-off messages in parallel, the saga's
/// per-message handler loads the saga doc, mutates it, and saves. With
/// <see cref="IRevisioned"/> on the saga doc, Marten's optimistic concurrency check is
/// expected to throw <see cref="JasperFx.ConcurrencyException"/> on losing concurrent
/// commits, and the per-chain retry-and-reschedule policy is supposed to bring the loser
/// back through. In practice some tick-offs land successfully but a subset of the
/// mutations are silently dropped — the saga's <c>Outstanding</c> set never empties,
/// its <c>Completed</c> set is missing entries, no envelopes go to the dead-letter queue,
/// and the saga is permanently stuck.
///
/// The first test demonstrates the loss with the same exception-policy stack the affected
/// production system runs. The second test demonstrates that
/// <c>MessagePartitioning.UseInferredMessageGrouping()</c> serialises same-saga messages
/// onto a single partition slot and prevents the loss entirely — confirming the bug is in
/// the parallel-commit path, not in the saga handler logic.
///
/// Run order matters only insofar as both tests reuse the schema; the database is
/// configured to <c>CreateOrUpdate</c> at startup so reruns are idempotent.
/// </summary>
public class concurrent_tick_off_loss : PostgresqlContext
{
    private const int TickCount = 50;
    private const string TenantId = "tenant-tick-off";
    private static readonly TimeSpan WaitForCompletion = TimeSpan.FromMinutes(2);

    private readonly ITestOutputHelper _output;

    public concurrent_tick_off_loss(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact(DisplayName = "DUMP: print the generated TickOff saga handler source")]
    public async Task dump_generated_handler_source()
    {
        using var host = await BuildHostAsync(useMessagePartitioning: false);

        var runtime = (WolverineRuntime)host.Services.GetRequiredService<IWolverineRuntime>();
        var graph = runtime.Handlers;

        // Force compile by resolving handlers — SourceCode is null until the chain compiles.
        var tickOffHandler = (MessageHandler)graph.HandlerFor<TickOff>()!;
        var startHandler = (MessageHandler)graph.HandlerFor<StartTickOffSaga>()!;

        var dumpPath = "/tmp/tick_off_saga_handler_codegen.txt";
        using var writer = new StreamWriter(dumpPath);

        writer.WriteLine($"=== Chain for {startHandler.Chain!.MessageType.FullName} ===");
        writer.WriteLine(startHandler.Chain.SourceCode ?? "<no source>");
        writer.WriteLine();
        writer.WriteLine($"=== Chain for {tickOffHandler.Chain!.MessageType.FullName} ===");
        writer.WriteLine(tickOffHandler.Chain.SourceCode ?? "<no source>");
        writer.WriteLine();
        _output.WriteLine($"Wrote {dumpPath}");
    }

    [Fact(DisplayName = "BUG: parallel tick-offs against a single revisioned saga silently drop mutations")]
    public async Task parallel_tick_offs_silently_lose_mutations()
    {
        TickOffSaga.HandleInvocationCount = 0;
        TickOffSaga.HandleEarlyReturnCount = 0;
        marten_update_revision_probe.PendingOperationsLogger.Captured.Clear();
        marten_update_revision_probe.PendingOperationsLogger.BeforeSaveCount = 0;
        marten_update_revision_probe.PendingOperationsLogger.AfterCommitCount = 0;

        using var host = await BuildHostAsync(useMessagePartitioning: false);

        var (sagaId, registered, lost) = await DriveSagaAsync(host);

        _output.WriteLine($"Tick count:                    {TickCount}");
        _output.WriteLine($"Handle invocations:            {TickOffSaga.HandleInvocationCount}");
        _output.WriteLine($"  of which early-returned:     {TickOffSaga.HandleEarlyReturnCount}");
        _output.WriteLine($"Registered (Completed.Count):  {registered}");
        _output.WriteLine($"Lost mutations:                {lost}");

        var captures = marten_update_revision_probe.PendingOperationsLogger.Captured.ToArray();
        var byType = captures
            .GroupBy(c => c)
            .OrderByDescending(g => g.Count())
            .Select(g => $"  {g.Count()}x: {g.Key}")
            .ToArray();
        _output.WriteLine($"Pending-op snapshots captured: {captures.Length}");
        _output.WriteLine($"BeforeSaveChangesAsync count:  {marten_update_revision_probe.PendingOperationsLogger.BeforeSaveCount}");
        _output.WriteLine($"AfterCommitAsync count:        {marten_update_revision_probe.PendingOperationsLogger.AfterCommitCount}");
        foreach (var line in byType.Take(20))
        {
            _output.WriteLine(line);
        }
        // Also dump to a stable file so the captures survive xUnit's per-line truncation.
        System.IO.File.WriteAllLines("/tmp/pending_ops_capture.txt", captures);

        // The bug: with parallel tick-offs against a single saga, some mutations are
        // silently lost. The retry policy never fires the rescheduled retry that would
        // recover them. Without a fix, the saga sits permanently with Outstanding > 0.
        registered.ShouldBe(TickCount);
    }

    [Fact(DisplayName = "FIX: MessagePartitioning.UseInferredMessageGrouping serialises same-saga messages and prevents loss")]
    public async Task message_partitioning_prevents_tick_off_loss()
    {
        using var host = await BuildHostAsync(useMessagePartitioning: true);

        var (sagaId, registered, lost) = await DriveSagaAsync(host);

        _output.WriteLine($"Tick count: {TickCount}");
        _output.WriteLine($"Registered (Completed.Count): {registered}");
        _output.WriteLine($"Lost mutations:                 {lost}");

        registered.ShouldBe(TickCount);
        lost.ShouldBe(0);
    }

    private static async Task<IHost> BuildHostAsync(bool useMessagePartitioning)
    {
        return await Host.CreateDefaultBuilder()
            .UseWolverine(opts =>
            {
                opts.Services.AddMarten(m =>
                {
                    // Npgsql logging is intentionally enabled so this run logs
                    // every UPDATE/INSERT against the saga table. We grep the
                    // log for `update tick_off_loss.mt_doc_tickoffsaga` to see
                    // how many actual SQL UPDATEs reach the database vs. how
                    // many handler invocations Wolverine logs as successful.
                    m.DisableNpgsqlLogging = true;
                    m.Connection(Servers.PostgresConnectionString);
                    m.DatabaseSchemaName = "tick_off_loss";
                    m.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
                    m.Listeners.Add(new marten_update_revision_probe.PendingOperationsLogger());
                    // Do NOT call UseNumericRevisions here — Wolverine.Marten's
                    // MartenIntegration sets it automatically for all Saga types
                    // and also pins the revision member to Saga.Version. Setting
                    // it manually is redundant and risks fighting with that policy.
                }).IntegrateWithWolverine();

                opts.Services.AddResourceSetupOnStartup();

                opts.Discovery.DisableConventionalDiscovery().IncludeType<TickOffSaga>();

                opts.Policies.AutoApplyTransactions();
                opts.Policies.UseDurableInboxOnAllListeners();
                opts.Policies.UseDurableOutboxOnAllSendingEndpoints();

                // Differential probe: catch ANY exception (not just
                // ConcurrencyException) and move it straight to DLQ. If the
                // ConcurrencyException-typed policy was missing some failures
                // because they throw a different type, this catch-all should
                // route every contended commit to the DLQ.
                opts.Policies.OnAnyException().MoveToErrorQueue();

                if (useMessagePartitioning)
                {
                    // The fix: tell Wolverine to derive a partition group id from the
                    // saga id and route same-saga messages to a single local partition.
                    // With this enabled, no two tick-off messages for the same saga ever
                    // execute concurrently, so the optimistic-concurrency window can't
                    // open.
                    opts.MessagePartitioning
                        .ByMessage<ITickOffSagaMessage>(m => m.SagaId.ToString())
                        .PublishToPartitionedLocalMessaging("tick-off-saga", 4, topology =>
                        {
                            topology.MessagesImplementing<ITickOffSagaMessage>();
                            topology.ConfigureQueues(queue =>
                            {
                                queue.UseDurableInbox();
                            });
                        });
                }
            })
            .StartAsync();
    }

    private static async Task<(Guid SagaId, int Registered, int Lost)> DriveSagaAsync(IHost host)
    {
        var sagaId = Guid.NewGuid();

        // 1. Start the saga with TickCount outstanding indices.
        await host.MessageBus().InvokeAsync(
            new StartTickOffSaga(sagaId, TickCount),
            new DeliveryOptions { TenantId = TenantId });

        // 2. Fan out TickCount tick-off messages with the same SagaId, in parallel, to
        // force optimistic-concurrency collisions on the saga doc's revision check.
        await Parallel.ForEachAsync(
            Enumerable.Range(0, TickCount),
            new ParallelOptions { MaxDegreeOfParallelism = TickCount },
            async (i, ct) =>
            {
                await host.MessageBus().PublishAsync(
                    new TickOff(sagaId, i),
                    new DeliveryOptions
                    {
                        TenantId = TenantId,
                        SagaId = sagaId.ToString(),
                    });
            });

        // 3. Wait for the saga to either reach Outstanding.Count == 0 or for the
        // scheduled-retry budget to drain (whichever comes first).
        await WaitForCompletionOrTimeoutAsync(host, sagaId, WaitForCompletion);

        // 4. Inspect the actual saga state.
        var store = host.Services.GetRequiredService<IDocumentStore>();
        await using var session = store.LightweightSession(TenantId);
        var saga = await session.LoadAsync<TickOffSaga>(sagaId);

        if (saga is null)
        {
            // The saga hard-deleted on completion: every tick landed.
            return (sagaId, TickCount, 0);
        }

        return (sagaId, saga.Completed.Count, saga.Outstanding.Count);
    }

    private static async Task WaitForCompletionOrTimeoutAsync(IHost host, Guid sagaId, TimeSpan timeout)
    {
        var store = host.Services.GetRequiredService<IDocumentStore>();
        using var cts = new CancellationTokenSource(timeout);
        while (!cts.IsCancellationRequested)
        {
            await using var session = store.LightweightSession(TenantId);
            var saga = await session.LoadAsync<TickOffSaga>(sagaId, cts.Token);
            if (saga is null || saga.Outstanding.Count == 0)
            {
                return;
            }

            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);
            }
            catch (TaskCanceledException)
            {
                return;
            }
        }
    }
}

/// <summary>
/// Empirical probes for Marten's <c>UpdateRevision</c> semantics under contention.
/// Wolverine is not involved here — this drives Marten directly to confirm that
/// <c>UpdateRevision(doc, expected)</c> does in fact throw <see cref="JasperFx.ConcurrencyException"/>
/// when the stored revision has already moved past <c>expected - 1</c>.
/// </summary>
public class marten_update_revision_probe : PostgresqlContext
{
    private readonly ITestOutputHelper _output;

    public marten_update_revision_probe(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact(DisplayName = "PROBE: two concurrent UpdateRevision(doc, 2) — second commit must throw ConcurrencyException")]
    public async Task two_concurrent_update_revisions_against_same_expected_version()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.Connection(Servers.PostgresConnectionString);
            opts.DatabaseSchemaName = "tick_off_loss_probe";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Schema.For<TickOffSaga>().UseNumericRevisions(true);
        });

        var sagaId = Guid.NewGuid();

        // Seed a saga at version 1.
        await using (var seed = store.LightweightSession())
        {
            seed.Insert(new TickOffSaga
            {
                Id = sagaId,
                Outstanding = [1, 2],
                Completed = [],
                TotalTicks = 2,
            });
            await seed.SaveChangesAsync();
        }

        // Two sessions both load v=1, both call UpdateRevision(doc, 2). The first
        // SaveChanges should win and bump stored to v=2. The second SaveChanges must
        // throw ConcurrencyException — if Marten silently no-ops here, that is the bug.
        await using var sessionA = store.LightweightSession();
        await using var sessionB = store.LightweightSession();

        var sagaA = await sessionA.LoadAsync<TickOffSaga>(sagaId);
        var sagaB = await sessionB.LoadAsync<TickOffSaga>(sagaId);

        sagaA.ShouldNotBeNull();
        sagaB.ShouldNotBeNull();
        sagaA!.Version.ShouldBe(1);
        sagaB!.Version.ShouldBe(1);

        sagaA.Completed.Add(1);
        sagaA.Outstanding.Remove(1);
        sagaB.Completed.Add(2);
        sagaB.Outstanding.Remove(2);

        sessionA.UpdateRevision(sagaA, sagaA.Version + 1);
        sessionB.UpdateRevision(sagaB, sagaB.Version + 1);

        await sessionA.SaveChangesAsync();
        _output.WriteLine($"After A commit: stored Version expected = 2");

        Exception? caught = null;
        try
        {
            await sessionB.SaveChangesAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        _output.WriteLine($"Session B SaveChanges result: {(caught is null ? "NO EXCEPTION (silent no-op!)" : caught.GetType().FullName + ": " + caught.Message)}");

        // Inspect final state.
        await using var inspect = store.LightweightSession();
        var final = await inspect.LoadAsync<TickOffSaga>(sagaId);
        _output.WriteLine($"Final saga Version: {final!.Version}");
        _output.WriteLine($"Final Completed:   [{string.Join(",", final.Completed)}]");
        _output.WriteLine($"Final Outstanding: [{string.Join(",", final.Outstanding)}]");

        // The assertion that pins down the bug: B MUST have thrown ConcurrencyException.
        // If it didn't, Marten silently overwrote A's mutation — and that would be the
        // direct mechanism for the saga state loss we observe in production.
        caught.ShouldBeOfType<JasperFx.ConcurrencyException>();
    }

    [Fact(DisplayName = "PROBE: after load, does saga.Version == mt_version (Marten metadata) or == data.Version (stale in JSON)?")]
    public async Task saga_version_after_load_matches_mt_version_or_data_version()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.Connection(Servers.PostgresConnectionString);
            opts.DatabaseSchemaName = "tick_off_loss_probe";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Schema.For<TickOffSaga>().UseNumericRevisions(true);
        });

        var sagaId = Guid.NewGuid();

        // Insert at v=1 (initial), then perform 3 successful UpdateRevision commits
        // sequentially. After this, mt_version should be 4. What's saga.Version on
        // a fresh load?
        await using (var s = store.LightweightSession())
        {
            s.Insert(new TickOffSaga
            {
                Id = sagaId,
                Outstanding = [.. Enumerable.Range(0, 10)],
                Completed = [],
                TotalTicks = 10,
            });
            await s.SaveChangesAsync();
        }
        for (var i = 0; i < 3; i++)
        {
            await using var s = store.LightweightSession();
            var saga = await s.LoadAsync<TickOffSaga>(sagaId);
            saga!.Outstanding.Remove(i);
            saga.Completed.Add(i);
            s.UpdateRevision(saga, saga.Version + 1);
            await s.SaveChangesAsync();
        }

        // Now a fresh load: read saga.Version directly.
        await using var inspect = store.LightweightSession();
        var loaded = await inspect.LoadAsync<TickOffSaga>(sagaId);

        // Read mt_version directly from the DB.
        var conn = inspect.Connection!;
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select mt_version, data->>'Version' as data_version from tick_off_loss_probe.mt_doc_tickoffsaga where id = @id";
        var p = cmd.CreateParameter();
        p.ParameterName = "id";
        p.Value = sagaId;
        cmd.Parameters.Add(p);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        var mtVersion = reader.GetInt32(0);
        var dataVersion = reader.GetString(1);

        _output.WriteLine($"loaded.Version (in-memory after load): {loaded!.Version}");
        _output.WriteLine($"mt_version (Marten revision column):  {mtVersion}");
        _output.WriteLine($"data.Version (JSON property):         {dataVersion}");

        // The bug-or-not check: if loaded.Version != mt_version, then a parallel
        // handler that computes expectedSagaRevision = loaded.Version + 1 will
        // be requesting a revision that is ALREADY THE STORED VALUE — so the
        // UpdateRevision either silently no-ops (if Marten's WHERE is `<=`) or
        // throws ConcurrencyException without an actual contention.
        loaded.Version.ShouldBe(mtVersion);
    }

    [Fact(DisplayName = "PROBE: failing UpdateRevision in a batch with a raw SQL UPDATE — does the raw SQL persist when UpdateRevision is a no-op?")]
    public async Task raw_sql_command_persists_even_when_update_revision_no_ops()
    {
        // This probe mimics what Wolverine.Marten's FlushOutgoingMessagesOnCommit
        // session listener does: it queues a raw SQL UPDATE on the SAME session
        // alongside the saga's UpdateRevision. If Marten's batch commits the tx
        // BEFORE checking rowcounts and throwing ConcurrencyException, then the
        // raw SQL UPDATE could persist while the saga stays untouched — and the
        // exception (if any) reaches the caller AFTER the commit.

        var store = DocumentStore.For(opts =>
        {
            opts.Connection(Servers.PostgresConnectionString);
            opts.DatabaseSchemaName = "tick_off_loss_probe";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Schema.For<TickOffSaga>().UseNumericRevisions(true);
        });

        var sagaId = Guid.NewGuid();
        await using (var seed = store.LightweightSession())
        {
            seed.Insert(new TickOffSaga
            {
                Id = sagaId,
                Outstanding = [1, 2],
                Completed = [],
                TotalTicks = 2,
            });
            await seed.SaveChangesAsync();
        }

        // Create the side-effect table with a dedicated connection (Marten's
        // QueueSqlCommand rejects multi-statement SQL, so we can't piggy-back).
        await using (var ddlConn = new Npgsql.NpgsqlConnection(Servers.PostgresConnectionString))
        {
            await ddlConn.OpenAsync();
            await using var ddl1 = ddlConn.CreateCommand();
            ddl1.CommandText = "create table if not exists tick_off_loss_probe.probe_marker(id uuid primary key, marked_at timestamptz not null)";
            await ddl1.ExecuteNonQueryAsync();
            await using var ddl2 = ddlConn.CreateCommand();
            ddl2.CommandText = "delete from tick_off_loss_probe.probe_marker";
            await ddl2.ExecuteNonQueryAsync();
        }

        // Bump the saga's stored revision to 2 BEFORE we open the test session,
        // so our test session's UpdateRevision(saga, 2) is guaranteed to be a
        // no-op (stored 2, attempting 2 → WHERE 2 < 2 is false, 0 rows updated).
        await using (var bump = store.LightweightSession())
        {
            var s = await bump.LoadAsync<TickOffSaga>(sagaId);
            s!.Completed.Add(99);
            bump.UpdateRevision(s, s.Version + 1);
            await bump.SaveChangesAsync();
        }

        var probeMarkerId = Guid.NewGuid();

        await using var session = store.LightweightSession();
        var saga = await session.LoadAsync<TickOffSaga>(sagaId);
        saga!.Version.ShouldBe(2);

        // Mutate locally and queue UpdateRevision targeting expected=2 — will
        // be a no-op because stored is already 2 (we bumped it above).
        saga.Outstanding.Remove(1);
        saga.Completed.Add(1);
        session.UpdateRevision(saga, 2);

        // Queue a raw SQL UPDATE in the SAME batch — this is what
        // FlushOutgoingMessagesOnCommit.BeforeSaveChangesAsync does.
        session.QueueSqlCommand(
            "insert into tick_off_loss_probe.probe_marker(id, marked_at) values (?, now());",
            probeMarkerId);

        Exception? caught = null;
        try
        {
            await session.SaveChangesAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        _output.WriteLine($"SaveChanges threw: {caught?.GetType().FullName ?? "<no exception>"}");
        if (caught is not null)
        {
            _output.WriteLine($"Message: {caught.Message}");
        }

        // Now inspect: did the raw SQL persist anyway?
        await using var inspect = store.LightweightSession();
        var conn = inspect.Connection!;
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from tick_off_loss_probe.probe_marker where id = @id";
        var p = cmd.CreateParameter();
        p.ParameterName = "id";
        p.Value = probeMarkerId;
        cmd.Parameters.Add(p);
        var probeRow = await cmd.ExecuteScalarAsync();
        var rowCount = Convert.ToInt32(probeRow);

        _output.WriteLine($"Raw SQL UPDATE persisted: {rowCount > 0}");
        _output.WriteLine($"Saga still at version 2 (unchanged): {(await inspect.LoadAsync<TickOffSaga>(sagaId))!.Version == 2}");

        // The hypothesis under test: when UpdateRevision is a no-op AND a raw
        // SQL command was queued in the same batch, the raw SQL MUST be rolled
        // back together with the failed UpdateRevision. If the raw SQL persists,
        // that is a Marten batching bug and likely the saga loss mechanism.
        rowCount.ShouldBe(0);
        caught.ShouldBeOfType<JasperFx.ConcurrencyException>();
    }

    [Fact(DisplayName = "PROBE: failing UpdateRevision with raw SQL queued from a session listener BeforeSaveChangesAsync — does the listener's SQL persist?")]
    public async Task raw_sql_command_queued_from_session_listener_persists_when_update_revision_fails()
    {
        // Mirrors what Wolverine.Marten's FlushOutgoingMessagesOnCommit does:
        // it queues a raw SQL UPDATE inside the session-listener's
        // BeforeSaveChangesAsync hook, NOT inline before SaveChanges. If
        // listener-queued raw SQL is dispatched as a separate transaction
        // (or otherwise outside the batch that contains UpdateRevision),
        // it could persist even when UpdateRevision is a no-op.

        var store = DocumentStore.For(opts =>
        {
            opts.Connection(Servers.PostgresConnectionString);
            opts.DatabaseSchemaName = "tick_off_loss_probe";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Schema.For<TickOffSaga>().UseNumericRevisions(true);
        });

        var sagaId = Guid.NewGuid();
        await using (var seed = store.LightweightSession())
        {
            seed.Insert(new TickOffSaga
            {
                Id = sagaId,
                Outstanding = [1, 2],
                Completed = [],
                TotalTicks = 2,
            });
            await seed.SaveChangesAsync();
        }

        await using (var ddlConn = new Npgsql.NpgsqlConnection(Servers.PostgresConnectionString))
        {
            await ddlConn.OpenAsync();
            await using var ddl1 = ddlConn.CreateCommand();
            ddl1.CommandText = "create table if not exists tick_off_loss_probe.probe_marker(id uuid primary key, marked_at timestamptz not null)";
            await ddl1.ExecuteNonQueryAsync();
            await using var ddl2 = ddlConn.CreateCommand();
            ddl2.CommandText = "delete from tick_off_loss_probe.probe_marker";
            await ddl2.ExecuteNonQueryAsync();
        }

        // Bump stored to 2 so our test session's UpdateRevision(saga, 2) fails.
        await using (var bump = store.LightweightSession())
        {
            var s = await bump.LoadAsync<TickOffSaga>(sagaId);
            s!.Completed.Add(99);
            bump.UpdateRevision(s, s.Version + 1);
            await bump.SaveChangesAsync();
        }

        var probeMarkerId = Guid.NewGuid();

        await using var session = store.LightweightSession();
        // Attach a listener that queues a raw SQL INSERT in BeforeSaveChangesAsync —
        // mirroring FlushOutgoingMessagesOnCommit.BeforeSaveChangesAsync.
        session.Listeners.Add(new InsertProbeMarkerOnBeforeSave(probeMarkerId));

        var saga = await session.LoadAsync<TickOffSaga>(sagaId);
        saga!.Version.ShouldBe(2);
        saga.Outstanding.Remove(1);
        saga.Completed.Add(1);
        session.UpdateRevision(saga, 2);

        Exception? caught = null;
        try
        {
            await session.SaveChangesAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        _output.WriteLine($"SaveChanges threw: {caught?.GetType().FullName ?? "<no exception>"}");

        await using var inspect = store.LightweightSession();
        var conn = inspect.Connection!;
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from tick_off_loss_probe.probe_marker where id = @id";
        var p = cmd.CreateParameter();
        p.ParameterName = "id";
        p.Value = probeMarkerId;
        cmd.Parameters.Add(p);
        var rowCount = Convert.ToInt32(await cmd.ExecuteScalarAsync());

        _output.WriteLine($"Listener-queued raw SQL persisted: {rowCount > 0}");
        _output.WriteLine($"Saga still at version 2 (unchanged): {(await inspect.LoadAsync<TickOffSaga>(sagaId))!.Version == 2}");

        // Hypothesis: listener-queued raw SQL must roll back along with
        // UpdateRevision. If it persists, that's the saga loss mechanism.
        rowCount.ShouldBe(0);
        caught.ShouldBeOfType<JasperFx.ConcurrencyException>();
    }

    /// <summary>
    /// Document listener that captures, in <see cref="BeforeSaveChangesAsync"/>,
    /// the type and revision-property (if any) of every pending storage
    /// operation queued on the session. Used to verify whether the saga handler's
    /// session actually has an UpdateRevision-style op queued at SaveChanges
    /// time, or whether the op vanished / mutated into something else.
    /// </summary>
    public sealed class PendingOperationsLogger : Marten.DocumentSessionListenerBase
    {
        public static readonly System.Collections.Concurrent.ConcurrentBag<string> Captured = new();
        public static int BeforeSaveCount;
        public static int AfterCommitCount;

        public override Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
        {
            Interlocked.Increment(ref BeforeSaveCount);
            var ops = session.PendingChanges.Operations().ToArray();
            var summary = $"OPS_COUNT={ops.Length} | " +
                string.Join(" || ", ops.Select(o =>
                {
                    var t = o.GetType();
                    var revisionProp = t.GetProperty("Revision");
                    var revisionVal = revisionProp?.GetValue(o);
                    var docTypeProp = t.GetProperty("DocumentType");
                    var docTypeVal = docTypeProp?.GetValue(o);
                    return $"{t.Name}(DocType={docTypeVal?.ToString()?.Split('.').Last() ?? "?"}, Revision={revisionVal?.ToString() ?? "?"})";
                }));
            Captured.Add(summary);
            return Task.CompletedTask;
        }

        public override Task AfterCommitAsync(IDocumentSession session, Marten.Services.IChangeSet commit, CancellationToken token)
        {
            Interlocked.Increment(ref AfterCommitCount);
            return Task.CompletedTask;
        }
    }

    private sealed class InsertProbeMarkerOnBeforeSave : Marten.DocumentSessionListenerBase
    {
        private readonly Guid _markerId;

        public InsertProbeMarkerOnBeforeSave(Guid markerId)
        {
            _markerId = markerId;
        }

        public override Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
        {
            session.QueueSqlCommand(
                "insert into tick_off_loss_probe.probe_marker(id, marked_at) values (?, now())",
                _markerId);
            return Task.CompletedTask;
        }
    }

    [Fact(DisplayName = "PROBE: stress mt_upsert race — does AfterCommit ever exceed actual UPDATEs?")]
    public async Task mt_upsert_race_under_load_can_silently_succeed_without_persisting()
    {
        // Goal: demonstrate (or rule out) that Marten's mt_upsert_<sagaType>
        // function can return non-zero (and therefore make Marten fire
        // AfterCommitAsync) for a SaveChanges that did NOT actually update the
        // row. The function reads current_version, then runs INSERT…ON
        // CONFLICT…DO UPDATE WHERE revision > mt_version, then SELECTs
        // final_version. If a concurrent tx bumps mt_version between the
        // first SELECT and the ON CONFLICT WHERE, the UPDATE silently
        // skips while final_version is read as the concurrently-bumped
        // value — non-zero, no error.
        //
        // This is a standalone Marten probe. Wolverine is not in the loop.

        const int Parallelism = 50;
        const int Iterations = 5;

        var listener = new PendingOperationsLogger();
        var store = DocumentStore.For(opts =>
        {
            opts.Connection(Servers.PostgresConnectionString);
            opts.DatabaseSchemaName = "tick_off_loss_probe";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Schema.For<TickOffSaga>().UseNumericRevisions(true);
            opts.Listeners.Add(listener);
        });

        for (var iter = 0; iter < Iterations; iter++)
        {
            // Fresh saga per iteration so each iteration has the same starting
            // condition (a saga at v=1 with N outstanding indices).
            var sagaId = Guid.NewGuid();
            await using (var seed = store.LightweightSession())
            {
                seed.Insert(new TickOffSaga
                {
                    Id = sagaId,
                    Outstanding = [.. Enumerable.Range(0, Parallelism)],
                    Completed = [],
                    TotalTicks = Parallelism,
                });
                await seed.SaveChangesAsync();
            }

            PendingOperationsLogger.BeforeSaveCount = 0;
            PendingOperationsLogger.AfterCommitCount = 0;

            // All N sessions load the saga at v=1 sequentially so they ALL see
            // the same starting Version=1 and request expectedRevision=2.
            var sessions = new IDocumentSession[Parallelism];
            for (var i = 0; i < Parallelism; i++)
            {
                sessions[i] = store.LightweightSession();
                var s = (await sessions[i].LoadAsync<TickOffSaga>(sagaId))!;
                s.Version.ShouldBe(1);
                s.Outstanding.Remove(i);
                s.Completed.Add(i);
                sessions[i].UpdateRevision(s, 2);
            }

            // Race the SaveChanges in parallel. Mimic Wolverine's per-handler
            // jitter (Task.Delay before save) so the race window between the
            // function's first SELECT and its ON CONFLICT WHERE is exercised.
            var results = new (int Index, Exception? Error)[Parallelism];
            await Parallel.ForEachAsync(
                Enumerable.Range(0, Parallelism),
                new ParallelOptions { MaxDegreeOfParallelism = Parallelism },
                async (i, ct) =>
                {
                    try
                    {
                        await Task.Delay(Random.Shared.Next(0, 10), ct);
                        await sessions[i].SaveChangesAsync(ct);
                        results[i] = (i, null);
                    }
                    catch (Exception ex)
                    {
                        results[i] = (i, ex);
                    }
                });

            for (var i = 0; i < Parallelism; i++)
            {
                await sessions[i].DisposeAsync();
            }

            // Inspect actual saga state.
            await using var inspect = store.LightweightSession();
            var final = await inspect.LoadAsync<TickOffSaga>(sagaId);

            var successes = results.Count(r => r.Error is null);
            var concurrencyFailures = results.Count(r => r.Error is JasperFx.ConcurrencyException);
            var actualMutations = final!.Completed.Count;
            var beforeSave = PendingOperationsLogger.BeforeSaveCount;
            var afterCommit = PendingOperationsLogger.AfterCommitCount;

            _output.WriteLine(
                $"iter={iter}: SaveChanges successes={successes}, ConcurrencyExceptions={concurrencyFailures}, " +
                $"BeforeSave={beforeSave}, AfterCommit={afterCommit}, actual saga mutations={actualMutations}, " +
                $"final mt_version={final.Version}");

            // The hypothesis under test: AfterCommit > actualMutations means
            // Marten's SaveChanges reported success for a SaveChanges that
            // did not actually persist. If this happens at the bare-Marten
            // level, that confirms the race is in mt_upsert_*, not in
            // Wolverine's saga handler chain.
            if (afterCommit > actualMutations)
            {
                _output.WriteLine($"  *** RACE REPRODUCED at the bare Marten level: AfterCommit={afterCommit}, actual mutations={actualMutations} ***");
            }
        }
    }

    [Fact(DisplayName = "PROBE: N concurrent UpdateRevision(doc, 2) — exactly one wins, the rest throw ConcurrencyException")]
    public async Task n_concurrent_update_revisions_against_same_expected_version()
    {
        const int Parallelism = 32;

        var store = DocumentStore.For(opts =>
        {
            opts.Connection(Servers.PostgresConnectionString);
            opts.DatabaseSchemaName = "tick_off_loss_probe";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Schema.For<TickOffSaga>().UseNumericRevisions(true);
        });

        var sagaId = Guid.NewGuid();
        await using (var seed = store.LightweightSession())
        {
            seed.Insert(new TickOffSaga
            {
                Id = sagaId,
                Outstanding = [.. Enumerable.Range(0, Parallelism)],
                Completed = [],
                TotalTicks = Parallelism,
            });
            await seed.SaveChangesAsync();
        }

        // All N sessions load v=1 first (synchronously, sequentially), so they ALL hold a
        // snapshot at version 1 and will ALL try to UpdateRevision(saga, 2). Then we
        // SaveChanges in parallel. Exactly one should win and bump stored to v=2; the
        // other N-1 must throw ConcurrencyException. If any of them silently succeed
        // without throwing, that is the loss mechanism.
        var sessions = new IDocumentSession[Parallelism];
        var sagas = new TickOffSaga[Parallelism];
        for (var i = 0; i < Parallelism; i++)
        {
            sessions[i] = store.LightweightSession();
            sagas[i] = (await sessions[i].LoadAsync<TickOffSaga>(sagaId))!;
            sagas[i].Version.ShouldBe(1);
            sagas[i].Outstanding.Remove(i);
            sagas[i].Completed.Add(i);
            sessions[i].UpdateRevision(sagas[i], sagas[i].Version + 1);
        }

        var results = new (int Index, Exception? Error)[Parallelism];
        await Parallel.ForEachAsync(
            Enumerable.Range(0, Parallelism),
            new ParallelOptions { MaxDegreeOfParallelism = Parallelism },
            async (i, ct) =>
            {
                try
                {
                    await sessions[i].SaveChangesAsync(ct);
                    results[i] = (i, null);
                }
                catch (Exception ex)
                {
                    results[i] = (i, ex);
                }
            });

        for (var i = 0; i < Parallelism; i++)
        {
            await sessions[i].DisposeAsync();
        }

        var successes = results.Count(r => r.Error is null);
        var concurrencyFailures = results.Count(r => r.Error is JasperFx.ConcurrencyException);
        var otherFailures = results
            .Where(r => r.Error is not null and not JasperFx.ConcurrencyException)
            .Select(r => $"#{r.Index}: {r.Error!.GetType().FullName}: {r.Error.Message}")
            .ToArray();

        _output.WriteLine($"Parallelism: {Parallelism}");
        _output.WriteLine($"Successes: {successes}");
        _output.WriteLine($"ConcurrencyExceptions: {concurrencyFailures}");
        _output.WriteLine($"Other failures: {otherFailures.Length}");
        foreach (var msg in otherFailures.Take(5))
        {
            _output.WriteLine($"  {msg}");
        }

        await using var inspect = store.LightweightSession();
        var final = await inspect.LoadAsync<TickOffSaga>(sagaId);
        _output.WriteLine($"Final saga Version:  {final!.Version}");
        _output.WriteLine($"Final Completed:     [{string.Join(",", final.Completed.OrderBy(x => x))}]");
        _output.WriteLine($"Final Outstanding:   [{string.Join(",", final.Outstanding.OrderBy(x => x))}]");

        successes.ShouldBe(1);
        concurrencyFailures.ShouldBe(Parallelism - 1);
        otherFailures.ShouldBeEmpty();
        final.Version.ShouldBe(2);
        final.Completed.Count.ShouldBe(1);
    }
}

/// <summary>Common surface for the saga's messages so MessagePartitioning can derive the
/// group id (saga id) without relying on a <c>[SagaIdentity]</c> attribute or convention.</summary>
public interface ITickOffSagaMessage
{
    Guid SagaId { get; }
}

/// <summary>Starts a <see cref="TickOffSaga"/> with <paramref name="Count"/> indices outstanding.</summary>
public sealed record StartTickOffSaga(Guid SagaId, int Count) : ITickOffSagaMessage;

/// <summary>Tick-off message. Carries the saga id explicitly so it can be partitioned.</summary>
public sealed record TickOff(Guid SagaId, int Index) : ITickOffSagaMessage;

/// <summary>
/// Saga modelled on the Routit invoice-sync saga in the affected production system: a
/// list of outstanding indices, a parallel set of completed indices, and a payload of
/// nested data so JSON (de)serialisation per commit is non-trivial — this widens the
/// load → mutate → save window enough that concurrent tick-offs reliably collide on the
/// IRevisioned check.
/// </summary>
public class TickOffSaga : Wolverine.Saga, Marten.Metadata.IRevisioned
{
    /// <summary>Counts how many times <see cref="Handle"/> is actually invoked
    /// across all instances in the current process. If only 22 of 50 invocations
    /// happen, we know envelopes are being dropped before reaching the handler.</summary>
    public static int HandleInvocationCount;

    public static int HandleEarlyReturnCount;

    public Guid Id { get; set; }

    public HashSet<int> Outstanding { get; set; } = [];

    public HashSet<int> Completed { get; set; } = [];

    public int TotalTicks { get; set; }

    public static (TickOffSaga, OutgoingMessages) Start(StartTickOffSaga message)
    {
        var saga = new TickOffSaga
        {
            Id = message.SagaId,
            TotalTicks = message.Count,
            Outstanding = [.. Enumerable.Range(0, message.Count)],
        };
        return (saga, []);
    }

    public async Task<OutgoingMessages> Handle(TickOff message)
    {
        Interlocked.Increment(ref HandleInvocationCount);

        // Hold the load → mutate → save window open long enough that concurrently-
        // dispatched handlers genuinely overlap.
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        var index = message.Index;
        if (!Outstanding.Remove(index))
        {
            Interlocked.Increment(ref HandleEarlyReturnCount);
            return [];
        }

        Completed.Add(index);

        if (Outstanding.Count == 0)
        {
            MarkCompleted();
        }

        return [];
    }
}

