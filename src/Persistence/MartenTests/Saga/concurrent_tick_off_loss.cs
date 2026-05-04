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

    [Fact(DisplayName = "BUG: parallel tick-offs against a single revisioned saga silently drop mutations")]
    public async Task parallel_tick_offs_silently_lose_mutations()
    {
        using var host = await BuildHostAsync(useMessagePartitioning: false);

        var (sagaId, registered, lost) = await DriveSagaAsync(host);

        _output.WriteLine($"Tick count: {TickCount}");
        _output.WriteLine($"Registered (Completed.Count): {registered}");
        _output.WriteLine($"Lost mutations:                 {lost}");

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
                    m.DisableNpgsqlLogging = true;
                    m.Connection(Servers.PostgresConnectionString);
                    m.DatabaseSchemaName = "tick_off_loss";
                    m.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
                    m.Schema.For<TickOffSaga>().UseNumericRevisions(true);
                }).IntegrateWithWolverine();

                opts.Services.AddResourceSetupOnStartup();

                opts.Discovery.DisableConventionalDiscovery().IncludeType<TickOffSaga>();

                opts.Policies.AutoApplyTransactions();
                opts.Policies.UseDurableInboxOnAllListeners();
                opts.Policies.UseDurableOutboxOnAllSendingEndpoints();

                // Same retry shape as the affected production service: an explicit
                // ConcurrencyException policy with a finite inline budget falling back
                // to ScheduleRetryIndefinitely.
                opts.Policies
                    .OnException<JasperFx.ConcurrencyException>()
                    .RetryWithCooldown(
                        50.Milliseconds(), 100.Milliseconds(), 200.Milliseconds(), 400.Milliseconds())
                    .Then.ScheduleRetryIndefinitely(1.Seconds(), 2.Seconds(), 4.Seconds());

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
    public Guid Id { get; set; }

    public HashSet<int> Outstanding { get; set; } = [];

    public HashSet<int> Completed { get; set; } = [];

    public int TotalTicks { get; set; }

    public List<TickPayload> Payload { get; set; } = [];

    public static (TickOffSaga, OutgoingMessages) Start(StartTickOffSaga message)
    {
        var saga = new TickOffSaga
        {
            Id = message.SagaId,
            TotalTicks = message.Count,
            Outstanding = [.. Enumerable.Range(0, message.Count)],
            Payload = [.. Enumerable.Range(0, message.Count).Select(i => new TickPayload(
                Id: Guid.NewGuid(),
                Index: i,
                Description: new string('x', 1024),
                Note: $"tick {i} payload — random {Guid.NewGuid()}",
                Tags: [.. Enumerable.Range(0, 8).Select(t => $"tag-{i}-{t}")]))],
        };
        return (saga, []);
    }

    public async Task<OutgoingMessages> Handle(TickOff message)
    {
        // Hold the load → mutate → save window open long enough that concurrently-
        // dispatched handlers genuinely overlap.
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        var index = message.Index;
        if (!Outstanding.Remove(index))
        {
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

public sealed record TickPayload(
    Guid Id,
    int Index,
    string Description,
    string Note,
    List<string> Tags);
