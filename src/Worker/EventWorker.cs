using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

public sealed class EventWorker(ILogger<EventWorker> logger) : BackgroundService
{
    // One simulated work slot PER REPLICA. Without this gate, a single processor
    // could delay concurrently on all four partitions, hiding the scaling benefit.
    private readonly SemaphoreSlim workSlot = new(1, 1);
    private readonly string replica = Environment.GetEnvironmentVariable("CONTAINER_APP_REPLICA_NAME")
        ?? Environment.MachineName;
    private static string Required(string name) =>
        Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value :
        throw new InvalidOperationException($"Set {name}.");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var delayText = Environment.GetEnvironmentVariable("PROCESSING_DELAY_MS") ?? "500";
        if (!int.TryParse(delayText, out var delayMs) || delayMs < 0 || delayMs > 60000)
            throw new InvalidOperationException("PROCESSING_DELAY_MS must be 0..60000.");
        var store = new BlobContainerClient(Required("STORAGE_CONNECTION_STRING"),
            Required("CHECKPOINT_CONTAINER"));
        // Provision this private blob container in the portal before starting.
        var processor = new EventProcessorClient(store, Required("CONSUMER_GROUP"),
            Required("EVENTHUB_CONNECTION_STRING"), Required("EVENTHUB_NAME"),
            new EventProcessorClientOptions { Identifier = replica });
        processor.PartitionInitializingAsync += e =>
        {
            // Used only when this partition has no saved checkpoint.
            e.DefaultStartingPosition = EventPosition.Earliest;
            logger.LogInformation("OWN Replica={Replica} Partition={Partition}", replica, e.PartitionId);
            return Task.CompletedTask;
        };
        processor.PartitionClosingAsync += e =>
        {
            logger.LogInformation("RELEASE Replica={Replica} Partition={Partition} Reason={Reason}",
                replica, e.PartitionId, e.Reason);
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += e =>
        {
            logger.LogError(e.Exception, "SDK_ERROR Replica={Replica} Partition={Partition} Operation={Operation}",
                replica, e.PartitionId, e.Operation);
            return Task.CompletedTask;
        };
        processor.ProcessEventAsync += async e =>
        {
            if (!e.HasEvent) return;
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, e.CancellationToken);
            var token = linked.Token;
            try
            {
                // Keep retrying THIS event. Returning after a failure would allow a
                // later checkpoint to move past it. A production app needs a durable
                // poison-event policy plus idempotent business operations.
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await workSlot.WaitAsync(token);
                        try
                        {
                            using var body = JsonDocument.Parse(e.Data.EventBody.ToString());
                            var eventId = body.RootElement.GetProperty("eventId").GetString();
                            await Task.Delay(delayMs, token); // Replace with real work.
                            logger.LogInformation("PROCESSED Replica={Replica} Partition={Partition} Sequence={Sequence} EventId={EventId}",
                                replica, e.Partition.PartitionId, e.Data.SequenceNumber, eventId);
                            // Modern SDK stores offset/sequence in blob metadata.
                            await e.UpdateCheckpointAsync(token);
                            logger.LogInformation("CHECKPOINT Replica={Replica} Partition={Partition} Sequence={Sequence}",
                                replica, e.Partition.PartitionId, e.Data.SequenceNumber);
                        }
                        finally { workSlot.Release(); }
                        break;
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "RETRY Replica={Replica} Partition={Partition} Sequence={Sequence}",
                            replica, e.Partition.PartitionId, e.Data.SequenceNumber);
                        await Task.Delay(TimeSpan.FromSeconds(5), token);
                    }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        };
        try
        {
            await processor.StartProcessingAsync(stoppingToken);
            logger.LogInformation("START Replica={Replica} DelayMs={DelayMs}", replica, delayMs);
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await processor.StopProcessingAsync(timeout.Token);
            logger.LogInformation("STOP Replica={Replica}", replica);
        }
    }
}
