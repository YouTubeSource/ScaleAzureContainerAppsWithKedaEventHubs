using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Diagnostics;

// Arguments: total event count, events per batch, pause between batches (ms).
// Each batch targets the next partition to make the tutorial repeatable.
int ReadInt(int index, int fallback, int min, int max) =>
    args.Length <= index ? fallback :
    int.TryParse(args[index], out var value) && value >= min && value <= max ? value :
    throw new ArgumentException($"Argument {index + 1} must be between {min} and {max}.");
string Required(string name) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value
    ? value : throw new InvalidOperationException($"Set {name}.");
var count = ReadInt(0, 1200, 1, 10_000_000);
var batchSize = ReadInt(1, 20, 1, 1000);
var pauseMs = ReadInt(2, 1000, 0, 60_000);
var partitionIds = (Environment.GetEnvironmentVariable("EVENTHUB_PARTITION_IDS") ?? "0,1,2,3")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
if (partitionIds.Length == 0) throw new ArgumentException("Set at least one partition ID.");
using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; shutdown.Cancel(); };
await using var producer = new EventHubProducerClient(
    Required("EVENTHUB_CONNECTION_STRING"), Required("EVENTHUB_NAME"));
var runId = Guid.NewGuid().ToString("N");
var elapsed = Stopwatch.StartNew();
var sent = 0;
var batchIndex = 0;
try
{
    while (sent < count)
    {
        var partition = partitionIds[batchIndex++ % partitionIds.Length];
        using var batch = await producer.CreateBatchAsync(
            new CreateBatchOptions { PartitionId = partition }, shutdown.Token);
        while (batch.Count < batchSize && sent + batch.Count < count)
        {
            var number = sent + batch.Count + 1;
            var data = new EventData(BinaryData.FromObjectAsJson(new
            {
                eventId = $"{runId}-{number}", runId, number,
                createdUtc = DateTimeOffset.UtcNow,
                message = "KEDA scaling demo"
            }));
            if (!batch.TryAdd(data))
            {
                if (batch.Count == 0) throw new InvalidOperationException("Event is too large.");
                break;
            }
        }
        await producer.SendAsync(batch, shutdown.Token);
        sent += batch.Count;
        Console.WriteLine($"Sent={sent}/{count} Partition={partition} Batch={batch.Count} RunId={runId}");
        if (sent < count && pauseMs > 0) await Task.Delay(pauseMs, shutdown.Token);
    }
}
catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
{
    Console.WriteLine("Stopped by user.");
}
Console.WriteLine($"Confirmed sends={sent}; seconds={elapsed.Elapsed.TotalSeconds:F1}; RunId={runId}");
