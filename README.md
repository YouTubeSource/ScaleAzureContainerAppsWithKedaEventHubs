# Build and run the .NET 10 applications

## Requirements

Install the .NET 10 SDK and Azure CLI on your computer. Use an editor that supports .NET 10. Docker is optional because Azure Container Registry can build the image. Use the Azure portal guide to create the Event Hub and private checkpoint container first.

Run these commands from the folder containing `EventHubs-KEDA.slnx`:

```bash
dotnet --version
dotnet restore EventHubs-KEDA.slnx
dotnet build EventHubs-KEDA.slnx -c Release --no-restore
```

The SDK version must start with `10.`. The included `global.json` permits later stable .NET 10 feature bands. The Event Hubs packages use version 5.12.2. The Worker also references Hosting 10.0.0. Update packages and scan dependencies as part of your normal maintenance process.

## Producer configuration

Use a NAMESPACE connection string from the `demo-send` policy. It should not contain `EntityPath`. The application supplies `EVENTHUB_NAME` separately. Do not record your screen while entering credentials.

Bash (Linux, macOS or WSL):

```bash
read -rsp 'Event Hubs Send connection string: ' EVENTHUB_CONNECTION_STRING
export EVENTHUB_CONNECTION_STRING
export EVENTHUB_NAME=demo-events
export EVENTHUB_PARTITION_IDS=0,1,2,3
# Warm-up: one event in each partition.
dotnet run --project src/Producer -- 4 1 0
# Load: 1,200 events, up to 20 per batch, one second between batches.
dotnet run --project src/Producer -- 1200 20 1000
```

PowerShell:

```powershell
$secret = Read-Host 'Event Hubs Send connection string' -AsSecureString
$env:EVENTHUB_CONNECTION_STRING = [System.Net.NetworkCredential]::new('', $secret).Password
$env:EVENTHUB_NAME = 'demo-events'
$env:EVENTHUB_PARTITION_IDS = '0,1,2,3'
dotnet run --project src/Producer -- 4 1 0
dotnet run --project src/Producer -- 1200 20 1000
```

Arguments are `totalEvents batchSize pauseBetweenBatchesMs`. The default load lasts roughly one minute, plus send overhead. Each batch goes to the next partition in the configured list. A fresh run gets a unique run ID. Press Ctrl+C to stop. Network errors fail visibly instead of claiming that every event was sent. A failed send can have an uncertain outcome, so real systems still need duplicate handling.

## Optional local worker smoke check

Open a SECOND terminal. Set its `EVENTHUB_CONNECTION_STRING` to the `demo-listen` namespace connection string, then set:

```bash
export EVENTHUB_NAME=demo-events
export CONSUMER_GROUP=aca-worker
export CHECKPOINT_CONTAINER=checkpoints
export PROCESSING_DELAY_MS=500
read -rsp 'Storage account connection string: ' STORAGE_CONNECTION_STRING
export STORAGE_CONNECTION_STRING
# Also securely set and export EVENTHUB_CONNECTION_STRING in this terminal.
dotnet run --project src/Worker
```

Send four warm-up events from the first terminal. The worker should log `OWN`, `PROCESSED` and `CHECKPOINT`. In Storage browser, confirm a checkpoint for each partition. Stop this local worker BEFORE running the Azure scaling demo. Otherwise it joins the same consumer group and takes partitions away from the Azure replicas.

## Build the container in Azure

Create the registry using the portal guide. From the project root, replace the registry placeholder and run:

```bash
az login
az account set --subscription '<subscription-id>'
az acr build --registry '<your-registry-name>' --image eventhub-worker:v1 --file src/Worker/Dockerfile .
```

The final `.` sends the project folder as build context. The Dockerfile uses .NET 10 SDK and runtime images. Copy the registry's actual login server from its Overview page for the image address; do not assume the hostname if Azure adds a suffix.

If Azure registry tasks are unavailable for your subscription, use Docker locally:

```bash
docker build --platform linux/amd64 -f src/Worker/Dockerfile -t '<login-server>/eventhub-worker:v1' .
az acr login --name '<your-registry-name>'
docker push '<login-server>/eventhub-worker:v1'
```