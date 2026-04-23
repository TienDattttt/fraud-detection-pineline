Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:ProjectRoot = Split-Path -Parent $PSScriptRoot
$datasetPath = Join-Path $script:ProjectRoot "data\paysim_transactions.csv"

function Write-Step {
    param(
        [string]$Number,
        [string]$Message
    )

    Write-Host ""
    Write-Host "[$Number] $Message"
}

function Test-DockerExec {
    param(
        [string[]]$Arguments
    )

    & docker exec @Arguments *> $null
    return $LASTEXITCODE -eq 0
}

function Test-HttpEndpoint {
    param(
        [string]$Url
    )

    try {
        $null = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
        return $true
    }
    catch {
        return $false
    }
}

function Wait-UntilReady {
    param(
        [string]$Name,
        [scriptblock]$Probe,
        [int]$DelaySeconds = 3
    )

    Write-Host -NoNewline "  $Name..."
    while (-not (& $Probe)) {
        Write-Host -NoNewline "."
        Start-Sleep -Seconds $DelaySeconds
    }
    Write-Host " OK"
}

function Get-HdfsSafeModeStatus {
    $output = & docker exec namenode hdfs dfsadmin -safemode get 2>$null
    if ($LASTEXITCODE -ne 0) {
        return $null
    }
    return ($output | Out-String).Trim()
}

function Wait-HdfsWritable {
    Write-Host -NoNewline "  HDFS safemode..."
    while ($true) {
        $status = Get-HdfsSafeModeStatus
        if ($status -and $status -match "Safe mode is OFF") {
            break
        }
        Write-Host -NoNewline "."
        Start-Sleep -Seconds 2
    }
    Write-Host " OK"
}

Push-Location $script:ProjectRoot
try {
    Write-Host "================================="
    Write-Host " Fraud Detection Pipeline Startup"
    Write-Host "================================="

    Write-Step "1/5" "Starting Docker containers..."
    docker compose up --build -d

    Write-Step "2/5" "Waiting for services..."
    Wait-UntilReady -Name "Kafka" -Probe {
        Test-DockerExec -Arguments @(
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092",
            "--list"
        )
    }
    Wait-UntilReady -Name "HDFS" -Probe {
        Test-DockerExec -Arguments @(
            "namenode",
            "hdfs", "dfs", "-ls", "/"
        )
    }
    Wait-UntilReady -Name "Redis" -Probe {
        Test-DockerExec -Arguments @(
            "redis",
            "redis-cli", "ping"
        )
    } -DelaySeconds 2
    Wait-UntilReady -Name "Spark" -Probe {
        Test-HttpEndpoint -Url "http://localhost:8081/"
    }

    Write-Step "3/5" "Setting up HDFS directories..."
    Wait-HdfsWritable
    docker exec namenode hdfs dfs -mkdir -p /datalake/bronze
    docker exec namenode hdfs dfs -mkdir -p /datalake/silver
    docker exec namenode hdfs dfs -mkdir -p /datalake/gold
    docker exec namenode hdfs dfs -mkdir -p /datalake/checkpoints/streaming
    docker exec namenode hdfs dfs -chmod -R 777 /datalake
    Write-Host "  HDFS directories created"
    if (Test-DockerExec -Arguments @("namenode", "hdfs", "dfs", "-test", "-d", "/datalake/transactions")) {
        Write-Host "  Legacy directory detected: /datalake/transactions (unused by current Medallion flow)"
    }

    Write-Step "4/5" "Creating Kafka topic..."
    docker exec kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --create --topic transactions `
        --partitions 3 `
        --replication-factor 1 `
        --if-not-exists
    Write-Host "  Topic 'transactions' ready"

    Write-Step "5/5" "Checking dataset..."
    if (Test-Path $datasetPath) {
        $dataset = Get-Item -LiteralPath $datasetPath
        $sizeMb = [Math]::Round($dataset.Length / 1MB, 1)
        Write-Host "  Dataset found: $sizeMb MB"
    }
    else {
        Write-Host "  Dataset not found!"
        Write-Host "  Run: python scripts/download_dataset.py"
    }

    Write-Host ""
    Write-Host "================================="
    Write-Host " All services running!"
    Write-Host "================================="
    Write-Host ""
    Write-Host "  Kafka:       localhost:9092"
    Write-Host "  HDFS UI:     http://localhost:9870"
    Write-Host "  Spark UI:    http://localhost:8081"
    Write-Host "  API:         http://localhost:8000"
    Write-Host "  API Docs:    http://localhost:8000/docs"
    Write-Host ""
    Write-Host "Next steps:"
    Write-Host "  1. Train model (if not done):"
    Write-Host "     docker exec spark-master spark-submit /opt/spark/work/spark/train_model.py"
    Write-Host ""
    Write-Host "  2. Start streaming:"
    Write-Host "     docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.3.0 /opt/spark/work/spark/streaming_pipeline.py"
    Write-Host ""
    Write-Host "  3. Start producer:"
    Write-Host "     docker exec spark-master python3 /opt/spark/work/producer/kafka_producer.py --speed 0.5"
}
finally {
    Pop-Location
}
