# This script reads a file containing Kafka topic names, one per line,
# and calls the Python script 'kafka_schema_inspector.py' for each one.

# Define the path to your topics file. Update if necessary.
$topicsFile = "topics.txt"

# Define the path to your Python script
$pythonScript = "kafka_schema_inspector.py"

# Define the full path to your Python executable
# Use the same path that works for you, e.g., 'C:\Python39\python.exe'
$pythonPath = "C:\Users\rafaelj\Development\Tools\schema\venv\Scripts\python.exe"

# Check if the topics file exists
if (-not (Test-Path -Path $topicsFile)) {
    Write-Host "Error: The topics file '$topicsFile' was not found." -ForegroundColor Red
    exit
}

# Read the file line by line
Get-Content -Path $topicsFile | ForEach-Object {
    $topicName = $_.Trim()

    # Skip empty lines or comments
    if ([string]::IsNullOrWhiteSpace($topicName) -or $topicName.StartsWith("#")) {
        return
    }

    Write-Host "==================================================" -ForegroundColor Green
    Write-Host "Starting inspection for topic: $topicName" -ForegroundColor Green
    Write-Host "==================================================" -ForegroundColor Green

    # Call the Python script with the current topic name.
    # You can add the --brokers or --num-messages options here if needed.
    & $pythonPath $pythonScript $topicName

    Write-Host ""
}

Write-Host "Topic inspection process complete." -ForegroundColor Green