$ErrorActionPreference = "Stop"

$neo4jBat = $env:NEO4J_BAT

if ([string]::IsNullOrWhiteSpace($neo4jBat) -or -not (Test-Path $neo4jBat)) {
    throw "Set NEO4J_BAT to the Neo4j executable script, e.g. C:\path\to\neo4j\bin\neo4j.bat"
}

$neo4jBin = Split-Path $neo4jBat
$neo4jHome = Split-Path $neo4jBin
$neo4jPattern = [regex]::Escape($neo4jHome)

Write-Host "Stopping Neo4j..."

Get-CimInstance Win32_Process |
  Where-Object {
    $_.CommandLine -match $neo4jPattern -and
    $_.CommandLine -match "java"
  } |
  ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force
  }

Start-Sleep -Seconds 10

Write-Host "Starting Neo4j..."

Start-Process `
  -FilePath $neo4jBat `
  -ArgumentList "console" `
  -WorkingDirectory $neo4jBin `
  -WindowStyle Minimized

Start-Sleep -Seconds 30