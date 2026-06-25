$ErrorActionPreference = "Stop"

$fusekiDir = $env:FUSEKI_DIR

if ([string]::IsNullOrWhiteSpace($fusekiDir) -or -not (Test-Path $fusekiDir)) {
    throw "Set FUSEKI_DIR to the Apache Jena Fuseki directory, e.g. C:\path\to\apache-jena-fuseki-6.0.0"
}

Write-Host "Stopping Fuseki..."

$fusekiPattern = [regex]::Escape($fusekiDir)

Get-CimInstance Win32_Process |
  Where-Object {
    ($_.Name -eq "java.exe" -or $_.Name -eq "javaw.exe" -or $_.Name -eq "cmd.exe") -and
    (($_.CommandLine -match "fuseki") -or ($_.CommandLine -match $fusekiPattern))
  } |
  ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force
  }

Start-Sleep -Seconds 10

Write-Host "Starting Fuseki..."

Start-Process `
  -FilePath "cmd.exe" `
  -ArgumentList "/c", "`"$fusekiDir\fuseki-server.bat`"" `
  -WorkingDirectory $fusekiDir `
  -WindowStyle Minimized

Start-Sleep -Seconds 30