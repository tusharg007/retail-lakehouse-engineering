[CmdletBinding()]
param(
  [Parameter(Position = 0, Mandatory = $true)]
  [ValidateSet('init','doctor','up','stop','status','logs','load','run','verify','s3-check','s3-upload')]
  [string]$Action,
  [int]$Sequence = 1
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$dockerCandidates = @(
  (Get-Command docker -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -ErrorAction SilentlyContinue),
  "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin\docker.exe",
  "$env:ProgramFiles\Docker\Docker\resources\bin\docker.exe"
) | Where-Object { $_ -and (Test-Path -LiteralPath $_) } | Select-Object -First 1
if (-not $dockerCandidates -and $Action -notin @('init')) { throw 'Docker CLI was not found. Restart PowerShell after Docker Desktop installation or add its CLI directory to PATH.' }
function Invoke-Compose([string[]]$Arguments) { & $dockerCandidates compose --project-directory $root @Arguments; if ($LASTEXITCODE) { throw "docker compose failed ($LASTEXITCODE)" } }
function Read-EnvKeys { if (Test-Path (Join-Path $root '.env')) { Get-Content (Join-Path $root '.env') | Where-Object { $_ -match '^[A-Za-z_][A-Za-z0-9_]*=' } | ForEach-Object { ($_ -split '=',2)[0] } } }
switch ($Action) {
  'init' {
    $target = Join-Path $root '.env'; $template = Join-Path $root '.env.example'
    if (-not (Test-Path $target)) { Copy-Item $template $target }
    $content = Get-Content $target -Raw
    $fernetBytes = New-Object byte[] 32; $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create(); $rng.GetBytes($fernetBytes); $rng.Dispose()
    $fernetKey = [Convert]::ToBase64String($fernetBytes).Replace('+','-').Replace('/','_')
    $generated = @{ 'SOURCE_DB_PASSWORD' = ([guid]::NewGuid().ToString('N')); 'AIRFLOW_DB_PASSWORD' = ([guid]::NewGuid().ToString('N')); 'AIRFLOW_FERNET_KEY' = $fernetKey; 'AIRFLOW_WEBSERVER_SECRET_KEY' = ([guid]::NewGuid().ToString('N')); 'AIRFLOW_ADMIN_PASSWORD' = ([guid]::NewGuid().ToString('N')) }
    foreach ($key in $generated.Keys) { if ($content -match "(?m)^$key=$") { $content = $content -replace "(?m)^$key=$", "$key=$($generated[$key])" } }
    Set-Content -LiteralPath $target -Value $content -NoNewline
    Write-Output 'Created or preserved root .env. Add AWS credential values and S3 configuration privately before S3 checks.'
  }
  'doctor' { Invoke-Compose @('config','--quiet'); Write-Output "Docker CLI: $dockerCandidates"; Write-Output ('Configured keys: ' + ((Read-EnvKeys) -join ', ')); Get-PSDrive -Name (Split-Path $root -Qualifier).TrimEnd(':') | Select-Object Used,Free }
  'up' { Invoke-Compose @('up','-d','--build','source-postgres','airflow-postgres','spark-thrift','airflow-init','airflow-webserver','airflow-scheduler') }
  'stop' { Invoke-Compose @('stop') }
  'status' { Invoke-Compose @('ps') }
  'logs' { Invoke-Compose @('logs','--tail','100') }
  'load' { Invoke-Compose @('run','--rm','airflow-scheduler','python','/opt/retail/walmart_dataset/load_data.py') }
  'run' { Invoke-Compose @('exec','airflow-scheduler','airflow','dags','trigger','retail_lakehouse') }
  's3-check' { Invoke-Compose @('run','--rm','airflow-scheduler','python','-m','pipelines.s3_files','check') }
  's3-upload' { Invoke-Compose @('run','--rm','airflow-scheduler','python','-m','pipelines.s3_upload','--sequence',$Sequence) }
  'verify' { powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $root 'scripts\audit_dataset.ps1'); Invoke-Compose @('run','--rm','airflow-scheduler','/opt/dbt-venv/bin/dbt','parse','--project-dir','/opt/retail/airflow_dbt_project/walmart_project','--profiles-dir','/opt/retail/airflow_dbt_project/walmart_project','--target-path','/opt/lakehouse/dbt-target','--log-path','/opt/lakehouse/dbt-logs') }
}
