<#
.SYNOPSIS
    Build, run, stop, clean and check the voltstream stack from PowerShell.

.DESCRIPTION
    The Windows counterpart of the Makefile, for machines without `make`. Works from any
    directory: every path is resolved from this script's location.

    Simulated time runs from the clock anchor in .env: one simulated day is 5 real minutes,
    starting at simulation.epoch_sim (2026-01-01). A fresh stack gets a new anchor. A stack
    that still holds data keeps its anchor, because a new one would restart simulated time
    at 2026-01-01 and write into days that were already billed.

    Commands:
      run      Wipe everything, build the images, start a fresh simulation.
      build    Build the three images (app, spark, airflow).
      start    Start the stack. Fresh stack: new clock. Existing data: resume on the old clock.
      stop     Stop the containers, keep all data. 'start' resumes.
      clean    Delete containers and ALL data (Kafka, Postgres, MinIO, Spark checkpoints).
      status   Simulated clock, container health and where the pipeline has got to.
      check    Verify every stage end to end (takes about 30 seconds).
      bill     Show one household's bill for a day, and which layer served it.
      logs     Follow one service's logs.

.EXAMPLE
    .\scripts\voltstream.ps1 run
.EXAMPLE
    .\scripts\voltstream.ps1 check -Date 2026-01-02
.EXAMPLE
    .\scripts\voltstream.ps1 bill -Household HH-0007 -Date 2026-01-02
.EXAMPLE
    .\scripts\voltstream.ps1 logs -Service speed-layer

.NOTES
    If Windows refuses to run the script, either run it as
        powershell -ExecutionPolicy Bypass -File .\scripts\voltstream.ps1 <command>
    or allow local scripts once:
        Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('help', 'build', 'start', 'run', 'stop', 'clean', 'status', 'check', 'bill', 'logs')]
    [string]$Command = 'help',

    # Simulated date (yyyy-MM-dd). check: defaults to the latest billed day.
    # bill: defaults to the current simulated day.
    [string]$Date,

    # Household for 'bill' and for the merge-function check.
    [string]$Household = 'HH-0001',

    # Compose service for 'logs', e.g. speed-layer, raw-archiver, meter-producer, api.
    [string]$Service,

    # Skip the confirmation before 'clean' or 'run' delete data.
    [switch]$Force
)

# Native tools (docker, psql, kafka CLIs) write progress to stderr; exit codes are checked
# explicitly instead of letting stderr abort the script.
$ErrorActionPreference = 'Continue'

$Root        = Split-Path -Parent $PSScriptRoot
# Invariant culture for every date and time: in .NET format strings ':' is the *culture's*
# time separator, and some cultures use '.', which would write an unparseable anchor.
$Inv         = [Globalization.CultureInfo]::InvariantCulture
$ComposeFile = Join-Path $Root 'docker\docker-compose.yml'
$EnvFile     = Join-Path $Root '.env'
$EnvExample  = Join-Path $Root '.env.example'
$BaseYaml    = Join-Path $Root 'config\base.yaml'

# What each service should look like once the stack is up.
$ExpectedServices = [ordered]@{
    'kafka'               = 'healthy'
    'postgres'            = 'healthy'
    'minio'               = 'healthy'
    'postgres-init'       = 'exited'
    'kafka-init'          = 'exited'
    'minio-init'          = 'exited'
    'meter-producer'      = 'running'
    'reference-dropper'   = 'running'
    'raw-archiver'        = 'running'
    'speed-layer'         = 'running'
    'api'                 = 'healthy'
    'docker-socket-proxy' = 'running'
    'airflow'             = 'running'
}

$script:Tally = @{ PASS = 0; WAIT = 0; WARN = 0; FAIL = 0 }

# ---------------------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------------------

function Read-BaseSetting([string]$Pattern, [string]$Default) {
    if (Test-Path $BaseYaml) {
        $match = [regex]::Match([IO.File]::ReadAllText($BaseYaml), $Pattern, 'Multiline')
        if ($match.Success) { return $match.Groups[1].Value }
    }
    return $Default
}

$TimeScale  = [int](Read-BaseSetting '^\s*time_scale:\s*(\d+)' '288')
$Households = [int](Read-BaseSetting '^\s*households:\s*(\d+)' '50')
$SimEpoch   = [DateTimeOffset]::Parse(
    (Read-BaseSetting '^\s*epoch_sim:\s*"([^"]+)"' '2026-01-01T00:00:00Z'),
    $Inv)
$SeedDay    = $SimEpoch.UtcDateTime.Date.AddDays(-1).ToString('yyyy-MM-dd', $Inv)

function Get-EnvValue([string]$Name, [string]$Default = '') {
    if (Test-Path $EnvFile) {
        foreach ($line in [IO.File]::ReadAllLines($EnvFile)) {
            if ($line -match ('^\s*' + [regex]::Escape($Name) + '=(.*)$')) {
                $value = $Matches[1].Trim()
                if ($value) { return $value }
            }
        }
    }
    return $Default
}

function Initialize-Settings {
    if (-not (Test-Path $EnvFile)) {
        Copy-Item $EnvExample $EnvFile
        Write-Host 'Created .env from .env.example (safe local defaults).' -ForegroundColor DarkGray
    }
    $script:PgUser      = Get-EnvValue 'POSTGRES_USER' 'voltstream'
    $script:PgDb        = Get-EnvValue 'POSTGRES_DB' 'voltstream'
    $script:MinioUser   = Get-EnvValue 'MINIO_ROOT_USER' 'voltstream'
    $script:MinioPass   = Get-EnvValue 'MINIO_ROOT_PASSWORD' 'voltstream-dev'
    $script:ApiPort     = Get-EnvValue 'API_HOST_PORT' '8000'
    $script:AirflowPort = Get-EnvValue 'AIRFLOW_HOST_PORT' '8080'
    $script:ConsolePort = Get-EnvValue 'MINIO_CONSOLE_HOST_PORT' '9001'
    $script:ArchiverPort = Get-EnvValue 'ARCHIVER_METRICS_PORT' '8011'
    $script:SpeedPort   = Get-EnvValue 'SPEED_LAYER_METRICS_PORT' '8012'
    $script:ImageTag    = Get-EnvValue 'VOLTSTREAM_IMAGE_TAG' 'local'
}

# ---------------------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------------------

function Write-Section([string]$Title) {
    Write-Host ''
    Write-Host $Title -ForegroundColor Cyan
}

function Write-Check([string]$Status, [string]$Label, [string]$Detail = '') {
    $colour = @{ PASS = 'Green'; WAIT = 'Yellow'; WARN = 'Yellow'; FAIL = 'Red'; INFO = 'Gray' }[$Status]
    Write-Host ('  {0,-7}' -f "[$Status]") -ForegroundColor $colour -NoNewline
    Write-Host $Label -NoNewline
    if ($Detail) { Write-Host "  $Detail" -ForegroundColor DarkGray } else { Write-Host '' }
    if ($script:Tally.ContainsKey($Status)) { $script:Tally[$Status]++ }
}

function Write-Summary {
    Write-Host ''
    Write-Host ('Summary: {0} passed, {1} waiting, {2} warnings, {3} failed' -f `
        $script:Tally.PASS, $script:Tally.WAIT, $script:Tally.WARN, $script:Tally.FAIL)
    if ($script:Tally.WAIT -gt 0) {
        Write-Host 'WAIT means "not expected yet at this point in the simulation" - run check again later.' -ForegroundColor DarkGray
    }
}

function Stop-WithError([string]$Message) {
    Write-Host $Message -ForegroundColor Red
    exit 1
}

# ---------------------------------------------------------------------------------------
# Docker, Postgres, MinIO, metrics
# ---------------------------------------------------------------------------------------

# A simple function (no [Parameter] attributes), so arguments like -d and --wait pass
# through to docker compose untouched instead of being parsed as PowerShell parameters.
function Invoke-Compose { & docker compose --env-file $EnvFile -f $ComposeFile @args }

function Assert-Docker {
    $null = & docker info --format '{{.ServerVersion}}' 2>$null
    if ($LASTEXITCODE -ne 0) {
        Stop-WithError 'Docker is not running. Start Docker Desktop, wait until it is ready, and try again.'
    }
}

function Test-StackHasData {
    $volumes = @(& docker volume ls -q 2>$null)
    return ($volumes -contains 'voltstream_postgres_data')
}

function Test-ImagesBuilt {
    foreach ($name in 'voltstream-app', 'voltstream-spark', 'voltstream-airflow') {
        $null = & docker image inspect "${name}:$($script:ImageTag)" 2>$null
        if ($LASTEXITCODE -ne 0) { return $false }
    }
    return $true
}

function Invoke-Sql([string]$Sql, [string]$Database = '') {
    if (-not $Database) { $Database = $script:PgDb }
    # One line: some Windows argument-passing paths mangle newlines inside an argument.
    $oneLine = ($Sql -replace '\s+', ' ').Trim()
    $out = & docker exec voltstream-postgres psql -U $script:PgUser -d $Database -tA -F '|' -c $oneLine 2>$null
    [pscustomobject]@{ Ok = ($LASTEXITCODE -eq 0); Rows = @($out | Where-Object { $_ -ne '' }) }
}

function Get-BucketListing([string]$Path) {
    # `mc` ships inside the object-store image (D8), so it runs in the server container
    # itself - no client image to pull. MC_HOST_<alias> gives it endpoint and credentials
    # without an 'alias set' step.
    $mcHost = "MC_HOST_local=http://$($script:MinioUser):$($script:MinioPass)@localhost:9000"
    $lines = @(& docker exec -e $mcHost voltstream-minio mc ls --recursive "local/$Path" 2>$null)
    [pscustomobject]@{ Ok = ($LASTEXITCODE -eq 0); Lines = $lines }
}

function Get-MetricsText([string]$Port) {
    try {
        return (Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 -Uri "http://localhost:$Port/metrics").Content
    } catch {
        return $null
    }
}

# Sum (or max) of every sample whose name and labels match $Pattern; $null if none does.
function Get-MetricValue([string]$Text, [string]$Pattern, [switch]$Max) {
    if (-not $Text) { return $null }
    $values = @()
    foreach ($m in [regex]::Matches($Text, "(?m)^$Pattern\s+(\S+)\s*$")) {
        $values += [double]::Parse($m.Groups[1].Value, $Inv)
    }
    if ($values.Count -eq 0) { return $null }
    if ($Max) { return ($values | Measure-Object -Maximum).Maximum }
    return ($values | Measure-Object -Sum).Sum
}

function Get-TopicEndOffset {
    $lines = @(& docker exec voltstream-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic meter.readings 2>$null)
    if ($LASTEXITCODE -ne 0 -or $lines.Count -eq 0) { return -1 }
    $total = 0L
    foreach ($line in $lines) { if ($line -match ':(\d+)$') { $total += [long]$Matches[1] } }
    return $total
}

function Get-NewestZoneWindow {
    $r = Invoke-Sql "SELECT coalesce(to_char(max(window_start) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI'), ''), count(*) FROM zone_metrics_rt"
    if (-not $r.Ok -or $r.Rows.Count -eq 0) { return $null }
    $parts = $r.Rows[0] -split '\|'
    [pscustomobject]@{ Newest = $parts[0]; Count = [int]$parts[1] }
}

function Get-AirflowPassword {
    $text = & docker exec voltstream-airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated 2>$null
    if ($LASTEXITCODE -eq 0 -and $text) { return ($text -join '') }
    return $null
}

# ---------------------------------------------------------------------------------------
# The simulated clock
# ---------------------------------------------------------------------------------------

function Set-Anchor {
    $anchor = [DateTime]::UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", $Inv)
    $text = [IO.File]::ReadAllText($EnvFile)
    $pattern = '(?m)^VOLTSTREAM_ANCHOR_REAL=[^\r\n]*'
    if ([regex]::IsMatch($text, $pattern)) {
        $text = [regex]::Replace($text, $pattern, "VOLTSTREAM_ANCHOR_REAL=$anchor")
    } else {
        if ($text.Length -gt 0 -and -not $text.EndsWith("`n")) { $text += "`n" }
        $text += "VOLTSTREAM_ANCHOR_REAL=$anchor`n"
    }
    # No byte-order mark: Compose would read one as part of the first key.
    [IO.File]::WriteAllText($EnvFile, $text, (New-Object System.Text.UTF8Encoding($false)))
    return $anchor
}

# The anchor the running containers actually use. $null: no producer container yet.
# Empty string: the producer was started without one, so every container has its own clock.
function Get-ContainerAnchor {
    $lines = @(& docker inspect voltstream-meter-producer --format '{{range .Config.Env}}{{println .}}{{end}}' 2>$null)
    if ($LASTEXITCODE -ne 0) { return $null }
    foreach ($line in $lines) {
        if ($line -match '^VOLTSTREAM__SIMULATION__ANCHOR_REAL=(.*)$') { return $Matches[1].Trim() }
    }
    return ''
}

function Get-SimClock([string]$AnchorText) {
    if (-not $AnchorText) { return $null }
    try {
        $anchor = [DateTimeOffset]::Parse($AnchorText, $Inv)
    } catch {
        return $null
    }
    $elapsed  = [DateTimeOffset]::UtcNow - $anchor
    $simNow   = $SimEpoch.AddTicks([long]($elapsed.Ticks * $TimeScale))
    $simToday = $simNow.UtcDateTime.Date
    $fraction = $simNow.UtcDateTime.TimeOfDay.TotalSeconds / 86400.0
    [pscustomobject]@{
        Anchor            = $anchor
        SimNow            = $simNow
        SimTodayText      = $simToday.ToString('yyyy-MM-dd', $Inv)
        SimYesterdayText  = $simToday.AddDays(-1).ToString('yyyy-MM-dd', $Inv)
        DayNumber         = [int](($simToday - $SimEpoch.UtcDateTime.Date).TotalDays) + 1
        DayPercent        = [int]($fraction * 100)
        SecondsToMidnight = [int][math]::Ceiling((1.0 - $fraction) * 86400.0 / $TimeScale)
    }
}

# The clock every status/check computation uses: the containers' own anchor when they
# have one, otherwise .env's.
function Get-EffectiveClock {
    $containerAnchor = Get-ContainerAnchor
    if ($containerAnchor) { return Get-SimClock $containerAnchor }
    return Get-SimClock (Get-EnvValue 'VOLTSTREAM_ANCHOR_REAL')
}

function Show-Clock([object]$Clock) {
    if (-not $Clock) {
        Write-Check 'WARN' 'Simulated clock' "no clock anchor found - start the stack with this script ('run' or 'start')"
        return
    }
    Write-Host ('  Anchor (real, UTC)   {0}' -f $Clock.Anchor.UtcDateTime.ToString('yyyy-MM-dd HH:mm:ss', $Inv))
    Write-Host ('  Simulated now        {0} UTC   day {1}, {2}% through, next simulated midnight in {3} s' -f `
        $Clock.SimNow.ToString('yyyy-MM-dd HH:mm', $Inv), $Clock.DayNumber, $Clock.DayPercent, $Clock.SecondsToMidnight)
}

function Test-ClockAgreement {
    $containerAnchor = Get-ContainerAnchor
    if ($null -eq $containerAnchor) { return }
    if (-not $containerAnchor) {
        Write-Check 'FAIL' 'Shared clock' 'containers were started without a clock anchor (without --env-file?), so each has its own clock. Use: run'
        return
    }
    $envAnchor = Get-EnvValue 'VOLTSTREAM_ANCHOR_REAL'
    if ($envAnchor -ne $containerAnchor) {
        Write-Check 'WARN' 'Shared clock' ".env says $envAnchor but the containers use $containerAnchor (times here use the containers')"
        return
    }
    Write-Check 'PASS' 'Shared clock' "every container anchored at $containerAnchor"
}

# ---------------------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------------------

function Get-ContainerStates {
    $rows = @(& docker compose --env-file $EnvFile -f $ComposeFile ps -a --format '{{.Service}}|{{.State}}|{{.Health}}|{{.ExitCode}}' 2>$null)
    $states = @{}
    foreach ($row in $rows) {
        $parts = $row -split '\|'
        if ($parts.Count -ge 4) {
            $states[$parts[0]] = [pscustomobject]@{ State = $parts[1]; Health = $parts[2]; ExitCode = $parts[3] }
        }
    }
    return $states
}

# Returns $true when at least one container exists.
function Show-Containers {
    $states = Get-ContainerStates
    if ($states.Count -eq 0) {
        Write-Check 'FAIL' 'Containers' "none - the stack is not running. Use: run (fresh) or start"
        return $false
    }
    foreach ($service in $ExpectedServices.Keys) {
        $want = $ExpectedServices[$service]
        $s = $states[$service]
        if (-not $s) { Write-Check 'FAIL' $service 'no container'; continue }
        $desc = $s.State
        if ($s.Health) { $desc += " ($($s.Health))" }
        switch ($want) {
            'healthy' {
                if ($s.Health -eq 'healthy') { Write-Check 'PASS' $service $desc }
                elseif ($s.State -eq 'running') { Write-Check 'WAIT' $service $desc }
                else { Write-Check 'FAIL' $service $desc }
            }
            'running' {
                if ($s.State -eq 'running') { Write-Check 'PASS' $service $desc }
                else { Write-Check 'FAIL' $service "$desc - see: .\scripts\voltstream.ps1 logs -Service $service" }
            }
            'exited' {
                if ($s.State -eq 'exited' -and $s.ExitCode -eq '0') { Write-Check 'PASS' $service 'finished (exit 0)' }
                elseif ($s.State -eq 'running') { Write-Check 'WAIT' $service 'still running' }
                else { Write-Check 'FAIL' $service "$($s.State), exit $($s.ExitCode)" }
            }
        }
    }
    return $true
}

# ---------------------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------------------

function Show-Urls {
    Write-Section 'Where to look'
    Write-Host "  Dashboard        http://localhost:$($script:ApiPort)/"
    Write-Host "  API docs         http://localhost:$($script:ApiPort)/docs"
    Write-Host "  Airflow          http://localhost:$($script:AirflowPort)/   (login: see 'status')"
    Write-Host "  MinIO console    http://localhost:$($script:ConsolePort)/   ($($script:MinioUser) / $($script:MinioPass))"
    Write-Host "  Spark metrics    http://localhost:$($script:ArchiverPort)/metrics (archiver), http://localhost:$($script:SpeedPort)/metrics (speed layer)"
}

function Invoke-Build {
    Write-Section 'Building images (the first build is slow: PySpark and the Airflow base image are large)'
    Invoke-Compose build
    if ($LASTEXITCODE -ne 0) { Stop-WithError 'Build failed - see the output above.' }
}

function Invoke-Start {
    Assert-Docker
    if (-not (Test-ImagesBuilt)) {
        Write-Host 'Images are not built yet - building them first, so build time does not count as simulated time.' -ForegroundColor DarkGray
        Invoke-Build
    }

    if (Test-StackHasData) {
        $anchor = Get-EnvValue 'VOLTSTREAM_ANCHOR_REAL'
        if (-not $anchor) {
            Stop-WithError "This stack has data from an earlier run but .env has no clock anchor. Run 'clean' first, or 'run' to clean, build and start."
        }
        Write-Host "Resuming on the existing clock ($anchor). Simulated time kept running while the stack was stopped, so expect a gap in the data." -ForegroundColor DarkGray
    } else {
        $anchor = Set-Anchor
        Write-Host "Fresh stack: clock anchored at $anchor, which is simulated $($SimEpoch.ToString('yyyy-MM-dd HH:mm', $Inv))." -ForegroundColor DarkGray
    }

    Write-Section 'Starting the stack (waiting on health checks, up to 10 minutes)'
    Invoke-Compose up -d --wait --wait-timeout 600
    if ($LASTEXITCODE -ne 0) {
        Write-Host 'Not every service became healthy in time:' -ForegroundColor Yellow
        Invoke-Compose ps -a
        Stop-WithError 'Find the failing service above, then: .\scripts\voltstream.ps1 logs -Service <name>'
    }

    Show-Urls
    Write-Host ''
    Write-Host 'Timeline: ~5-7 min first billing (partial day); ~10-13 min the first complete day is billed and reconciled.'
    Write-Host 'Next: .\scripts\voltstream.ps1 status   and later   .\scripts\voltstream.ps1 check'
}

function Invoke-Stop {
    Assert-Docker
    Invoke-Compose stop
    if ($LASTEXITCODE -ne 0) { Stop-WithError 'Stop failed - see the output above.' }
    Write-Host 'Containers stopped; all data kept. Resume with: .\scripts\voltstream.ps1 start' -ForegroundColor DarkGray
}

function Invoke-Clean {
    Assert-Docker
    if (-not $Force -and (Test-StackHasData)) {
        $answer = Read-Host "This deletes ALL data: Kafka, Postgres, MinIO and the Spark checkpoints. Type 'yes' to continue"
        if ($answer -ne 'yes') { Write-Host 'Cancelled.'; exit 0 }
    }
    Write-Section 'Removing containers and volumes'
    Invoke-Compose down -v --remove-orphans
    if ($LASTEXITCODE -ne 0) { Stop-WithError 'Clean failed - see the output above.' }
}

function Invoke-Run {
    Invoke-Clean
    Invoke-Build
    Invoke-Start
}

function Invoke-Status {
    Assert-Docker
    $clock = Get-EffectiveClock

    Write-Section 'Simulated clock'
    Show-Clock $clock
    Test-ClockAgreement

    Write-Section 'Containers'
    if (-not (Show-Containers)) { return }

    Write-Section 'Pipeline'
    $zones = Get-NewestZoneWindow
    if ($zones -and $zones.Count -gt 0) {
        Write-Check 'INFO' 'Speed layer' "$($zones.Count) zone windows in Postgres, newest starts $($zones.Newest) (simulated)"
    } else {
        Write-Check 'INFO' 'Speed layer' 'no zone windows yet - the first appear ~10-20 s after the producer starts'
    }

    $r = Invoke-Sql "SELECT coalesce(max(sim_date)::text, ''), count(*) FILTER (WHERE status = 'failed') FROM pipeline_runs WHERE layer = 'batch_billing' AND status IN ('success', 'failed')"
    $latest = ''
    if ($r.Ok -and $r.Rows.Count -gt 0) {
        $parts = $r.Rows[0] -split '\|'
        $latest = $parts[0]
        Write-Check 'INFO' 'Batch layer' ("latest billed day: {0}; failed billing runs: {1}" -f $(if ($latest) { $latest } else { 'none yet' }), $parts[1])
    }

    if ($clock) {
        if ($clock.DayNumber -le 1) {
            $next = "day 1 (partial) in progress; its billing starts ~1-3 min after simulated midnight, in $($clock.SecondsToMidnight) s"
        } elseif (-not $latest -or $latest -lt $clock.SimYesterdayText) {
            $next = "$($clock.SimYesterdayText) has closed; its billing should finish ~1-3 min after its midnight"
        } else {
            $next = "up to date through $latest; the next billing starts after midnight, in $($clock.SecondsToMidnight) s"
        }
        Write-Check 'INFO' 'Next' $next
    }

    $password = Get-AirflowPassword
    if ($password) {
        Write-Check 'INFO' 'Airflow login' "user/password: $password"
    } else {
        Write-Check 'INFO' 'Airflow login' "not generated yet - try again in a minute, or: .\scripts\voltstream.ps1 logs -Service airflow"
    }
}

function Invoke-Check {
    Assert-Docker
    $clock = Get-EffectiveClock

    # --- 1 ------------------------------------------------------------------------------
    Write-Section '1. Containers and the shared clock'
    if (-not (Show-Containers)) { Write-Summary; exit 1 }
    Test-ClockAgreement
    Show-Clock $clock

    # --- 2 ------------------------------------------------------------------------------
    Write-Section '2. Kafka - the event log'
    $topics = @(& docker exec voltstream-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe 2>$null)
    $main = $topics | Where-Object { $_ -match '^Topic:\s*meter\.readings\s' } | Select-Object -First 1
    $dlq  = $topics | Where-Object { $_ -match '^Topic:\s*meter\.readings\.dlq\s' } | Select-Object -First 1
    if ($main -and $main -match 'PartitionCount:\s*(\d+)') {
        $partitions = [int]$Matches[1]
        if ($partitions -eq 3) { Write-Check 'PASS' 'Topic meter.readings' '3 partitions (the parallelism ceiling per consumer group)' }
        else { Write-Check 'WARN' 'Topic meter.readings' "$partitions partitions, expected 3" }
    } else {
        Write-Check 'FAIL' 'Topic meter.readings' 'missing - check: logs -Service kafka-init'
    }
    if ($dlq) { Write-Check 'PASS' 'Topic meter.readings.dlq' 'present' } else { Write-Check 'FAIL' 'Topic meter.readings.dlq' 'missing' }

    Write-Host '  Sampling Kafka and both Spark jobs for 12 seconds...' -ForegroundColor DarkGray
    $offset0   = Get-TopicEndOffset
    $archiver0 = Get-MetricValue (Get-MetricsText $script:ArchiverPort) 'voltstream_events_consumed_total\{layer="archiver"\}'
    $speed0    = Get-MetricValue (Get-MetricsText $script:SpeedPort) 'voltstream_events_consumed_total\{layer="speed"\}'
    $window0   = Get-NewestZoneWindow
    Start-Sleep -Seconds 12
    $offset1       = Get-TopicEndOffset
    $archiverText  = Get-MetricsText $script:ArchiverPort
    $speedText     = Get-MetricsText $script:SpeedPort
    $archiver1     = Get-MetricValue $archiverText 'voltstream_events_consumed_total\{layer="archiver"\}'
    $speed1        = Get-MetricValue $speedText 'voltstream_events_consumed_total\{layer="speed"\}'
    $window1       = Get-NewestZoneWindow

    if ($offset0 -ge 0 -and $offset1 -gt $offset0) {
        Write-Check 'PASS' 'Producer publishing' ("{0} messages in ~12 s (50 meters every 2 s is ~300)" -f ($offset1 - $offset0))
    } elseif ($offset1 -ge 0) {
        Write-Check 'FAIL' 'Producer publishing' 'no new messages - see: logs -Service meter-producer'
    } else {
        Write-Check 'FAIL' 'Producer publishing' 'could not read topic offsets'
    }

    # --- 3 ------------------------------------------------------------------------------
    Write-Section '3. Reference data - the daily landing zone'
    $landing = Get-BucketListing 'voltstream-landing/tariff/'
    $tariffFiles = @($landing.Lines | Where-Object { $_ -match '\.csv$' } | ForEach-Object { ($_ -split '\s+')[-1] } | Sort-Object)
    if ($tariffFiles.Count -gt 0) {
        Write-Check 'PASS' 'Tariff files' ("{0} in voltstream-landing/tariff/, newest {1}" -f $tariffFiles.Count, $tariffFiles[-1])
    } elseif ($landing.Ok) {
        Write-Check 'WAIT' 'Tariff files' 'none yet - the dropper writes its first two within seconds of starting'
    } else {
        Write-Check 'FAIL' 'Tariff files' 'could not list the landing bucket'
    }

    # --- 4 ------------------------------------------------------------------------------
    Write-Section '4. Raw archiver - Kafka to the master dataset (Spark)'
    if ($null -ne $archiver1 -and $null -ne $archiver0 -and $archiver1 -gt $archiver0) {
        Write-Check 'PASS' 'Consuming' ("{0} records archived in ~12 s" -f ($archiver1 - $archiver0))
    } elseif ($null -ne $archiver1) {
        Write-Check 'WARN' 'Consuming' 'no new records in the sample - it may be between micro-batches; run check again'
    } else {
        Write-Check 'FAIL' 'Consuming' "metrics endpoint not answering on port $($script:ArchiverPort)"
    }
    $lag = Get-MetricValue $archiverText 'voltstream_consumer_lag\{layer="archiver",partition="\d+"\}' -Max
    if ($null -ne $lag) { Write-Check 'INFO' 'Consumer lag' ("{0} records behind, worst partition" -f $lag) }
    $raw = Get-BucketListing 'voltstream-raw/meter_readings/'
    $parquet = @($raw.Lines | Where-Object { $_ -match '\.parquet$' })
    if ($parquet.Count -gt 0) {
        $days = @($parquet | ForEach-Object { if ($_ -match 'sim_date=([0-9-]+)/') { $Matches[1] } } | Sort-Object -Unique)
        Write-Check 'PASS' 'Parquet files' ("{0} files under sim_date=/hour= partitions, days: {1}" -f $parquet.Count, ($days -join ', '))
    } else {
        Write-Check 'WAIT' 'Parquet files' 'none yet - the first land one trigger (10 s) after records arrive'
    }

    # --- 5 ------------------------------------------------------------------------------
    Write-Section '5. Speed layer - live views in Postgres (Spark)'
    if ($null -ne $speed1 -and $null -ne $speed0 -and $speed1 -gt $speed0) {
        Write-Check 'PASS' 'Consuming' ("{0} records validated in ~12 s" -f ($speed1 - $speed0))
    } elseif ($null -ne $speed1) {
        Write-Check 'WARN' 'Consuming' 'no new records in the sample - run check again'
    } else {
        Write-Check 'FAIL' 'Consuming' "metrics endpoint not answering on port $($script:SpeedPort)"
    }
    $lag = Get-MetricValue $speedText 'voltstream_consumer_lag\{layer="speed",partition="\d+"\}' -Max
    if ($null -ne $lag) { Write-Check 'INFO' 'Consumer lag' ("{0} records behind, slowest of its three queries" -f $lag) }
    $latencySum = Get-MetricValue $speedText 'voltstream_e2e_latency_seconds_sum\{layer="speed"\}'
    $latencyCount = Get-MetricValue $speedText 'voltstream_e2e_latency_seconds_count\{layer="speed"\}'
    if ($latencyCount -gt 0) {
        $avg = [math]::Round($latencySum / $latencyCount, 2)
        if ($avg -lt 60) { Write-Check 'PASS' 'End-to-end latency' "average $avg s, Kafka to the zone view in Postgres (NFR: under 60 s)" }
        else { Write-Check 'FAIL' 'End-to-end latency' "average $avg s, above the 60 s NFR" }
    }
    if ($window0 -and $window1 -and $window1.Newest) {
        if ($window1.Newest -gt $window0.Newest) {
            $minutes = ([datetime]::ParseExact($window1.Newest, 'yyyy-MM-dd HH:mm', $Inv) - [datetime]::ParseExact($window0.Newest, 'yyyy-MM-dd HH:mm', $Inv)).TotalMinutes
            Write-Check 'PASS' 'Zone windows advancing' ("newest window moved {0} simulated minutes in ~12 s (event time at {1}x)" -f $minutes, $TimeScale)
        } else {
            Write-Check 'WARN' 'Zone windows advancing' "newest window still $($window1.Newest) - run check again"
        }
    } else {
        Write-Check 'WAIT' 'Zone windows advancing' 'no zone windows yet'
    }
    if ($clock) {
        $r = Invoke-Sql "SELECT count(*), count(*) FILTER (WHERE tariff_source_date = sim_date - 1) FROM household_running_rt WHERE sim_date = '$($clock.SimTodayText)'"
        if ($r.Ok -and $r.Rows.Count -gt 0) {
            $parts = $r.Rows[0] -split '\|'
            $rows = [int]$parts[0]
            if ($rows -eq $Households -and [int]$parts[1] -eq $rows) {
                Write-Check 'PASS' 'Provisional bills' "$rows households for $($clock.SimTodayText), all costed on yesterday's tariff"
            } elseif ($rows -gt 0) {
                Write-Check 'WAIT' 'Provisional bills' "$rows of $Households households so far for $($clock.SimTodayText)"
            } else {
                Write-Check 'WAIT' 'Provisional bills' "none yet for $($clock.SimTodayText) - they appear one trigger into the day"
            }
        }
    }

    # --- 6 ------------------------------------------------------------------------------
    Write-Section '6. Serving API'
    try {
        $ready = Invoke-RestMethod -UseBasicParsing -TimeoutSec 5 -Uri "http://localhost:$($script:ApiPort)/health/ready"
        if ($ready.ready) { Write-Check 'PASS' 'Readiness' 'Postgres and MinIO reachable' }
        else { Write-Check 'FAIL' 'Readiness' 'a dependency is down' }
    } catch {
        Write-Check 'FAIL' 'Readiness' "http://localhost:$($script:ApiPort)/health/ready did not answer 200"
    }
    try {
        # Windows PowerShell returns a JSON array as a single object: assign first, then count.
        $load = Invoke-RestMethod -UseBasicParsing -TimeoutSec 5 -Uri "http://localhost:$($script:ApiPort)/api/v1/zones/load"
        $zoneCount = @($load).Count
        if ($zoneCount -eq 5) { Write-Check 'PASS' 'Zone load endpoint' '5 zones served from the speed view' }
        else { Write-Check 'WAIT' 'Zone load endpoint' "$zoneCount zones so far" }
    } catch {
        Write-Check 'FAIL' 'Zone load endpoint' 'no answer'
    }

    # --- 7 ------------------------------------------------------------------------------
    Write-Section '7. Orchestration - Airflow'
    $watcher = Invoke-Sql "SELECT count(*) FROM dag_run WHERE dag_id = 'tariff_watcher'" 'airflow'
    if ($watcher.Ok -and $watcher.Rows.Count -gt 0 -and [int]$watcher.Rows[0] -gt 0) {
        Write-Check 'PASS' 'tariff_watcher' "$($watcher.Rows[0]) runs (it polls the landing zone every real minute)"
    } elseif ($watcher.Ok) {
        Write-Check 'WAIT' 'tariff_watcher' 'no runs yet - Airflow takes a minute or two to start'
    } else {
        Write-Check 'WAIT' 'tariff_watcher' 'Airflow metadata database not ready yet'
    }
    $runs = Invoke-Sql "SELECT run_id, state FROM dag_run WHERE dag_id = 'daily_billing' ORDER BY id" 'airflow'
    if ($runs.Ok -and $runs.Rows.Count -gt 0) {
        $byState = @{}
        foreach ($row in $runs.Rows) {
            $parts = $row -split '\|'
            $state = $parts[1]
            if (-not $byState.ContainsKey($state)) { $byState[$state] = @() }
            $byState[$state] += $parts[0]
        }
        $counts = ($byState.Keys | Sort-Object | ForEach-Object { "$_=$($byState[$_].Count)" }) -join ', '
        Write-Check 'INFO' 'daily_billing runs' $counts
        if ($byState.ContainsKey('failed')) {
            foreach ($runId in $byState['failed']) {
                # Since R04 the watcher never offers a day without readings, so a failed
                # run for the seed day is a regression, not an expected artefact.
                $hint = if ($runId -eq "billing__$SeedDay") { ' (the seed day should never be billed - R04 regressed)' } else { '' }
                Write-Check 'FAIL' $runId "failed - open it in Airflow to see which task$hint"
            }
        }
        if (-not ($runs.Rows -match "^billing__$SeedDay\|")) {
            Write-Check 'PASS' 'Seed day' "no billing run for $SeedDay, the day-zero tariff with no readings (R04)"
        }
        $latestRun = ($runs.Rows[-1] -split '\|')[0]
        $tasks = Invoke-Sql "SELECT task_id, coalesce(state, 'none') FROM task_instance WHERE dag_id = 'daily_billing' AND run_id = '$latestRun' ORDER BY start_date NULLS LAST, task_id" 'airflow'
        if ($tasks.Ok -and $tasks.Rows.Count -gt 0) {
            $summary = ($tasks.Rows | ForEach-Object { $p = $_ -split '\|'; "$($p[0])=$($p[1])" }) -join ', '
            Write-Check 'INFO' "Latest run $latestRun" $summary
        }
    } else {
        Write-Check 'WAIT' 'daily_billing runs' 'none yet - the first is triggered when the second tariff file appears'
    }

    # --- 8-10: one billed day ----------------------------------------------------------
    $day = $Date
    if (-not $day) {
        $r = Invoke-Sql "SELECT coalesce(max(sim_date)::text, '') FROM pipeline_runs WHERE layer = 'batch_billing' AND status = 'success'"
        if ($r.Ok -and $r.Rows.Count -gt 0) { $day = $r.Rows[0] }
    }

    Write-Section ("8. Batch layer - authoritative bills (Spark, via Airflow){0}" -f $(if ($day) { " for $day" } else { '' }))
    if (-not $day) {
        Write-Check 'WAIT' 'Billed day' 'no day billed yet - the first billing finishes ~5-7 min after the anchor, the first complete day ~10-13 min'
        Write-Summary
        exit 0
    }
    $r = Invoke-Sql "SELECT status, count(*) FROM pipeline_runs WHERE sim_date = '$day' AND layer = 'batch_billing' GROUP BY status ORDER BY status"
    $statuses = ($r.Rows | ForEach-Object { $p = $_ -split '\|'; "$($p[0])=$($p[1])" }) -join ', '
    if ($r.Rows -match '^success\|') { Write-Check 'PASS' 'Run ledger' "pipeline_runs: $statuses" }
    else { Write-Check 'FAIL' 'Run ledger' "no success row for $day ($statuses)" }

    $r = Invoke-Sql @"
SELECT
  (SELECT count(*) FROM household_bill_daily WHERE sim_date = '$day'),
  (SELECT count(*) FROM zone_metrics_daily WHERE sim_date = '$day'),
  (SELECT count(*) FROM reconciliation_daily WHERE sim_date = '$day'),
  (SELECT count(*) FROM reconciliation_daily WHERE sim_date = '$day'
     AND tariff_effect + data_effect <> speed_estimate - batch_final),
  (SELECT coalesce(round(avg(tariff_effect), 2)::text, '-') FROM reconciliation_daily WHERE sim_date = '$day'),
  (SELECT coalesce(round(avg(data_effect), 2)::text, '-') FROM reconciliation_daily WHERE sim_date = '$day'),
  (SELECT coalesce(round(avg(pct_divergence), 3)::text, '-') FROM reconciliation_daily WHERE sim_date = '$day')
"@
    $bills = 0; $zoneRows = 0; $reconciled = 0; $breaks = 0; $meanTariff = '-'; $meanData = '-'; $meanPct = '-'
    if ($r.Ok -and $r.Rows.Count -gt 0) {
        $p = $r.Rows[0] -split '\|'
        $bills = [int]$p[0]; $zoneRows = [int]$p[1]; $reconciled = [int]$p[2]; $breaks = [int]$p[3]
        $meanTariff = $p[4]; $meanData = $p[5]; $meanPct = $p[6]
    }
    if ($bills -eq $Households) { Write-Check 'PASS' 'Bills' "$bills households in household_bill_daily" }
    else { Write-Check 'FAIL' 'Bills' "$bills rows, expected $Households" }
    if ($zoneRows -eq 5) { Write-Check 'PASS' 'Zone rollup' '5 zones in zone_metrics_daily (cross-check against bills passed)' }
    else { Write-Check 'WAIT' 'Zone rollup' "$zoneRows zones - the rollup runs after the bills" }
    $archive = Get-BucketListing "voltstream-archive/tariff/sim_date=$day/"
    if (@($archive.Lines | Where-Object { $_ -match '\.parquet$' }).Count -gt 0) {
        Write-Check 'PASS' 'Archived tariff' "voltstream-archive/tariff/sim_date=$day/"
    } else {
        Write-Check 'FAIL' 'Archived tariff' "nothing under voltstream-archive/tariff/sim_date=$day/"
    }

    # --- 9 ------------------------------------------------------------------------------
    Write-Section "9. Reconciliation and daily report - $day"
    if ($reconciled -eq 0) {
        Write-Check 'WAIT' 'Reconciled rows' 'none yet - reconciliation runs after the rollup'
    } else {
        if ($reconciled -eq $Households) { Write-Check 'PASS' 'Reconciled rows' "$reconciled households" }
        else { Write-Check 'WARN' 'Reconciled rows' "$reconciled of $Households (households missing from one view are skipped)" }
        if ($breaks -eq 0) { Write-Check 'PASS' 'D4 identity' 'tariff_effect + data_effect = speed - batch on every row' }
        else { Write-Check 'FAIL' 'D4 identity' "$breaks rows break it" }
        Write-Check 'INFO' 'Divergence' "mean tariff_effect $meanTariff, mean data_effect $meanData, mean pct $meanPct%  (record these for T140)"
    }
    # The DAG's last task. A failure shows in stage 7 as a failed run, so a missing
    # report here only means that task has not finished yet.
    $reportKey = "voltstream-archive/reports/report_$day.md"
    $report = Get-BucketListing $reportKey
    if (@($report.Lines | Where-Object { $_ -match "report_$day\.md$" }).Count -gt 0) {
        Write-Check 'PASS' 'Daily report' $reportKey
    } else {
        Write-Check 'WAIT' 'Daily report' "not published yet - the DAG's last task, after reconciliation"
    }

    # --- 10 -----------------------------------------------------------------------------
    Write-Section "10. Merge function - $Household"
    try {
        $final = Invoke-RestMethod -UseBasicParsing -TimeoutSec 5 -Uri "http://localhost:$($script:ApiPort)/api/v1/households/$Household/bill?date=$day"
        if ($final.source -eq 'batch') { Write-Check 'PASS' "Bill for $day" "source=batch, provisional=false, total $($final.total)" }
        else { Write-Check 'FAIL' "Bill for $day" "source=$($final.source) although the day is billed" }
    } catch {
        Write-Check 'FAIL' "Bill for $day" 'no answer from the bill endpoint'
    }
    if ($clock -and $clock.SimTodayText -ne $day) {
        try {
            $live = Invoke-RestMethod -UseBasicParsing -TimeoutSec 5 -Uri "http://localhost:$($script:ApiPort)/api/v1/households/$Household/bill?date=$($clock.SimTodayText)"
            if ($live.source -eq 'speed') { Write-Check 'PASS' "Bill for $($clock.SimTodayText)" "source=speed, provisional=true, total $($live.total) (today, still open)" }
            else { Write-Check 'WARN' "Bill for $($clock.SimTodayText)" "source=$($live.source)" }
        } catch {
            Write-Check 'WAIT' "Bill for $($clock.SimTodayText)" 'no provisional bill yet for today'
        }
    }

    Write-Summary
    if ($script:Tally.FAIL -gt 0) { exit 1 }
}

function Invoke-Bill {
    Assert-Docker
    $day = $Date
    if (-not $day) {
        $clock = Get-EffectiveClock
        if (-not $clock) { Stop-WithError 'Pass -Date yyyy-MM-dd (no clock anchor found to work out today).' }
        $day = $clock.SimTodayText
    }
    try {
        $bill = Invoke-RestMethod -UseBasicParsing -TimeoutSec 5 -Uri "http://localhost:$($script:ApiPort)/api/v1/households/$Household/bill?date=$day"
    } catch {
        Stop-WithError "No bill for $Household on $day yet ($($_.Exception.Message))."
    }
    $colour = if ($bill.source -eq 'batch') { 'Green' } else { 'Yellow' }
    $label = if ($bill.source -eq 'batch') { 'FINAL (batch layer)' } else { 'PROVISIONAL (speed layer)' }
    Write-Host ''
    Write-Host "  $Household on $day  " -NoNewline
    Write-Host $label -ForegroundColor $colour
    Write-Host "  Tariff applied     $($bill.tariff_date)"
    Write-Host "  Consumption        $($bill.consumption_kwh) kWh   (solar $($bill.solar_kwh), exported $($bill.export_kwh))"
    Write-Host "  Energy charge      $($bill.energy_charge)"
    Write-Host "  Fixed charge       $($bill.fixed_charge)"
    Write-Host "  Subsidy discount  -$($bill.subsidy_discount)"
    Write-Host "  Export credit     -$($bill.export_credit)"
    Write-Host "  Total              $($bill.total)" -ForegroundColor $colour
    if ($bill.source -eq 'batch') {
        Write-Host "  From $($bill.readings_count) readings, $($bill.duplicates_removed) duplicates removed, run $($bill.pipeline_run_id)" -ForegroundColor DarkGray
    }
}

function Invoke-Logs {
    Assert-Docker
    if (-not $Service) {
        Stop-WithError ("Pass -Service <name>. Services: " + (@($ExpectedServices.Keys) -join ', '))
    }
    Invoke-Compose logs -f --tail 100 $Service
}

function Show-Help {
    Write-Host @'

voltstream - build, run and check the stack (Windows, no make needed)

  .\scripts\voltstream.ps1 run       Wipe, build and start a fresh simulation
  .\scripts\voltstream.ps1 build     Build the images
  .\scripts\voltstream.ps1 start     Start (fresh stack: new clock; existing data: resume)
  .\scripts\voltstream.ps1 stop      Stop containers, keep data
  .\scripts\voltstream.ps1 clean     Delete containers and ALL data   [-Force skips the prompt]
  .\scripts\voltstream.ps1 status    Clock, containers, where the pipeline has got to
  .\scripts\voltstream.ps1 check     Verify every stage end to end    [-Date yyyy-MM-dd]
  .\scripts\voltstream.ps1 bill      One household's bill and its source
                                     [-Household HH-0001] [-Date yyyy-MM-dd]
  .\scripts\voltstream.ps1 logs      Follow a service's logs          -Service speed-layer

Typical session:
  run  ->  status (now and then)  ->  check (after ~13 minutes)  ->  stop or clean

One simulated day is 5 real minutes. The first complete day (day 2) is billed and
reconciled about 10-13 minutes after 'run' finishes starting the stack.

'@
}

# ---------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------

if ($Command -eq 'help') { Show-Help; exit 0 }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Stop-WithError 'docker is not on PATH. Install Docker Desktop first.'
}
Initialize-Settings

switch ($Command) {
    'build'  { Assert-Docker; Invoke-Build }
    'start'  { Invoke-Start }
    'run'    { Invoke-Run }
    'stop'   { Invoke-Stop }
    'clean'  { Invoke-Clean }
    'status' { Invoke-Status }
    'check'  { Invoke-Check }
    'bill'   { Invoke-Bill }
    'logs'   { Invoke-Logs }
}
