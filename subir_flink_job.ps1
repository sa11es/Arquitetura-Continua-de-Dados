<#
.SYNOPSIS
  Compila o Motor Analitico e sobe o job no Flink, tudo automatico.

.EXAMPLE
  .\subir-job.ps1              # compila, cancela o job antigo e envia o novo
  .\subir-job.ps1 -Infra       # antes disso, sobe o docker compose
  .\subir-job.ps1 -SkipBuild   # so reenvia o jar que ja esta compilado
  .\subir-job.ps1 -SkipCdc     # nao verifica/ativa os conectores Debezium

  Coloque este arquivo na raiz do projeto (mesma pasta de ativar_cdc_debezium.py,
  infra\ e flink-job\).
#>
param(
    [switch]$Infra,
    [switch]$SkipBuild,
    [switch]$SkipCdc
)

$ErrorActionPreference = 'Continue'
$Inicio = Get-Date

# ---------------- Configuracao (ajuste aqui se algo mudar) ----------------
$Root       = $PSScriptRoot
$JobDir     = Join-Path $Root 'flink-job'
$Compose    = Join-Path $Root 'infra\docker-compose.yml'
$JobManager = 'flink-jobmanager'
$MainClass  = 'br.edu.tcc.MotorAnalitico'
$FlinkApi   = 'http://localhost:8081'
$ConnectApi = 'http://localhost:8083'
$MavenImage = 'maven:3.8.6-openjdk-11'
$Containers = @('kafka', 'debezium-connect', 'flink-jobmanager', 'flink-taskmanager')
$Conectores = @('sinan-postgres-ponta-grossa', 'sinan-postgres-curitiba', 'sinan-postgres-castro')

# ---------------- Funcoes auxiliares ----------------
function Step($msg) { Write-Host ""; Write-Host "==> $msg" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "    OK   $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "    AVISO $msg" -ForegroundColor Yellow }
function Fail($msg) { Write-Host "    ERRO $msg" -ForegroundColor Red; exit 1 }

function Run([string]$exe, [string[]]$argumentos) {
    & $exe @argumentos
    if ($LASTEXITCODE -ne 0) { Fail "Comando falhou: $exe $($argumentos -join ' ')" }
}

function Wait-Until([scriptblock]$cond, [int]$timeoutSec, [string]$oQue) {
    $limite = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $limite) {
        try { if (& $cond) { return } } catch { }
        Start-Sleep -Seconds 2
    }
    Fail "Tempo esgotado esperando: $oQue"
}

function Get-ConnectorNames {
    $r = Invoke-WebRequest "$ConnectApi/connectors" -UseBasicParsing -TimeoutSec 5
    $r.Content | ConvertFrom-Json
}

# ---------------- 1. Docker ----------------
Step 'Verificando Docker'
docker info *> $null
if ($LASTEXITCODE -ne 0) { Fail 'Docker nao esta rodando. Abra o Docker Desktop e tente de novo.' }
Ok 'Docker ativo'

if ($Infra) {
    Step 'Subindo containers (docker compose up -d)'
    Run 'docker' @('compose', '-f', $Compose, 'up', '-d')
}

Step 'Verificando containers'
$rodando = @(docker ps --format '{{.Names}}')
foreach ($c in $Containers) {
    if ($rodando -notcontains $c) {
        Fail "Container '$c' nao esta rodando. Rode: .\subir-job.ps1 -Infra"
    }
}
Ok 'Containers no ar'

Step 'Aguardando o Flink (JobManager + TaskManager)'
Wait-Until { (Invoke-RestMethod "$FlinkApi/overview" -TimeoutSec 5).taskmanagers -ge 1 } 180 'TaskManager registrar no Flink'
Ok 'Flink pronto'

# ---------------- 2. CDC (Debezium) ----------------
if (-not $SkipCdc) {
    Step 'Verificando CDC (Debezium)'
    Wait-Until { (Invoke-WebRequest "$ConnectApi/connectors" -UseBasicParsing -TimeoutSec 5).StatusCode -eq 200 } 180 'Debezium Connect responder na porta 8083'

    $nomes = @(Get-ConnectorNames)
    $faltando = @($Conectores | Where-Object { $nomes -notcontains $_ })
    if ($faltando.Count -gt 0) {
        Warn "Conectores ausentes: $($faltando -join ', '). Ativando..."
        if (-not (Get-Command python -ErrorAction SilentlyContinue)) { Fail 'python nao encontrado no PATH.' }
        Push-Location $Root
        python ativar_cdc_debezium.py
        Pop-Location
    }

    Wait-Until {
        foreach ($n in $Conectores) {
            $s = Invoke-RestMethod "$ConnectApi/connectors/$n/status" -TimeoutSec 5
            $tasks = @($s.tasks)
            $falhou = ($s.connector.state -eq 'FAILED') -or (@($tasks | Where-Object { $_.state -eq 'FAILED' }).Count -gt 0)
            if ($falhou) {
                Invoke-RestMethod -Method Post "$ConnectApi/connectors/$n/restart?includeTasks=true&onlyFailed=true" | Out-Null
                return $false
            }
            if ($s.connector.state -ne 'RUNNING' -or $tasks.Count -eq 0) { return $false }
            foreach ($t in $tasks) { if ($t.state -ne 'RUNNING') { return $false } }
        }
        return $true
    } 120 'os conectores Debezium ficarem RUNNING'
    Ok 'Conectores Debezium RUNNING'
}

# ---------------- 3. Build ----------------
if (-not $SkipBuild) {
    Step 'Compilando o Motor Analitico (Maven em container)'
    Warn 'Na primeira vez baixa as dependencias (demora); as proximas usam o cache do volume tcc-m2-cache.'
    Run 'docker' @('run', '--rm',
                   '-v', "${JobDir}:/app",
                   '-v', 'tcc-m2-cache:/root/.m2',
                   '-w', '/app', $MavenImage,
                   'mvn', '-B', '-q', 'clean', 'package', '-DskipTests')
    Ok 'Build concluido'
}

Step 'Localizando o jar'
$jar = Get-ChildItem (Join-Path $JobDir 'target') -Filter 'motor-analitico-epidemiologico-*.jar' -ErrorAction SilentlyContinue |
       Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $jar) { Fail 'Jar nao encontrado em flink-job\target. Rode sem -SkipBuild.' }
Ok "$($jar.Name) (gerado em $($jar.LastWriteTime))"

# ---------------- 4. Cancelar jobs antigos ----------------
Step 'Cancelando jobs antigos'
$estadosAtivos = @('RUNNING', 'RESTARTING', 'CREATED', 'INITIALIZING')
$antigos = @((Invoke-RestMethod "$FlinkApi/jobs/overview").jobs | Where-Object { $estadosAtivos -contains $_.state })
foreach ($j in $antigos) {
    Invoke-RestMethod -Method Patch "$FlinkApi/jobs/$($j.jid)?mode=cancel" | Out-Null
    Ok "Cancelando '$($j.name)' ($($j.jid))"
}
if ($antigos.Count -gt 0) {
    Wait-Until { $o = Invoke-RestMethod "$FlinkApi/overview"; $o.'slots-available' -eq $o.'slots-total' } 90 'os slots do Flink serem liberados'
    Ok 'Slots liberados'
} else {
    Ok 'Nenhum job ativo'
}

# ---------------- 5. Enviar e submeter ----------------
Step 'Enviando o jar para o JobManager'
Run 'docker' @('cp', $jar.FullName, "${JobManager}:/opt/flink/motor-analitico.jar")

Step 'Submetendo o job (modo detached)'
Run 'docker' @('exec', $JobManager, 'flink', 'run', '-d', '-c', $MainClass, '/opt/flink/motor-analitico.jar')

Step 'Confirmando que o job esta RUNNING'
$buscaJob = {
    (Invoke-RestMethod "$FlinkApi/jobs/overview").jobs |
        Where-Object { $_.name -like 'Motor*' } |
        Sort-Object 'start-time' -Descending | Select-Object -First 1
}
Wait-Until {
    $j = & $buscaJob
    if ($j -and $j.state -eq 'FAILED') { Fail "Job entrou em FAILED. Veja $FlinkApi (aba Exceptions)." }
    $j -and $j.state -eq 'RUNNING'
} 90 'o job ficar RUNNING'

$job = & $buscaJob
$seg = [int]((Get-Date) - $Inicio).TotalSeconds
Write-Host ""
Write-Host "Pronto em ${seg}s. Job '$($job.name)' RUNNING (id $($job.jid))" -ForegroundColor Green
Write-Host "Flink UI: $FlinkApi"