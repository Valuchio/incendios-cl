<# ---------------------------------------------------------------
 set_env.ps1 — Configura Spark/Hadoop + Python del venv
 - Ejecuta este archivo en la raíz del proyecto.
 - Normalmente no lo llamas directo, sino desde activate_project.ps1
---------------------------------------------------------------- #>

$ErrorActionPreference = "Stop"

$proj = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPython = Join-Path $proj ".venv\Scripts\python.exe"

# Validar Python del venv
if (!(Test-Path $venvPython)) {
  Write-Host "[ERROR] No se encontró python.exe en .venv\Scripts" -ForegroundColor Red
  exit 1
}

# Forzar PySpark a usar Python del venv
$env:PYSPARK_PYTHON        = $venvPython
$env:PYSPARK_DRIVER_PYTHON = $venvPython

# Configurar Hadoop si existe carpeta hadoop\bin
$hadoop = Join-Path $proj "hadoop"
$bin    = Join-Path $hadoop "bin"
if (Test-Path $bin) {
  $env:HADOOP_HOME = $hadoop
  if (-not ($env:PATH -split ";" | Where-Object { $_ -ieq $bin })) {
    $env:PATH = "$bin;$env:PATH"
  }
}

Write-Host "Variables de Spark/Hadoop configuradas " -ForegroundColor Green
Write-Host ("PYSPARK_PYTHON : {0}" -f $env:PYSPARK_PYTHON)
if ($env:HADOOP_HOME) {
  Write-Host ("HADOOP_HOME    : {0}" -f $env:HADOOP_HOME)
}
