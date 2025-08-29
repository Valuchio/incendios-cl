<# ---------------------------------------------------------------
 activate_project.ps1 — Activa venv y configura Spark/Hadoop
 - Coloca este archivo en la RAÍZ del proyecto.
 - Ejecuta:  .\activate_project.ps1
---------------------------------------------------------------- #>

$ErrorActionPreference = "Stop"

$proj = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($proj)) {
  if ($PSCommandPath) { $proj = Split-Path -Parent $PSCommandPath }
  else                { $proj = (Get-Location).Path }
}

# 1) Activar venv
$venvActivate = Join-Path $proj ".venv\Scripts\Activate.ps1"
if (!(Test-Path $venvActivate)) {
  Write-Host "[ERROR] No existe .venv, crea el entorno virtual primero." -ForegroundColor Red
  exit 1
}
& $venvActivate

# 2) Configurar Spark/Hadoop (llama a set_env.ps1)
& (Join-Path $proj "set_env.ps1")

Write-Host "`nProyecto listo  (venv + Spark/Hadoop activados)`n" -ForegroundColor Green
