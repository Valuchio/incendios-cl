# ============================================
#            PIPELINE ETL - POWERSHELL
# ============================================

# Ir a la carpeta del proyecto
Set-Location "C:\proyectos\incendios-cl"

Write-Host "============================================"
Write-Host "          PIPELINE ETL - CONFIG"
Write-Host "============================================"

# Pedir parámetros
$BEARER = Read-Host "Ingresa BEARER TOKEN"
$USER   = Read-Host "Ingresa usuario (ej: CentralCBS o CENTRAL132MAIPU)"
$MAX    = Read-Host "Ingresa MAX_RESULTS (ej: 10 - 100)"

Write-Host ""
Write-Host "============================================"
Write-Host "CONFIGURACION UTILIZADA:"
Write-Host "USER   : $USER"
Write-Host "MAX    : $MAX"
Write-Host "BASE   : C:\proyectos\incendios-cl\data"
Write-Host "============================================"
Write-Host ""

# ================================================
# 1) INGESTA
# ================================================
Write-Host "`n[1/4] Ejecutando INGESTA..." -ForegroundColor Cyan

spark-submit src/ingesta/ingesta_spark.py `
    bearer="$BEARER" `
    user="$USER" `
    max=$MAX `
    base_dir="C:\proyectos\incendios-cl\data"

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: La etapa de INGESTA falló." -ForegroundColor Red
    pause
    exit 1
}

# ================================================
# 2) NORMALIZACION
# ================================================
Write-Host "`n[2/4] Ejecutando NORMALIZACION..." -ForegroundColor Cyan

spark-submit src/normalizacion/normalizacion_spark.py `
    base_dir="C:\proyectos\incendios-cl\data"

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: La etapa de NORMALIZACION falló." -ForegroundColor Red
    pause
    exit 1
}

# ================================================
# 3) CURADO
# ================================================
Write-Host "`n[3/4] Ejecutando CURADO..." -ForegroundColor Cyan

spark-submit src/curado/curado_spark.py `
    base_dir="C:\proyectos\incendios-cl\data"

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: La etapa de CURADO falló." -ForegroundColor Red
    pause
    exit 1
}

# ================================================
# 4) LEER CURADO
# ================================================
Write-Host "`n[4/4] Mostrando CURADO (preview)..." -ForegroundColor Cyan

spark-submit src/funciones/leer_curado.py `
    base_dir="C:\proyectos\incendios-cl\data" `
    n=50

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: La etapa de LECTURA falló." -ForegroundColor Red
    pause
    exit 1
}

Write-Host "`n============================================="
Write-Host "        PIPELINE COMPLETADO CON ÉXITO" -ForegroundColor Green
Write-Host "============================================="
pause
