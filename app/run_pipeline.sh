<# 
run_pipeline.ps1
End-to-end demo runner for your Big Data pipeline (Postgres → Spark → Snowflake).
Run this from the folder that contains docker-compose.yml

Usage:
  pwsh ./run_pipeline.ps1
  or
  powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1
#>

$ErrorActionPreference = "Stop"

# ---- Config ----
# JARs inside the container (mounted from ./app/jars on host)
$JARS = "/home/jovyan/work/jars/spark-snowflake_2.12-2.14.0-spark_3.4.jar,/home/jovyan/work/jars/snowflake-jdbc-3.14.4.jar"

# Paths inside container
$APP_DIR   = "/home/jovyan/work"
$GENERATOR = "$APP_DIR/generate_data.py"
$BRONZE    = "$APP_DIR/ingest_to_snowflake.py"
$SILVER    = "$APP_DIR/transform_data.py"
$GOLD      = "$APP_DIR/create_kpis.py"

# ---- Prep host folders ----
if (!(Test-Path -LiteralPath ".\output")) {
  New-Item -ItemType Directory -Path ".\output" | Out-Null
}

Write-Host "▶ Bringing containers down (if any)..." -ForegroundColor Cyan
docker compose down

Write-Host "▶ Starting containers..." -ForegroundColor Cyan
docker compose up -d

Write-Host "▶ Waiting for postgres_fraud to be healthy..." -ForegroundColor Cyan
$maxWait = 60
for ($i=0; $i -lt $maxWait; $i++) {
  $status = (docker inspect -f '{{.State.Health.Status}}' postgres_fraud 2>$null)
  if ($status -eq "healthy") { break }
  Start-Sleep -Seconds 2
}
if ($status -ne "healthy") {
  throw "postgres_fraud container not healthy after wait."
}

Write-Host "▶ Checking Spark & jars..." -ForegroundColor Cyan
docker compose exec spark bash -lc "python -V; pyspark --version; ls -la /home/jovyan/work/jars"

Write-Host "▶ Checking Snowflake env vars in container..." -ForegroundColor Cyan
docker compose exec spark bash -lc "env | grep -E '^SNOWFLAKE_' || true"

# ---- Step 1: Generate & load fake data into Postgres (and write CSV to ./output) ----
Write-Host "`n▶ Step 1/4: Generating data into Postgres + writing CSV..." -ForegroundColor Green
docker compose exec spark python $GENERATOR

# ---- Step 2: Bronze (Postgres → Snowflake) ----
Write-Host "`n▶ Step 2/4: Loading Bronze tables into Snowflake..." -ForegroundColor Green
docker compose exec spark spark-submit --jars "$JARS" $BRONZE

# ---- Step 3: Silver (denormalize/enrich in Snowflake) ----
Write-Host "`n▶ Step 3/4: Creating Silver layer in Snowflake..." -ForegroundColor Green
docker compose exec spark spark-submit --jars "$JARS" $SILVER

# ---- Step 4: Gold (KPIs / flags in Snowflake) ----
Write-Host "`n▶ Step 4/4: Creating Gold layer (KPIs) in Snowflake..." -ForegroundColor Green
docker compose exec spark spark-submit --jars "$JARS" $GOLD

Write-Host "`n✅ Pipeline completed. Verify in Snowflake (ANJ_BD_PROJECT1) and check ./output/transactions_output.csv" -ForegroundColor Yellow
