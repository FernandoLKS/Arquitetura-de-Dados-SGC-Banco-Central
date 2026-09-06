Write-Host ""
Write-Host "============================================================"
Write-Host "BCB DATA PIPELINE"
Write-Host "============================================================"

Write-Host ""
Write-Host "[1/5] Starting infrastructure..."

docker compose up -d | Out-Null

Write-Host "Infrastructure ready."

Write-Host ""
Write-Host "[2/5] Running BCB ingestion..."

$ingestionOutput = @(python -m sources.bcb.ingestion_pipeline)
$ingestionExitCode = $LASTEXITCODE

$ingestionOutput | ForEach-Object {
    Write-Host $_
}

if ($ingestionExitCode -ne 0) {
    Write-Host ""
    Write-Host "BCB ingestion failed."
    exit 1
}

$ingestionDate = $ingestionOutput[-1]

Write-Host ""
Write-Host "Ingestion completed."
Write-Host "Ingestion date: $ingestionDate"

Write-Host ""
Write-Host "[3/5] Running RAW -> BRONZE..."

docker exec bcb-spark /opt/spark/bin/spark-submit `
    --conf spark.log.level=WARN `
    /opt/spark-apps/jobs/bcb/raw_to_bronze.py `
    $ingestionDate

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "RAW -> BRONZE transformation failed."
    exit 1
}

Write-Host "Bronze transformation completed."

Write-Host ""
Write-Host "[4/5] Running BRONZE -> SILVER..."

docker exec bcb-spark /opt/spark/bin/spark-submit `
    --conf spark.log.level=WARN `
    /opt/spark-apps/jobs/bcb/bronze_to_silver.py

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "BRONZE -> SILVER transformation failed."
    exit 1
}

Write-Host "Silver transformation completed."

Write-Host ""
Write-Host "[5/5] Running SILVER -> GOLD..."

docker exec bcb-spark /opt/spark/bin/spark-submit `
    --conf spark.log.level=WARN `
    /opt/spark-apps/jobs/bcb/silver_to_gold.py

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "SILVER -> GOLD transformation failed."
    exit 1
}

Write-Host "Gold transformation completed."

Write-Host ""
Write-Host "============================================================"
Write-Host "PIPELINE COMPLETED SUCCESSFULLY"
Write-Host "============================================================"