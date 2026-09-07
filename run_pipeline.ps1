param(
    [ValidateSet("all", "ingestion", "silver", "gold", "postgres")]
    [string]$Stage = "all"
)

Write-Host ""
Write-Host "============================================================"
Write-Host "BCB DATA PIPELINE"
Write-Host "============================================================"
Write-Host ""
Write-Host "Selected stage: $Stage"


# ============================================================
# Infrastructure
# ============================================================

Write-Host ""
Write-Host "[1/5] Starting infrastructure..."

docker compose up -d | Out-Null

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Infrastructure startup failed."
    exit 1
}

Write-Host "Infrastructure ready."


# ============================================================
# INGESTION
# ============================================================

if ($Stage -eq "all" -or $Stage -eq "ingestion") {

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
}


# ============================================================
# BRONZE -> SILVER
# ============================================================

if ($Stage -eq "all" -or $Stage -eq "silver") {

    Write-Host ""
    Write-Host "[3/5] Running BRONZE -> SILVER..."

    if ($Stage -eq "silver") {

        Write-Host ""
        Write-Host "Using latest Bronze ingestion."

        $ingestionDate = Get-ChildItem `
            -Path "." `
            -Recurse `
            -Directory `
            -ErrorAction SilentlyContinue |
            Where-Object {
                $_.Name -match "^ingestion_date=\d{4}-\d{2}-\d{2}$"
            } |
            Sort-Object Name -Descending |
            Select-Object -First 1 -ExpandProperty Name

        if (-not $ingestionDate) {

            Write-Host ""
            Write-Host "Could not determine ingestion date."
            Write-Host "Run the ingestion stage first."

            exit 1
        }

        $ingestionDate = $ingestionDate -replace "ingestion_date=", ""

        Write-Host "Ingestion date: $ingestionDate"
    }

    docker exec bcb-spark /opt/spark/bin/spark-submit `
        --conf spark.log.level=WARN `
        /opt/spark-apps/jobs/bcb/bronze_to_silver.py `
        $ingestionDate

    if ($LASTEXITCODE -ne 0) {

        Write-Host ""
        Write-Host "BRONZE -> SILVER transformation failed."

        exit 1
    }

    Write-Host "Silver transformation completed."
}


# ============================================================
# SILVER -> GOLD
# ============================================================

if ($Stage -eq "all" -or $Stage -eq "gold") {

    Write-Host ""
    Write-Host "[4/5] Running SILVER -> GOLD..."

    docker exec bcb-spark /opt/spark/bin/spark-submit `
        --conf spark.log.level=WARN `
        /opt/spark-apps/jobs/bcb/silver_to_gold.py

    if ($LASTEXITCODE -ne 0) {

        Write-Host ""
        Write-Host "SILVER -> GOLD transformation failed."

        exit 1
    }

    Write-Host "Gold transformation completed."
}


# ============================================================
# GOLD -> POSTGRESQL
# ============================================================

if ($Stage -eq "all" -or $Stage -eq "postgres") {

    Write-Host ""
    Write-Host "[5/5] Loading GOLD -> PostgreSQL..."

    docker exec bcb-spark /opt/spark/bin/spark-submit `
        --conf spark.log.level=WARN `
        /opt/spark-apps/jobs/bcb/gold_to_postgres.py

    if ($LASTEXITCODE -ne 0) {

        Write-Host ""
        Write-Host "GOLD -> PostgreSQL load failed."

        exit 1
    }

    Write-Host "Gold loaded into PostgreSQL."
}


# ============================================================
# SUCCESS
# ============================================================

Write-Host ""
Write-Host "============================================================"
Write-Host "PIPELINE COMPLETED SUCCESSFULLY"
Write-Host "============================================================"