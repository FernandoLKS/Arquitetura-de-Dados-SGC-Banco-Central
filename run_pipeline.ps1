param(
    [ValidateSet(
        "all",
        "ingestion",
        "silver",
        "gold",
        "postgres",
        "dbt"
    )]
    [string]$Stage = "all"
)


Write-Host ""
Write-Host "============================================================"
Write-Host "BCB DATA PIPELINE"
Write-Host "============================================================"
Write-Host ""
Write-Host "Selected stage: $Stage"


# ============================================================
# INFRASTRUCTURE
# ============================================================

Write-Host ""
Write-Host "[1/6] Starting infrastructure..."

docker compose up -d

if ($LASTEXITCODE -ne 0) {

    Write-Host ""
    Write-Host "Infrastructure startup failed."

    exit 1
}

Write-Host "Infrastructure started."


# ============================================================
# MINIO INITIALIZATION
# ============================================================

Write-Host ""
Write-Host "Checking MinIO initialization..."

$minioInitStatus = docker inspect minio-init `
    --format "{{.State.Status}}"

$minioInitExitCode = docker inspect minio-init `
    --format "{{.State.ExitCode}}"

if ($minioInitStatus -ne "exited" -or $minioInitExitCode -ne "0") {

    Write-Host ""
    Write-Host "MinIO initialization failed."

    Write-Host ""
    Write-Host "Container status: $minioInitStatus"
    Write-Host "Exit code: $minioInitExitCode"

    exit 1
}

Write-Host "MinIO initialization completed successfully."
Write-Host "Buckets and users are ready."

# ============================================================
# INGESTION
# ============================================================

if ($Stage -eq "all" -or $Stage -eq "ingestion") {

    Write-Host ""
    Write-Host "[2/6] Running BCB ingestion..."

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
    Write-Host "[3/6] Running BRONZE -> SILVER..."

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

    docker exec spark /opt/spark/bin/spark-submit `
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
    Write-Host "[4/6] Running SILVER -> GOLD..."

    docker exec spark /opt/spark/bin/spark-submit `
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
    Write-Host "[5/6] Loading GOLD -> PostgreSQL..."

    docker exec spark /opt/spark/bin/spark-submit `
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
# DBT
# ============================================================

if ($Stage -eq "all" -or $Stage -eq "dbt") {

    Write-Host ""
    Write-Host "[6/6] Running dbt transformations..."

    docker exec dbt dbt build

    if ($LASTEXITCODE -ne 0) {

        Write-Host ""
        Write-Host "dbt build failed."

        exit 1
    }

    Write-Host "dbt transformations completed."
}


# ============================================================
# SUCCESS
# ============================================================

Write-Host ""
Write-Host "============================================================"
Write-Host "PIPELINE COMPLETED SUCCESSFULLY"
Write-Host "============================================================"