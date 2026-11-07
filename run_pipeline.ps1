<#
.SYNOPSIS
    1-Click Execution & Demo Script for NYC Transit Big Data Project (IT4931 - SoICT, HUST).

.DESCRIPTION
    Executes the end-to-end Lambda Architecture pipeline, runs automated unit & acceptance tests,
    regenerates report figures & the interactive web dashboard, and opens the dashboard.
#>

param (
    [switch]$FullScale = $false,
    [switch]$SkipBatch = $false,
    [switch]$ServeDashboard = $false
)

$ErrorActionPreference = "Stop"
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host " NYC TRANSIT REAL-TIME PASSENGER FLOW & OVERCROWDING ANALYTICS (IT4931)" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan

if (-not $SkipBatch) {
    if ($FullScale) {
        Write-Host "`n[1/5] Running Full-Scale Batch Cleaning & Baseline (27 weeks, 5.25M rows)..." -ForegroundColor Yellow
        python src/batch/clean_turnstile.py --engine local
        Write-Host "`n[2/5] Running Full-Scale Operational Correlation Analysis (2016 KPIs)..." -ForegroundColor Yellow
        python src/batch/correlation_analysis.py
    } else {
        Write-Host "`n[1/5] Using existing Full-Scale Batch Baseline in data/processed/ (use -FullScale to recompute)..." -ForegroundColor Green
    }

    Write-Host "`n[2/5] Running K-Means Station Behavioral Clustering (K=3)..." -ForegroundColor Yellow
    python src/batch/kmeans_clustering.py --engine local

    Write-Host "`n[3/5] Running Speed Layer Sliding Window (15m/1m) & Z-Score Anomaly Detection..." -ForegroundColor Yellow
    python src/streaming/stream_processor.py --engine local --max-events 80000
}

Write-Host "`n[4/5] Generating High-Resolution Report Figures & Interactive Live Dashboard..." -ForegroundColor Yellow
python src/serving/generate_report_figures.py
python src/serving/dashboard_server.py

Write-Host "`n[5/5] Running Automated Unit & Acceptance Test Suite (pytest)..." -ForegroundColor Yellow
python -m pytest tests/ -v

Write-Host "`n[DONE] All Lambda Architecture deliverables verified! Opening docs/live_dashboard.html..." -ForegroundColor Green
if ($ServeDashboard) {
    python src/serving/dashboard_server.py --serve
} else {
    Start-Process "docs/live_dashboard.html"
}
