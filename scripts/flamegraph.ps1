# Flamegraph profiling script for goose-db
# Prerequisites: cargo install flamegraph

Write-Host "goose-db Flamegraph Profiler" -ForegroundColor Cyan
Write-Host "=============================" -ForegroundColor Cyan
Write-Host ""

# Check if flamegraph is installed
$flamegraphInstalled = Get-Command cargo-flamegraph -ErrorAction SilentlyContinue
if (-not $flamegraphInstalled) {
    Write-Host "Installing cargo-flamegraph..." -ForegroundColor Yellow
    cargo install flamegraph
}

# Build release binary
Write-Host "Building release binary..." -ForegroundColor Yellow
cargo build --release

# Generate flamegraph
Write-Host "Generating flamegraph (this may take a moment)..." -ForegroundColor Yellow
cargo flamegraph --release --bin goose-db -o flamegraph.svg

Write-Host ""
Write-Host "Flamegraph saved to: flamegraph.svg" -ForegroundColor Green
Write-Host "Open in browser to analyze hot paths" -ForegroundColor Green
