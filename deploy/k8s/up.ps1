# Brings the whole system up in a local Kubernetes cluster (kind).
# Run from the project root:   .\deploy\k8s\up.ps1
# Skip image rebuild:          .\deploy\k8s\up.ps1 -SkipBuild
# Skip only the slow analyzer: .\deploy\k8s\up.ps1 -SkipAnalyzer
param(
    [switch]$SkipBuild,
    [switch]$SkipAnalyzer
)
# Not "Stop": in Windows PowerShell 5.1 any stderr text from a native program
# with redirected output (e.g. "No kind clusters found.") would kill the script.
# Failures are caught explicitly by exit code in Run and below.
$ErrorActionPreference = "Continue"
$Cluster = "photo"

function Run {
    # Runs a native command and stops the script if it fails.
    # A plain function (no [Parameter] attributes) on purpose: then flags like
    # -f or -n go to the command as-is instead of being parsed by PowerShell.
    $exe = $args[0]
    $rest = @($args | Select-Object -Skip 1)
    Write-Host ">> $($args -join ' ')" -ForegroundColor DarkGray
    & $exe @rest
    if ($LASTEXITCODE -ne 0) { throw "failed (exit $LASTEXITCODE): $($args -join ' ')" }
}

if (-not (Test-Path "deploy/kustomization.yaml")) {
    throw "Run this script from the project root (D:\photo-analysis)."
}

# 1. Cluster
$clusters = @(kind get clusters 2>$null)
if ($clusters -notcontains $Cluster) {
    Write-Host "`n== Creating kind cluster '$Cluster'" -ForegroundColor Cyan
    Run kind create cluster --config deploy/k8s/kind-config.yaml
} else {
    Write-Host "`n== Cluster '$Cluster' already exists" -ForegroundColor Cyan
}
Run kubectl config use-context "kind-$Cluster"

# 2. Our images
if (-not $SkipBuild) {
    Write-Host "`n== Building images" -ForegroundColor Cyan
    # Go deps are copied from the local module cache, so the build needs no network.
    Run go mod vendor
    Run docker build -f deploy/docker/go.Dockerfile --build-arg SERVICE=api -t photo-api:dev .
    Run docker build -f deploy/docker/go.Dockerfile --build-arg SERVICE=worker -t photo-worker:dev .
    if (-not $SkipAnalyzer) {
        Run docker build -f deploy/docker/analyzer.Dockerfile -t photo-analyzer:dev .
    }
}

# 3. Load images into kind. The kind node cannot reach Docker Hub,
#    so every image is copied from the laptop.
$images = @(
    "photo-api:dev", "photo-worker:dev", "photo-analyzer:dev",
    "postgres:17-alpine", "apache/kafka:4.1.0", "chrislusf/seaweedfs:latest",
    "quay.io/prometheus/prometheus:latest", "grafana/grafana:12.1.0"
)
Write-Host "`n== Loading images into kind" -ForegroundColor Cyan
foreach ($img in $images) {
    docker image inspect $img *> $null
    if ($LASTEXITCODE -ne 0) { throw "Image $img is not on this laptop. Pull or build it first." }
    Write-Host ">> kind load docker-image $img" -ForegroundColor DarkGray
    kind load docker-image $img --name $Cluster
    if ($LASTEXITCODE -ne 0) {
        # Fallback for multi-platform images: save only linux/amd64 and load the archive.
        Write-Host "   retrying via image archive (linux/amd64)" -ForegroundColor Yellow
        $tar = Join-Path $env:TEMP "kind-image.tar"
        Run docker save --platform linux/amd64 -o $tar $img
        Run kind load image-archive $tar --name $Cluster
        Remove-Item $tar -ErrorAction SilentlyContinue
    }
}

# 4. Manifests
Write-Host "`n== Applying manifests" -ForegroundColor Cyan
Run kubectl apply -k deploy

# Pods do not pick up a rebuilt image with the same tag by themselves.
if (-not $SkipBuild) {
    Run kubectl -n photo rollout restart deployment/api deployment/worker
    if (-not $SkipAnalyzer) { Run kubectl -n photo rollout restart deployment/analyzer }
}

# 5. Wait
Write-Host "`n== Waiting for pods (first start can take a few minutes)" -ForegroundColor Cyan
Run kubectl -n photo rollout status statefulset/postgres --timeout=180s
Run kubectl -n photo rollout status statefulset/kafka --timeout=240s
Run kubectl -n photo rollout status deployment/seaweedfs --timeout=180s
Run kubectl -n photo rollout status deployment/analyzer --timeout=240s
Run kubectl -n photo rollout status deployment/api --timeout=180s
Run kubectl -n photo rollout status deployment/worker --timeout=180s
Run kubectl -n photo get pods -o wide

Write-Host "`nReady:" -ForegroundColor Green
Write-Host "  API + Swagger: http://localhost:30080/docs"
Write-Host "  Grafana:       http://localhost:30300"
Write-Host "  Prometheus:    http://localhost:30090"
