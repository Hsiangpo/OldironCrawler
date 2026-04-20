param(
    [switch]$SkipDirBuild
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$venvPython = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
$pythonExe = if (Test-Path $venvPython) { $venvPython } else { "python" }
$distDir = Join-Path $repoRoot "dist"
$buildDir = Join-Path $repoRoot "build"
$specPath = Join-Path $PSScriptRoot "OldIronCrawler.spec"
$iconPath = Join-Path $PSScriptRoot "OldIronCrawler.ico"
$srcPath = Join-Path $repoRoot "src"
$entryScript = Join-Path $repoRoot "run.py"
$onedirName = "OldIronCrawler-dir"
$portableDir = Join-Path $distDir "OldIronCrawler"
$onefileExe = Join-Path $distDir "OldIronCrawler.exe"
$onedirDir = Join-Path $distDir $onedirName

function Stop-RunningOldIronCrawlerPackages {
    param(
        [string]$RepoDistDir
    )

    $normalizedDistDir = [System.IO.Path]::GetFullPath($RepoDistDir)
    $runningPackages = Get-Process OldIronCrawler -ErrorAction SilentlyContinue | Where-Object {
        try {
            $_.Path -and [System.IO.Path]::GetFullPath($_.Path).StartsWith($normalizedDistDir, [System.StringComparison]::OrdinalIgnoreCase)
        }
        catch {
            $false
        }
    }

    foreach ($process in $runningPackages) {
        Write-Output "Stopping running package: $($process.Id) $($process.Path)"
        Stop-Process -Id $process.Id -Force -ErrorAction Stop
        Wait-Process -Id $process.Id -Timeout 5 -ErrorAction SilentlyContinue
    }
}

function Remove-PathWithRetry {
    param(
        [string]$LiteralPath,
        [int]$Attempts = 20,
        [int]$DelayMilliseconds = 500
    )

    $lastErrorMessage = ""
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        if (-not (Test-Path $LiteralPath)) {
            return
        }
        try {
            Remove-Item -LiteralPath $LiteralPath -Recurse -Force -ErrorAction Stop
            if (-not (Test-Path $LiteralPath)) {
                return
            }
        }
        catch {
            $lastErrorMessage = $_.Exception.Message
        }
        Start-Sleep -Milliseconds $DelayMilliseconds
    }

    if (Test-Path $LiteralPath) {
        throw "无法清理路径：$LiteralPath。请先关闭占用它的进程；如果仍然拒绝访问，先运行 chkdsk /f 修复磁盘。原始错误：$lastErrorMessage"
    }
}

Push-Location $repoRoot
try {
    Stop-RunningOldIronCrawlerPackages -RepoDistDir $distDir

    Remove-PathWithRetry -LiteralPath $portableDir
    Remove-PathWithRetry -LiteralPath $onefileExe
    Remove-PathWithRetry -LiteralPath $onedirDir
    Remove-PathWithRetry -LiteralPath $buildDir

    if (-not $SkipDirBuild) {
        & $pythonExe -m PyInstaller `
            --noconfirm `
            --clean `
            --onedir `
            --console `
            --name $onedirName `
            --icon $iconPath `
            --paths $srcPath `
            --distpath $distDir `
            --workpath $buildDir `
            $entryScript
        if ($LASTEXITCODE -ne 0) {
            throw "Onedir build failed."
        }
    }

    Remove-PathWithRetry -LiteralPath $buildDir

    & $pythonExe -m PyInstaller `
        --noconfirm `
        --clean `
        --distpath $distDir `
        --workpath $buildDir `
        $specPath
    if ($LASTEXITCODE -ne 0) {
        throw "Onefile build failed."
    }

    $code = @'
import sys
from pathlib import Path

repo_root = Path(sys.argv[1]).resolve()
exe_path = Path(sys.argv[2]).resolve()
sys.path.insert(0, str(repo_root / "src"))

from oldironcrawler.package_layout import build_portable_dist_folder

package_root = build_portable_dist_folder(repo_root=repo_root, built_exe_path=exe_path)
print(package_root)
'@
    $builtExe = Join-Path $distDir "OldIronCrawler.exe"
    $packageRoot = & $pythonExe -c $code $repoRoot $builtExe
    if ($LASTEXITCODE -ne 0) {
        throw "Portable folder assembly failed."
    }

    Remove-PathWithRetry -LiteralPath $builtExe
    $validationDir = Join-Path $distDir $onedirName
    Remove-PathWithRetry -LiteralPath $validationDir
    $validationSpec = Join-Path $repoRoot "$onedirName.spec"
    if (Test-Path $validationSpec) {
        Remove-Item -LiteralPath $validationSpec -Force
    }

    Write-Output "Build complete: $packageRoot"
}
finally {
    Pop-Location
}
