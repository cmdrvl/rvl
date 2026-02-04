#!/usr/bin/env pwsh

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Info {
    param([string]$Message)
    Write-Host $Message
}

function Write-Warn {
    param([string]$Message)
    Write-Warning $Message
}

function Write-Fail {
    param([string]$Message)
    Write-Error $Message
    exit 1
}

function Get-InstallDir {
    if ($env:RVL_INSTALL_DIR -and $env:RVL_INSTALL_DIR.Trim() -ne '') {
        return $env:RVL_INSTALL_DIR
    }

    $localAppData = [Environment]::GetFolderPath('LocalApplicationData')
    if (-not $localAppData) {
        Write-Fail 'error: unable to resolve LocalApplicationData path'
    }

    return (Join-Path $localAppData 'rvl\bin')
}

function Get-LatestVersion {
    $headers = @{ 'User-Agent' = 'rvl-install' }
    $response = Invoke-WebRequest -Uri 'https://api.github.com/repos/cmdrvl/rvl/releases/latest' -UseBasicParsing -Headers $headers
    $payload = $response.Content | ConvertFrom-Json
    if (-not $payload.tag_name) {
        Write-Fail 'error: unable to resolve latest version tag'
    }
    return $payload.tag_name
}

function Normalize-Version {
    param([string]$Version)
    if ($Version.StartsWith('v')) {
        return $Version
    }
    return "v$Version"
}

function Get-TargetTriplet {
    $arch = $env:PROCESSOR_ARCHITECTURE
    if ($arch -eq 'AMD64' -or $arch -eq 'x86_64') {
        return 'x86_64-pc-windows-msvc'
    }

    Write-Fail "error: unsupported Windows architecture: $arch"
    return ''
}

try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
} catch {
    # ignore if not supported
}

$version = $env:RVL_VERSION
if (-not $version -or $version.Trim() -eq '') {
    Write-Info 'No RVL_VERSION set; resolving latest release...'
    $version = Get-LatestVersion
}

$version = Normalize-Version $version
$target = Get-TargetTriplet
$assetName = "rvl-$version-$target.zip"
$baseUrl = "https://github.com/cmdrvl/rvl/releases/download/$version"

$installDir = Get-InstallDir
$versionedBinary = Join-Path $installDir "rvl@$version.exe"
$activeBinary = Join-Path $installDir 'rvl.exe'

$tempRoot = Join-Path ([IO.Path]::GetTempPath()) ("rvl-install-" + [guid]::NewGuid().ToString())
$zipPath = Join-Path $tempRoot $assetName
$shaPath = Join-Path $tempRoot 'SHA256SUMS'
$extractDir = Join-Path $tempRoot 'extract'

Write-Info "Installing rvl $version for $target"
Write-Info "Install dir: $installDir"

New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null

try {
    Write-Info "Downloading $assetName..."
    Invoke-WebRequest -Uri "$baseUrl/$assetName" -UseBasicParsing -OutFile $zipPath

    Write-Info 'Downloading SHA256SUMS...'
    Invoke-WebRequest -Uri "$baseUrl/SHA256SUMS" -UseBasicParsing -OutFile $shaPath

    $expectedHash = $null
    foreach ($line in Get-Content -Path $shaPath) {
        if ($line -match '^([a-fA-F0-9]{64})\s+(.+)$') {
            $hash = $Matches[1]
            $name = $Matches[2]
            if ($name -eq $assetName) {
                $expectedHash = $hash
                break
            }
        }
    }

    if (-not $expectedHash) {
        Write-Fail "error: checksum for $assetName not found in SHA256SUMS"
    }

    $actualHash = (Get-FileHash -Path $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actualHash -ne $expectedHash.ToLowerInvariant()) {
        Write-Fail "error: checksum mismatch for $assetName"
    }

    Write-Info 'Checksum verified.'

    New-Item -ItemType Directory -Force -Path $extractDir | Out-Null
    Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force

    $binaryPath = Join-Path $extractDir 'rvl.exe'
    if (-not (Test-Path $binaryPath)) {
        $binaryPath = Get-ChildItem -Path $extractDir -Recurse -Filter 'rvl.exe' | Select-Object -First 1 | ForEach-Object { $_.FullName }
    }

    if (-not $binaryPath) {
        Write-Fail 'error: rvl.exe not found in archive'
    }

    New-Item -ItemType Directory -Force -Path $installDir | Out-Null
    Copy-Item -Force $binaryPath $versionedBinary
    Copy-Item -Force $binaryPath $activeBinary

    Write-Info "Installed $versionedBinary"
    Write-Info "Installed $activeBinary"

    Write-Info 'Running self-test...'
    & $activeBinary --version | Out-Null
    & $activeBinary --help | Out-Null

    Write-Info 'Self-test complete.'

    if (-not ($env:PATH -split ';' | Where-Object { $_ -eq $installDir })) {
        Write-Warn "rvl is not on PATH. Add it with:"
        Write-Host "  setx PATH `"$env:PATH;$installDir`""
        Write-Host "Then restart your shell."
    }

    Write-Info 'Install complete.'
    Write-Info "Rollback: copy $versionedBinary over $activeBinary"
} finally {
    if (Test-Path $tempRoot) {
        Remove-Item -Recurse -Force $tempRoot
    }
}
