#Requires -Version 5.1
<#
.SYNOPSIS
    Moon installer for Windows (PowerShell 5.1+).

.DESCRIPTION
    Downloads the latest (or specified) Moon release for Windows x86_64,
    verifies the SHA256 checksum, installs to %LOCALAPPDATA%\moon\bin
    (or %ProgramFiles%\moon\bin when running as Administrator), and
    appends the install directory to the user PATH if not already present.

.PARAMETER Version
    Specific version to install, e.g. "v0.2.0". Defaults to latest release.

.PARAMETER InstallDir
    Directory to install the moon.exe binary. Overrides the default.

.EXAMPLE
    irm https://raw.githubusercontent.com/pilotspace/moon/main/install.ps1 | iex
    # Or with overrides:
    $env:VERSION="v0.2.0"; irm .../install.ps1 | iex
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

# PowerShell 5.1 / older .NET defaults may negotiate TLS 1.0/1.1, which GitHub
# rejects ("Could not create SSL/TLS secure channel"). Force TLS 1.2 before any
# network call (same pattern as rustup-init.ps1).
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

# ── Version / directory from env overrides or params ─────────────────────────
$Version    = if ($env:VERSION)     { $env:VERSION }    else { $null }
$InstallDir = if ($env:INSTALL_DIR) { $env:INSTALL_DIR } else { $null }

# ── Helpers ───────────────────────────────────────────────────────────────────

function Write-Step  { param([string]$Msg) Write-Host $Msg }
function Write-Warn  { param([string]$Msg) Write-Warning $Msg }

function Test-IsAdmin {
    $id = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $p  = New-Object System.Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)
}

# ── Resolve latest version via GitHub redirect ────────────────────────────────
function Resolve-LatestVersion {
    $url = 'https://github.com/pilotspace/moon/releases/latest'
    # Use HEAD request; GitHub returns 302 → location header contains the tag
    try {
        $req = [System.Net.HttpWebRequest]::Create($url)
        $req.Method = 'HEAD'
        $req.AllowAutoRedirect = $false
        $req.Timeout = 15000
        $resp = $req.GetResponse()
        $location = $resp.Headers['Location']
        $resp.Close()
        if (-not $location) { throw 'No Location header in redirect response.' }
        return ($location -split '/tag/')[-1].Trim()
    } catch {
        throw "Could not resolve latest Moon version: $_"
    }
}

# ── Checksum verification ─────────────────────────────────────────────────────
function Confirm-Checksum {
    param(
        [string]$FilePath,
        [string]$ArtifactName,
        [string]$SumsFile
    )
    $content = Get-Content $SumsFile -Encoding UTF8
    $line    = $content | Where-Object { $_ -match " $([regex]::Escape($ArtifactName))$" }
    if (-not $line) {
        throw "Artifact '$ArtifactName' not found in SHA256SUMS.txt"
    }
    $expected = ($line -split '\s+')[0].Trim().ToUpperInvariant()
    $actual   = (Get-FileHash -Path $FilePath -Algorithm SHA256).Hash.ToUpperInvariant()
    if ($expected -ne $actual) {
        throw "Checksum mismatch!`n  expected: $expected`n  actual:   $actual"
    }
    Write-Step 'Checksum OK.'
}

# ── Main ──────────────────────────────────────────────────────────────────────

# Resolve version
if (-not $Version) {
    Write-Step 'Detecting latest Moon release…'
    $Version = Resolve-LatestVersion
}

Write-Step "Installing Moon $Version (x86_64-windows)"

# Artifact info (Windows ships as zip, x86_64-windows only)
$Artifact = "moon-${Version}-x86_64-windows.zip"
$BaseUrl  = "https://github.com/pilotspace/moon/releases/download/${Version}"

# Determine install dir
if (-not $InstallDir) {
    if (Test-IsAdmin) {
        $InstallDir = Join-Path $env:ProgramFiles 'moon\bin'
    } else {
        $InstallDir = Join-Path $env:LOCALAPPDATA 'moon\bin'
    }
}

# Temp directory with cleanup
$TmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ('moon-install-' + [System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $TmpDir | Out-Null
try {
    $ZipPath  = Join-Path $TmpDir $Artifact
    $SumsPath = Join-Path $TmpDir 'SHA256SUMS.txt'

    # Download tarball
    Write-Step "Downloading ${Artifact}…"
    $wc = New-Object System.Net.WebClient
    $wc.DownloadFile("${BaseUrl}/${Artifact}", $ZipPath)

    # Download checksum file
    Write-Step 'Downloading SHA256SUMS.txt…'
    $wc.DownloadFile("${BaseUrl}/SHA256SUMS.txt", $SumsPath)

    # Verify checksum — hard fail on mismatch
    Write-Step 'Verifying checksum…'
    Confirm-Checksum -FilePath $ZipPath -ArtifactName $Artifact -SumsFile $SumsPath

    # Extract zip
    Write-Step 'Extracting…'
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [System.IO.Compression.ZipFile]::ExtractToDirectory($ZipPath, $TmpDir)

    # Install binary
    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }
    $SrcBin = Join-Path $TmpDir 'moon.exe'
    if (-not (Test-Path $SrcBin)) {
        # Some archives nest under a subdirectory
        $SrcBin = Get-ChildItem -Path $TmpDir -Filter 'moon.exe' -Recurse |
                  Select-Object -First 1 -ExpandProperty FullName
    }
    if (-not $SrcBin -or -not (Test-Path $SrcBin)) {
        throw 'moon.exe not found in extracted archive'
    }
    Copy-Item -Path $SrcBin -Destination (Join-Path $InstallDir 'moon.exe') -Force
    Write-Step "Installed moon to ${InstallDir}\moon.exe"

} finally {
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}

# Append to user PATH if not already present
$UserPath = [System.Environment]::GetEnvironmentVariable('PATH', 'User')
if ($UserPath -notlike "*${InstallDir}*") {
    $NewPath = if ($UserPath) { "${UserPath};${InstallDir}" } else { $InstallDir }
    [System.Environment]::SetEnvironmentVariable('PATH', $NewPath, 'User')
    Write-Step "Added ${InstallDir} to your user PATH."
    Write-Step 'Restart your terminal for the PATH change to take effect.'
}

Write-Step ''
Write-Step 'Quick start:'
Write-Step '  moon --port 6379 --appendonly yes'
Write-Step '  redis-cli ping'
Write-Step ''
Write-Step 'NOTE: Windows binaries are not Authenticode-signed in this release.'
Write-Step '      If Windows SmartScreen warns you, click "More info" then "Run anyway".'
Write-Step '      Authenticode signing is planned for v0.3.0.'
Write-Step ''
Write-Step 'Documentation: https://github.com/pilotspace/moon'
