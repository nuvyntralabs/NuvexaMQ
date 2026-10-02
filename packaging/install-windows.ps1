# Installs the desktop app and puts nuvexamq on PATH.
param(
    [Parameter(Mandatory = $true)]
    [string] $PublishDir
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path (Join-Path $PublishDir "server\nuvexamq.exe"))) {
    throw "Missing $PublishDir\server\nuvexamq.exe. Run packaging\publish.ps1 first."
}
if (-not (Test-Path (Join-Path $PublishDir "desktop\Nuventra.NuvexaMQ.Desktop.exe"))) {
    throw "Missing the desktop executable. Run packaging\publish.ps1 first."
}

$dest = Join-Path $env:LOCALAPPDATA "Programs\NuvexaMQ"
if (Test-Path $dest) {
    Remove-Item -Recurse -Force $dest
}
New-Item -ItemType Directory -Force -Path (Join-Path $dest "server") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $dest "desktop") | Out-Null
Copy-Item -Recurse -Force (Join-Path $PublishDir "server\*") (Join-Path $dest "server")
Copy-Item -Recurse -Force (Join-Path $PublishDir "desktop\*") (Join-Path $dest "desktop")

$desktopExe = Join-Path $dest "desktop\Nuventra.NuvexaMQ.Desktop.exe"
$launcher = Join-Path $dest "server\nuvexamq-desktop.cmd"
@"
@echo off
start "" "$desktopExe"
"@ | Set-Content -Encoding ascii -Path $launcher

$startDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs"
New-Item -ItemType Directory -Force -Path $startDir | Out-Null
$shortcut = Join-Path $startDir "NuvexaMQ.lnk"
$shell = New-Object -ComObject WScript.Shell
$link = $shell.CreateShortcut($shortcut)
$link.TargetPath = $desktopExe
$link.WorkingDirectory = Join-Path $dest "desktop"
$link.Description = "NuvexaMQ"
$link.Save()

& (Join-Path $dest "server\nuvexamq.exe") path install
if ($LASTEXITCODE -ne 0) {
    throw "nuvexamq path install failed."
}
Write-Host "Installed $dest"
Write-Host "Open a new terminal and run: nuvexamq"
