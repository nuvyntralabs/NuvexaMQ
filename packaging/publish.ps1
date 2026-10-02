# Publishes the desktop app and the nuvexamq server into artifacts/publish.
param(
    [string] $Rid = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
if (-not $Rid) {
    $arch = if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -eq "Arm64") { "arm64" } else { "x64" }
    $Rid = "win-$arch"
}
$out = Join-Path $root "artifacts\publish"
if (Test-Path $out) {
    Remove-Item -Recurse -Force $out
}
New-Item -ItemType Directory -Force -Path $out | Out-Null
dotnet publish (Join-Path $root "src\Nuventra.NuvexaMQ.Server\Nuventra.NuvexaMQ.Server.csproj") `
    -c Release -f net10.0 -r $Rid --self-contained true `
    -o (Join-Path $out "server") --nologo -p:NuGetAudit=false -p:RestoreIgnoreFailedSources=true
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
dotnet publish (Join-Path $root "src\Nuventra.NuvexaMQ.Desktop\Nuventra.NuvexaMQ.Desktop.csproj") `
    -c Release -f net10.0 -r $Rid --self-contained true `
    -o (Join-Path $out "desktop") --nologo -p:NuGetAudit=false -p:RestoreIgnoreFailedSources=true
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Published $out ($Rid)"
