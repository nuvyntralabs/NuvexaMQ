#!/usr/bin/env bash
# Publishes the desktop app and the nuvexamq server into artifacts/publish.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RID="${1:-}"
if [[ -z "$RID" ]]; then
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) RID=osx-arm64 ;;
    Darwin-x86_64) RID=osx-x64 ;;
    Linux-x86_64) RID=linux-x64 ;;
    Linux-aarch64|Linux-arm64) RID=linux-arm64 ;;
    *)
      echo "Pass a runtime id, for example: ./packaging/publish.sh osx-arm64" >&2
      exit 1
      ;;
  esac
fi
OUT="$ROOT/artifacts/publish"
rm -rf "$OUT"
mkdir -p "$OUT"
dotnet publish "$ROOT/src/Nuventra.NuvexaMQ.Server/Nuventra.NuvexaMQ.Server.csproj" \
  -c Release -f net10.0 -r "$RID" --self-contained true \
  -o "$OUT/server" --nologo -p:NuGetAudit=false -p:RestoreIgnoreFailedSources=true
dotnet publish "$ROOT/src/Nuventra.NuvexaMQ.Desktop/Nuventra.NuvexaMQ.Desktop.csproj" \
  -c Release -f net10.0 -r "$RID" --self-contained true \
  -o "$OUT/desktop" --nologo -p:NuGetAudit=false -p:RestoreIgnoreFailedSources=true
echo "Published $OUT ($RID)"
