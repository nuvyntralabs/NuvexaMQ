#!/usr/bin/env bash
# Builds an MSI, PKG, or DEB and RPM for one runtime id.
# Usage: packaging/pack.sh <version> <rid> <output-dir>
set -euo pipefail
export COPYFILE_DISABLE=1
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="${1:?Usage: packaging/pack.sh <version> <rid> <output-dir>}"
RID="${2:?Usage: packaging/pack.sh <version> <rid> <output-dir>}"
OUTDIR="${3:?Usage: packaging/pack.sh <version> <rid> <output-dir>}"
PACKAGING="$ROOT/packaging"
PUBLISH="$ROOT/artifacts/publish-$RID"
STAGE="$ROOT/artifacts/stage-$RID"
SERVER_PROJ="$ROOT/src/Nuventra.NuvexaMQ.Server/Nuventra.NuvexaMQ.Server.csproj"
DESKTOP_PROJ="$ROOT/src/Nuventra.NuvexaMQ.Desktop/Nuventra.NuvexaMQ.Desktop.csproj"

rm -rf "$PUBLISH" "$STAGE"
mkdir -p "$PUBLISH/server" "$PUBLISH/desktop" "$STAGE" "$OUTDIR"

publish_common=(
  -c Release -f net10.0 -r "$RID" --self-contained true --nologo
  -p:NuGetAudit=false
  -p:RestoreIgnoreFailedSources=true
  -p:DebugType=embedded
  -p:CopyOutputSymbolsToPublishDirectory=false
  -p:IncludeNativeLibrariesForSelfExtract=true
  -p:EnableCompressionInSingleFile=true
)

dotnet publish "$SERVER_PROJ" "${publish_common[@]}" -p:PublishSingleFile=true -o "$PUBLISH/server"

desktop_single=true
if [[ "$RID" == osx-* ]]; then
  desktop_single=false
fi
if [[ "$desktop_single" == true ]]; then
  dotnet publish "$DESKTOP_PROJ" "${publish_common[@]}" -p:PublishSingleFile=true -o "$PUBLISH/desktop"
else
  dotnet publish "$DESKTOP_PROJ" "${publish_common[@]}" -p:PublishSingleFile=false -o "$PUBLISH/desktop"
fi
find "$PUBLISH" \( -name '*.pdb' -o -name '*.staticwebassets.endpoints.json' -o -name '._*' -o -name '.DS_Store' \) -delete

case "$RID" in
  win-*)
    if [[ ! -f "$PUBLISH/server/nuvexamq.exe" ]]; then
      echo "Missing $PUBLISH/server/nuvexamq.exe" >&2
      exit 1
    fi
    if [[ ! -f "$PUBLISH/desktop/Nuventra.NuvexaMQ.Desktop.exe" ]]; then
      echo "Missing the desktop executable." >&2
      exit 1
    fi
    cat > "$PUBLISH/server/nuvexamq-desktop.cmd" <<'EOF'
@echo off
start "" "%~dp0..\desktop\Nuventra.NuvexaMQ.Desktop.exe"
EOF
    export PATH="$PATH:$HOME/.dotnet/tools"
    if ! command -v wix >/dev/null 2>&1; then
      echo "wix CLI not found. Install with: dotnet tool install -g wix --version 5.0.2" >&2
      exit 1
    fi
    case "$RID" in
      win-arm64) wix_arch=arm64 ;;
      *) wix_arch=x64 ;;
    esac
    server_dir="$PUBLISH/server"
    desktop_dir="$PUBLISH/desktop"
    wxs="$PACKAGING/nuvexamq.wxs"
    msi="$OUTDIR/NuvexaMQ-${VERSION}-${RID}.msi"
    if command -v cygpath >/dev/null 2>&1; then
      server_dir="$(cygpath -m "$server_dir")"
      desktop_dir="$(cygpath -m "$desktop_dir")"
      wxs="$(cygpath -m "$wxs")"
      msi="$(cygpath -m "$msi")"
    fi
    wix build "$wxs" \
      -arch "$wix_arch" \
      -d "ProductVersion=$VERSION" \
      -d "ServerDir=$server_dir" \
      -d "DesktopDir=$desktop_dir" \
      -o "$msi"
    ;;
  osx-*)
    if [[ ! -f "$PUBLISH/server/nuvexamq" ]]; then
      echo "Missing $PUBLISH/server/nuvexamq" >&2
      exit 1
    fi
    if [[ ! -f "$PUBLISH/desktop/Nuventra.NuvexaMQ.Desktop" ]]; then
      echo "Missing the desktop executable." >&2
      exit 1
    fi
    if ! command -v pkgbuild >/dev/null 2>&1; then
      echo "pkgbuild not found. Build the macOS package on macOS." >&2
      exit 1
    fi
    app="$STAGE/root/Applications/NuvexaMQ.app"
    mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources/server" \
      "$STAGE/root/Library/LaunchDaemons" "$STAGE/root/etc/paths.d" \
      "$STAGE/scripts"
    cp -R "$PUBLISH/desktop/." "$app/Contents/MacOS/"
    cp -R "$PUBLISH/server/." "$app/Contents/Resources/server/"
    chmod 755 "$app/Contents/MacOS/Nuventra.NuvexaMQ.Desktop" \
      "$app/Contents/Resources/server/nuvexamq"
    cat > "$app/Contents/Resources/server/nuvexamq-desktop" <<'EOF'
#!/bin/sh
open "/Applications/NuvexaMQ.app"
EOF
    chmod 755 "$app/Contents/Resources/server/nuvexamq-desktop"
    cp "$PACKAGING/macos/nuvexamq.icns" "$app/Contents/Resources/nuvexamq.icns"
    python3 - "$PACKAGING/Info.plist" "$app/Contents/Info.plist" "$VERSION" <<'PY'
from pathlib import Path
import sys
src, dest, version = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
text = src.read_text(encoding="utf-8").replace("<string>0.1.0</string>", f"<string>{version}</string>")
dest.write_text(text, encoding="utf-8")
PY
    cp "$PACKAGING/macos/com.nuventra.nuvexamq.plist" \
      "$STAGE/root/Library/LaunchDaemons/com.nuventra.nuvexamq.plist"
    printf '%s\n' "/Applications/NuvexaMQ.app/Contents/Resources/server" \
      > "$STAGE/root/etc/paths.d/nuvexamq"
    cp "$PACKAGING/macos/postinstall" "$STAGE/scripts/postinstall"
    chmod 755 "$STAGE/scripts/postinstall"
    find "$STAGE/root" \( -name '._*' -o -name '.DS_Store' \) -delete
    if command -v codesign >/dev/null 2>&1; then
      codesign --force --deep --sign - "$app" || true
    fi
    if command -v xattr >/dev/null 2>&1; then
      xattr -cr "$STAGE/root" || true
    fi
    find "$STAGE/root" \( -name '._*' -o -name '.DS_Store' \) -delete
    pkgbuild \
      --root "$STAGE/root" \
      --scripts "$STAGE/scripts" \
      --component-plist "$PACKAGING/macos/component.plist" \
      --identifier com.nuventra.nuvexamq \
      --version "$VERSION" \
      --install-location / \
      "$OUTDIR/NuvexaMQ-${VERSION}-${RID}.pkg"
    ;;
  linux-*)
    if ! command -v fpm >/dev/null 2>&1; then
      echo "fpm not found. Install ruby and fpm, plus rpmbuild for the .rpm." >&2
      exit 1
    fi
    case "$RID" in
      linux-x64) deb_arch=amd64; rpm_arch=x86_64 ;;
      linux-arm64) deb_arch=arm64; rpm_arch=aarch64 ;;
      *)
        echo "Unsupported Linux RID: $RID" >&2
        exit 1
        ;;
    esac
    if [[ ! -f "$PUBLISH/server/nuvexamq" || ! -f "$PUBLISH/desktop/Nuventra.NuvexaMQ.Desktop" ]]; then
      echo "Published Linux binaries were not found in $PUBLISH" >&2
      exit 1
    fi
    rootfs="$STAGE/root"
    mkdir -p "$rootfs/usr/lib/nuvexamq" "$rootfs/usr/lib/nuvexamq-desktop" \
      "$rootfs/usr/bin" "$rootfs/usr/lib/systemd/system" "$rootfs/usr/share/applications" \
      "$rootfs/usr/share/icons/hicolor/512x512/apps"
    cp -R "$PUBLISH/server/." "$rootfs/usr/lib/nuvexamq/"
    cp -R "$PUBLISH/desktop/." "$rootfs/usr/lib/nuvexamq-desktop/"
    chmod 755 "$rootfs/usr/lib/nuvexamq/nuvexamq" \
      "$rootfs/usr/lib/nuvexamq-desktop/Nuventra.NuvexaMQ.Desktop"
    ln -s ../lib/nuvexamq/nuvexamq "$rootfs/usr/bin/nuvexamq"
    cat > "$rootfs/usr/bin/nuvexamq-desktop" <<'EOF'
#!/bin/sh
exec /usr/lib/nuvexamq-desktop/Nuventra.NuvexaMQ.Desktop "$@"
EOF
    chmod 755 "$rootfs/usr/bin/nuvexamq-desktop"
    cp "$PACKAGING/linux/nuvexamq.service" "$rootfs/usr/lib/systemd/system/nuvexamq.service"
    cp "$PACKAGING/linux/nuvexamq.png" "$rootfs/usr/share/icons/hicolor/512x512/apps/nuvexamq.png"
    sed 's|^Exec=nuvexamq-desktop|Exec=/usr/bin/nuvexamq-desktop|' \
      "$PACKAGING/nuvexamq.desktop" > "$rootfs/usr/share/applications/nuvexamq.desktop"
    chmod 755 "$PACKAGING/linux/postinst" "$PACKAGING/linux/prerm" "$PACKAGING/linux/postrm"
    fpm_flags=(
      -s dir
      -n nuvexamq
      -v "$VERSION"
      --iteration 1
      --description "NuvexaMQ message broker"
      --license MIT
      --url https://github.com/nuvyntralabs/NuvexaMQ
      --maintainer "Niladri Prasad Padhy"
      --after-install "$PACKAGING/linux/postinst"
      --before-remove "$PACKAGING/linux/prerm"
      --after-remove "$PACKAGING/linux/postrm"
      --config-files usr/lib/nuvexamq/appsettings.json
      -C "$rootfs"
    )
    fpm "${fpm_flags[@]}" -t deb -a "$deb_arch" \
      -p "$OUTDIR/nuvexamq_${VERSION}_${deb_arch}.deb" \
      usr
    fpm "${fpm_flags[@]}" -t rpm -a "$rpm_arch" \
      -p "$OUTDIR/nuvexamq-${VERSION}-1.${rpm_arch}.rpm" \
      usr
    ;;
  *)
    echo "Unsupported RID: $RID" >&2
    exit 1
    ;;
esac

echo "Packed NuvexaMQ $VERSION for $RID into $OUTDIR"
ls -lh "$OUTDIR"
