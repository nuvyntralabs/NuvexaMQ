#!/usr/bin/env bash
# Installs the desktop app and puts nuvexamq on PATH.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SRC="${1:?Usage: install-macos.sh /path/to/artifacts/publish}"
APP="${2:-$HOME/Applications/NuvexaMQ.app}"
if [[ ! -f "$SRC/server/nuvexamq" ]]; then
  echo "Missing $SRC/server/nuvexamq. Run packaging/publish.sh first." >&2
  exit 1
fi
if [[ ! -f "$SRC/desktop/Nuventra.NuvexaMQ.Desktop" ]]; then
  echo "Missing $SRC/desktop/Nuventra.NuvexaMQ.Desktop. Run packaging/publish.sh first." >&2
  exit 1
fi
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources/server" "$HOME/Applications"
cp "$HERE/Info.plist" "$APP/Contents/Info.plist"
cp -R "$SRC/desktop/." "$APP/Contents/MacOS/"
cp -R "$SRC/server/." "$APP/Contents/Resources/server/"
chmod +x "$APP/Contents/MacOS/Nuventra.NuvexaMQ.Desktop" "$APP/Contents/Resources/server/nuvexamq"
cat > "$APP/Contents/Resources/server/nuvexamq-desktop" <<EOF
#!/bin/sh
open $(printf '%q' "$APP")
EOF
chmod +x "$APP/Contents/Resources/server/nuvexamq-desktop"
LSREGISTER="/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister"
if [[ -x "$LSREGISTER" ]]; then
  "$LSREGISTER" -f "$APP" || true
fi
"$APP/Contents/Resources/server/nuvexamq" path install
echo "Installed $APP"
echo "Open a new terminal and run: nuvexamq"
