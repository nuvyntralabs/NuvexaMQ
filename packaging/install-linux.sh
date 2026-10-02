#!/usr/bin/env bash
# Installs the desktop app and puts nuvexamq on PATH.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SRC="${1:?Usage: install-linux.sh /path/to/artifacts/publish}"
DEST="${2:-$HOME/.local/share/nuvexamq}"
if [[ ! -f "$SRC/server/nuvexamq" ]]; then
  echo "Missing $SRC/server/nuvexamq. Run packaging/publish.sh first." >&2
  exit 1
fi
if [[ ! -f "$SRC/desktop/Nuventra.NuvexaMQ.Desktop" ]]; then
  echo "Missing $SRC/desktop/Nuventra.NuvexaMQ.Desktop. Run packaging/publish.sh first." >&2
  exit 1
fi
rm -rf "$DEST"
mkdir -p "$DEST/server" "$DEST/desktop"
cp -R "$SRC/server/." "$DEST/server/"
cp -R "$SRC/desktop/." "$DEST/desktop/"
chmod +x "$DEST/server/nuvexamq" "$DEST/desktop/Nuventra.NuvexaMQ.Desktop"
cat > "$DEST/server/nuvexamq-desktop" <<EOF
#!/bin/sh
exec $(printf '%q' "$DEST/desktop/Nuventra.NuvexaMQ.Desktop") "\$@"
EOF
chmod +x "$DEST/server/nuvexamq-desktop"
APPDIR="${XDG_DATA_HOME:-$HOME/.local/share}/applications"
mkdir -p "$APPDIR"
sed "s|^Exec=nuvexamq-desktop|Exec=$DEST/desktop/Nuventra.NuvexaMQ.Desktop|" "$HERE/nuvexamq.desktop" > "$APPDIR/nuvexamq.desktop"
update-desktop-database "$APPDIR" >/dev/null 2>&1 || true
"$DEST/server/nuvexamq" path install
echo "Installed $DEST"
echo "Open a new terminal and run: nuvexamq"
