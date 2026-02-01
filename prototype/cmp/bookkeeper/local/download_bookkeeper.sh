#!/usr/bin/env bash
set -e

BOOKKEEPER_VERSION="4.16.3"
BOOKKEEPER_DIR="bookkeeper-server-${BOOKKEEPER_VERSION}"
BOOKKEEPER_TARBALL="bookkeeper-server-${BOOKKEEPER_VERSION}-bin.tar.gz"
BOOKKEEPER_URL="https://archive.apache.org/dist/bookkeeper/bookkeeper-${BOOKKEEPER_VERSION}/${BOOKKEEPER_TARBALL}"

if [ -d "$BOOKKEEPER_DIR" ]; then
    echo "BookKeeper $BOOKKEEPER_VERSION already downloaded"
    exit 0
fi

echo "Downloading BookKeeper $BOOKKEEPER_VERSION..."
# Work in the local directory relative to script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Download if not exists
if [ ! -f "$BOOKKEEPER_TARBALL" ]; then
    curl -L -o "$BOOKKEEPER_TARBALL" "$BOOKKEEPER_URL"
fi

# Extract
echo "Extracting BookKeeper..."
tar -xzf "$BOOKKEEPER_TARBALL"

# Create symlink for easier access
ln -sf "bookkeeper-server-${BOOKKEEPER_VERSION}" bookkeeper

echo "BookKeeper downloaded to $BOOKKEEPER_DIR"
echo "Symlinked as local/bookkeeper"