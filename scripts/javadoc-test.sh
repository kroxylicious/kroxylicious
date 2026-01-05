#!/usr/bin/env bash

SOURCE_JAVADOC="./target/javadoc-web"
DEST_JAVADOC="../website/javadoc"
RAW_SUBDIR="raw"

echo "Nuking and recreating $DEST_JAVADOC..."
rm -rf "$DEST_JAVADOC"
mkdir -p "$DEST_JAVADOC/$RAW_SUBDIR"

# 1. Copy original Javadoc files into the hidden subdirectory
# These will be served as static files by Jekyll/GitHub Pages
cp -R "$SOURCE_JAVADOC"/* "$DEST_JAVADOC/$RAW_SUBDIR/"

# 2. Generate a Jekyll wrapper for every HTML file
find "$DEST_JAVADOC/$RAW_SUBDIR" -name "*.html" | while read -r raw_file; do
    # Calculate the relative path (e.g., io/kroxylicious/proxy/KafkaProxy.html)
    rel_path=${raw_file#$DEST_JAVADOC/$RAW_SUBDIR/}
    wrapper_file="$DEST_JAVADOC/$rel_path"

    mkdir -p "$(dirname "$wrapper_file")"

    # Create the wrapper with front matter pointing to the raw content
    cat <<EOF > "$wrapper_file"
---
layout: javadoc-wrapper
raw_path: "/javadoc/$RAW_SUBDIR/$rel_path"
---
EOF
done

echo "Done! Wrappers generated for all Javadoc pages."