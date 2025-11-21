#!/bin/bash

# --- Configuration ---
# You would need to update this to the latest CC index (e.g., CC-MAIN-2025-43)
CC_INDEX="CC-MAIN-2025-43"
INDEX_SERVER="https://index.commoncrawl.org"
CC_DATA_SERVER="https://data.commoncrawl.org"

# --- Script Logic ---

if [ -z "$1" ]; then
    echo "Usage: $0 <full_url>"
    echo "Example: $0 'https://duckdb.org/docs/stable/clients/adbc'"
    exit 1
fi

TARGET_URL="$1"

## 1. Query the Common Crawl Index Server

echo "🔍 Querying Common Crawl Index for: $TARGET_URL (Index: $CC_INDEX)"
echo "--------------------------------------------------"

# Construct the index query URL, using * for flexibility, and requesting JSON output
INDEX_QUERY="$INDEX_SERVER/$CC_INDEX-index?url=$TARGET_URL*&output=json"

# Use curl to fetch the JSON record. We need the raw JSON line for parsing.
# The index server returns a JSON object per line.
INDEX_DATA=$(curl -s "$INDEX_QUERY")

if [ -z "$INDEX_DATA" ]; then
    echo "❌ Error: Could not retrieve data from the index server."
    exit 1
fi

## 2. Extract Data (Offset, Length, and Filename) using JQ

# Since the index returns one JSON object per line, we use 'tail -n 1'
# to take the last record (usually the most recent one if multiple exist)
# and then use JQ to parse the required fields.

# Note: The 'fromjson' filter is crucial because the index returns raw JSON lines,
# not a JSON array.

OFFSET=$(echo "$INDEX_DATA" | tail -n 1 | jq -r '.offset')
LENGTH=$(echo "$INDEX_DATA" | tail -n 1 | jq -r '.length')
FILENAME=$(echo "$INDEX_DATA" | tail -n 1 | jq -r '.filename')

# Check if data extraction was successful
if [ -z "$OFFSET" ] || [ -z "$LENGTH" ] || [ -z "$FILENAME" ] || [ "$OFFSET" == "null" ]; then
    echo "❌ Error: Failed to extract offset, length, or filename from index data."
    echo "Raw Index Response: $INDEX_DATA"
    exit 1
fi

## 3. Calculate Byte Range and Construct the WARC URL

END_BYTE=$((OFFSET + LENGTH - 1))
RANGE_HEADER="$OFFSET-$END_BYTE"
WARC_URL="$CC_DATA_SERVER/$FILENAME"

echo "✅ Index Data Found:"
echo "   Offset: $OFFSET, Length: $LENGTH"
echo "   File: $FILENAME"
echo "   Range: bytes=$RANGE_HEADER"
echo "--------------------------------------------------"

## 4. Fetch the Content using HTTP Range Request and Decompress

echo "⬇️ Downloading only the specific byte range and decompressing..."

# Use curl with the -r (range) flag to request the specific bytes,
# then pipe the output to 'gzip -dc' to decompress and print the content.
# -s for silent mode
curl -s -r"$RANGE_HEADER" "$WARC_URL" | gzip -dc

echo
echo "--------------------------------------------------"
echo "✨ Content retrieval complete."