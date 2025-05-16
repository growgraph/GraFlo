#!/bin/bash

# Usage: ./convert_pdfs.sh /path/to/search

SEARCH_DIR="${1:-.}"  # Default to current directory if no argument given

find "$SEARCH_DIR" -type f -iname "*.pdf" | while read -r pdf_file; do
    dir="$(dirname "$pdf_file")"
    base="$(basename "$pdf_file" .pdf)"
    output_path="${dir}/${base}.png"

    echo "Converting: $pdf_file -> $output_path"
    pdftoppm -png "$pdf_file" -r 300 > "$output_path"
done