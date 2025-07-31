#!/bin/bash

# Usage: ./convert_pdfs.sh /path/to/input_root /path/to/output_root

INPUT_ROOT="$1"
OUTPUT_ROOT="$2"

if [[ -z "$INPUT_ROOT" || -z "$OUTPUT_ROOT" ]]; then
    echo "Usage: $0 /path/to/input_root /path/to/output_root"
    exit 1
fi

find "$INPUT_ROOT" -type f -iname "*.pdf" | while read -r pdf_file; do
    # Strip input root and leading slash to get relative path
    relative_path="${pdf_file#$INPUT_ROOT/}"
    relative_dir="$(dirname "$relative_path")"
    base_name="$(basename "$pdf_file" .pdf)"

    # Construct output directory and file path
    output_dir="${OUTPUT_ROOT}/${relative_dir}"
    output_file="${output_dir}/${base_name}.png"

    # Create output directory if it doesn't exist
    mkdir -p "$output_dir"

    echo "Converting: $pdf_file -> $output_file"
    pdftoppm -png "$pdf_file" -r 300 > "$output_file"
done
