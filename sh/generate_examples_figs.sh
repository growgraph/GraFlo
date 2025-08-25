#!/bin/bash

# Base directory to search for examples
BASE_DIR="../examples/"

# Check if base directory exists
if [[ ! -d "$BASE_DIR" ]]; then
    echo "Error: Directory $BASE_DIR does not exist"
    exit 1
fi

# Find all subdirectories with schema.yaml
for subdir in "$BASE_DIR"*/; do
    # Skip if not a directory
    [[ ! -d "$subdir" ]] && continue

    schema_file="${subdir}schema.yaml"
    output_dir="${subdir}figs/"

    # Create assets dir path: strip BASE_DIR prefix and add docs/assets
    subdir_name=$(basename "$subdir")
    assets_dir="../docs/assets/${subdir_name}/figs/"

    # Check if schema.yaml exists
    if [[ -f "$schema_file" ]]; then
        echo "Processing: $(basename "$subdir")"

        # Create output directory if it doesn't exist
        mkdir -p "$output_dir"

        # Run plot_schema
        if plot_schema -c "$schema_file" -o "$output_dir"; then
            echo "  ✓ Generated plots"

            # Convert PDFs to PNGs using local conv.sh script
            if [[ -x "./conv.sh" ]]; then
                echo "  Converting PDFs to PNGs..."
                ./conv.sh "$output_dir" "$output_dir"

                # Create assets directory and copy files
                mkdir -p "$assets_dir"
                cp -r "$output_dir"*.png "$assets_dir"
                echo "  ✓ Copied to $assets_dir"
            else
                echo "  Warning: conv.sh not found or not executable"
            fi
        else
            echo "  ✗ Failed to generate plots"
        fi

        echo ""
    else
        echo "Skipping $(basename "$subdir"): no schema.yaml found"
    fi
done

echo "Processing complete."