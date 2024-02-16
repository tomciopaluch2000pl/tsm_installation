#!/bin/bash

# Function to copy a file to a specified location and handle existing files.
# Arguments:
#   $1 - source path of the file
#   $2 - destination directory path to copy the file to
function copy_file {
    # Check if all required arguments are provided
    if [ $# -ne 2 ]; then
        echo "Usage: $0 <source_path> <destination_directory>"
        return 1
    fi

    # Extract arguments
    local source_path=$1
    local destination_directory=$2

    # Check if source file exists
    if [ ! -e "$source_path" ]; then
        echo "Error: Source file $source_path does not exist."
        return 1
    fi

    # Extract filename from source path
    local filename=$(basename "$source_path")

    # Create full destination path
    local destination_path="$destination_directory/$filename"

    # Check if destination file exists
    if [ -f "$destination_path" ]; then
        # Rename existing file to _old_timestamp
        timestamp=$(date +"%Y%m%d_%H%M%S")
        mv "$destination_path" "${destination_path}_old_$timestamp"
        echo "Existing file $destination_path renamed to ${destination_path}_old_$timestamp"
    fi

    # Copy the file to the destination
    cp "$source_path" "$destination_path"
    echo "File $source_path copied to $destination_path"
}

# Call the function with provided arguments
copy_file "$1" "$2"
