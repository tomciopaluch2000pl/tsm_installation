#!/bin/bash

# Function to change owner, group, and permissions of a file or directory.
# Arguments:
#   $1 - path to the file or directory
#   $2 - new owner
#   $3 - new group
#   $4 - permissions (in octal format, e.g., 755)
#   $5 - (optional) "-R" to enable recursive change
function change_permissions {
    # Check if all required arguments are provided
    if [ $# -lt 3 ]; then
        echo "Usage: $0 <path> [<new_owner>] [<new_group>] [<permissions>] [<recursive_flag>]"
        return 1
    fi

    # Extract arguments
    local path=$1
    local owner=$2
    local group=$3
    local permissions=$4
    local recursive_flag=$5

    # Check if the path exists
    if [ ! -e "$path" ]; then
        echo "Error: $path does not exist."
        return 1
    fi

    # Change owner and group
    if [ -n "$owner" ]; then
        if [ -n "$group" ]; then
            chown_string="$owner:$group"
        else
            chown_string="$owner"
        fi
    elif [ -n "$group" ]; then
        chown_string=":$group"
    fi

    # Apply changes
    if [ -n "$chown_string" ]; then
        if [ "$recursive_flag" == "-R" ]; then
            chown -R "$chown_string" "$path"
        else
            chown "$chown_string" "$path"
        fi
    fi

    # Change permissions if provided
    if [ -n "$permissions" ]; then
        if [ "$recursive_flag" == "-R" ]; then
            chmod -R "$permissions" "$path"
        else
            chmod "$permissions" "$path"
        fi
        echo "Permissions changed for $path: owner=$owner, group=$group, permissions=$permissions"
    else
        echo "Ownership changed for $path: owner=$owner, group=$group"
    fi
}

# Call the function with provided arguments
change_permissions "$1" "$2" "$3" "$4" "$5"
