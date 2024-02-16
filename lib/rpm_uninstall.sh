#!/bin/bash

# The rpm_uninstall function uninstalls RPM packages matching the provided string
# in their package names, along with their dependencies.
# Arguments:
#   $1 - string to search for in package names
function rpm_uninstall {
    # Check if an argument has been provided
    if [ -z "$1" ]; then
        echo "No string provided to search for in package names."
        return 1
    fi

    # Find matching packages
    matched_packages=($(rpm -qa | grep "$1"))

    # Check if any matching packages are found
    if [ ${#matched_packages[@]} -eq 0 ]; then
        echo "No matching packages found."
        return 1
    fi

    # Iterate through the matched packages
    for package in "${matched_packages[@]}"; do
        # Display information about the package
        echo "Uninstalling package: $package"
        # Uninstall the package along with its dependencies
        yum remove -y "$package"
        echo "Package $package has been uninstalled."
    done
}
