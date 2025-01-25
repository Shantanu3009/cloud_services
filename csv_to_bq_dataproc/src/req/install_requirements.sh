#!/bin/bash

set -euxo pipefail

# Function to execute a command with retries
function execute_with_retries() {
  local -r cmd=$1

  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      echo "Command succeeded: $cmd"
      return
    fi
    echo "Retrying command: $cmd"
    sleep 5
  done

  echo "Command '$cmd' failed after 10 attempts."
  return 1
}

# Main function
function main() {
  echo "Installing required Python packages..."

  # Install necessary Python packages
  #pip install torch==1.12.1+cu113 --extra-index-url https://download.pytorch.org/whl/cu113
  #pip install retrying==1.3.4
  #pip install bs4==0.0.2
  pip install google-cloud-storage==2.11.0
  pip install google-cloud-bigquery==3.12.0
  pip install google-cloud-bigtable==2.22.0
  pip install pandas==2.0.3  # Adjust version based on Python version

  echo "All packages installed successfully."
}

# Call the main function
main
