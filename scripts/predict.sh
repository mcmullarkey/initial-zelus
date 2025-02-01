#!/bin/bash

# Ensure script stops on error
set -e

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -team|--team)
      TEAM="$2"
      shift 2
      ;;
    -overs|--overs)
      OVERS="$2"
      shift 2
      ;;
    -order|--order)
      ORDER="$2"
      shift 2
      ;;
    -role|--role)
      ROLE="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Check for required arguments and set defaults
if [ -z "$TEAM" ] || [ -z "$OVERS" ]; then
  echo "Usage: $0 -team <team> -overs <overs> [-order <earliest|latest>] [-role <batting|bowling>]"
  echo "Example: $0 -team 'Ireland' -overs 5 -order latest -role bowling"
  echo "Defaults: -order earliest -role batting"
  exit 1
fi

# Set defaults if not specified
ORDER="${ORDER:-earliest}"
ROLE="${ROLE:-batting}"

# Validate order argument
if [ "$ORDER" != "earliest" ] && [ "$ORDER" != "latest" ]; then
  echo "Error: order must be either 'earliest' or 'latest'"
  exit 1
fi

# Validate role argument
if [ "$ROLE" != "batting" ] && [ "$ROLE" != "bowling" ]; then
  echo "Error: role must be either 'batting' or 'bowling'"
  exit 1
fi

# Define input file
INPUT_FILE="data/processed/intermediate_output.parquet"

# Run predictions
Rscript R/predict.R "$INPUT_FILE" "$TEAM" "$OVERS" "$ORDER" "$ROLE"


