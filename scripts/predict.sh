#!/bin/bash

# Ensure script stops on error
set -e

# Check for required arguments
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <team> <overs>"
  exit 1
fi

TEAM="$1"
OVERS="$2"

# Define paths
INPUT_FILE="data/processed/intermediate_output.parquet"
FILTERED_FILE="data/processed/${TEAM}_first${OVERS}.parquet"

# Filter for the given team's first N overs using R
Rscript -e "
library(arrow);
library(dplyr);
df <- read_parquet('$INPUT_FILE') |>
  mutate(dates = as.Date(dates)) |> 
  arrange(dates) |> 
  mutate(dates = as.character(dates)) |> 
  group_by(matchid) |>
  mutate(
    batting_team = first(team),
    bowling_team = last(team)
  ) |>
  ungroup() |>
  select(
  matchid,
  batting_team,
  bowling_team,
  dates,
  innings,
  over
  ) |>
  filter(batting_team == '$TEAM') |> 
  distinct(over, .keep_all = TRUE) |> 
  slice_head(n = 5);
write_parquet(df, '$FILTERED_FILE');
"

# Run predictions
Rscript R/predict.R "$FILTERED_FILE"


