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
suppressMessages(library(arrow));
suppressMessages(library(dplyr));
suppressMessages(library(duckplyr));
suppressMessages(library(stringr));
df <- df_from_parquet('$INPUT_FILE') |>
  mutate(dates_dt = as.Date(dates)) |> 
  arrange(dates_dt) |>
  group_by(matchid) |>
  group_by(matchid) |>
  mutate(
    batting_team = team,
    all_teams = toString(sort(unique(team))),
    bowling_team = str_trim(
      case_when(
        team == str_extract(all_teams, '^[^,]+') ~
          str_extract(all_teams, '[^,]+$'),
        TRUE ~ str_extract(all_teams, '^[^,]+')
      )
    )
  ) |>
  select(-all_teams) |>
  ungroup() |>
  select(
  matchid,
  batting_team,
  bowling_team,
  dates,
  dates_dt,
  innings,
  over
  ) |>
  filter(batting_team == '$TEAM') |> 
  distinct(over, .keep_all = TRUE) |> 
  slice_min(dates_dt,n = 5) |> 
  slice_head(n = 5) |> 
  select(-dates_dt);
write_parquet(df, '$FILTERED_FILE');
"

# Run predictions
Rscript R/predict.R "$FILTERED_FILE"


