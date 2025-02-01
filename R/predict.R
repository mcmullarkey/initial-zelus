suppressPackageStartupMessages({
  library(arrow)
  library(jsonlite)
  library(dplyr)
  library(tools)
  library(readr)
  library(stringr)
  library(recipes)
  library(parsnip)
  library(workflows)
  library(glmnet)
  library(duckplyr)
})

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) < 5) {
    stop("Usage: Rscript R/predict.R <input_file> <team> <overs> <order> <role>")
  }
  
  input_file <- args[1]
  team <- args[2]
  overs <- as.numeric(args[3])
  order <- args[4]  # "earliest" or "latest"
  role <- args[5]   # "batting" or "bowling"
  
  # Load and filter data
  raw_data <- load_data(input_file)
  filtered_data <- filter_team_data(raw_data, team, overs, order, role)
  
  model <- load_model("models/runs-avg-elnet-rds.rds")
  historical_data <- create_hist_runs()
  df_for_preds <- create_preds_df(filtered_data, historical_data)
  predictions <- make_predictions(model, df_for_preds)
  print_predictions(predictions)
}

# Modified function to handle the filtering logic with new parameters
filter_team_data <- function(df, team, overs, order, role) {
  # First, create the base dataframe with team information
  base_df <- df |>
    mutate(dates_dt = as.Date(dates)) |>
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
    )
  
  # Apply role-based filtering
  filtered_df <- base_df |>
    filter(
      case_when(
        role == "batting" ~ batting_team == team,
        role == "bowling" ~ bowling_team == team
      ),
      over <= overs
    ) |>
    distinct(over, .keep_all = TRUE)
  
  # Apply order-based filtering
  if (order == "latest") {
    filtered_df <- filtered_df |>
      slice_max(dates_dt, n = overs)
  } else {  # earliest
    filtered_df <- filtered_df |>
      slice_min(dates_dt, n = overs)
  }
  
  # Final selection and cleanup
  filtered_df |>
    slice_head(n = overs) |>
    select(-dates_dt)
}

# [Rest of the functions remain the same as in your current version]
load_model <- function(model_path) {
  return(readRDS(model_path))
}

load_data <- function(file_path) {
  ext <- file_ext(file_path)
  if (ext == "parquet") {
    df <- df_from_parquet(file_path) |>
      as_tibble()
  } else if (ext == "json") {
    df <- fromJSON(file_path, flatten = TRUE) |>
      as_tibble()
  } else {
    stop("Unsupported file format. Please provide a .parquet or .json file.")
  }
  return(df)
}

create_hist_runs <- function() {
  df_from_parquet("data/processed/intermediate_output.parquet") |>
    group_by(matchid, over, innings) |>
    summarize(total_runs = sum(runs.total, na.rm = TRUE), .groups = "drop") |>
    group_by(over) |>
    summarize(avg_runs_over = mean(total_runs, na.rm = TRUE))
}

create_preds_df <- function(df, hist_df) {
  df_hist_avg <- df |>
    left_join(hist_df, by = "over")
}

make_predictions <- function(workflow, new_data) {
  predictions <- predict(workflow, new_data = new_data)
  
  new_data <- new_data |>
    mutate(predicted_runs = predictions$.pred)
  
  return(new_data)
}

print_predictions <- function(predictions) {
  selected_cols <- predictions |>
    select(dates, batting_team, bowling_team, over, predicted_runs)
  print(selected_cols)
}

# Only run main() if this script is being run directly (not sourced)
if (sys.nframe() == 0) {
  main()
}
