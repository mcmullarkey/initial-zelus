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
  if (length(args) < 1) {
    stop("Usage: Rscript R/predict.R <input_file>")
  }

  input_file <- args[1]
  
  model <- load_model("models/runs-avg-elnet-rds.rds")
  
  grouped_data <- load_data(input_file)
  
  historical_data <- create_hist_runs()
  
  df_for_preds <- create_preds_df(grouped_data, historical_data)
  
  predictions <- make_predictions(model, df_for_preds)
  
  print_predictions(predictions)
}

# Load trained model
load_model <- function(model_path) {
  return(readRDS(model_path))
}

# Load data
load_data <- function(file_path) {
  ext <- file_ext(file_path)
  if (ext == "parquet") {
    df <- read_parquet(file_path) |>  
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
    select(dates, matchid, batting_team, over, predicted_runs)

  print(selected_cols)
}

main()
