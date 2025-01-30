library(googledrive)
library(purrr)
library(glue)
library(jsonlite)
library(arrow)
library(here)
library(dplyr)
library(duckplyr)
library(stringr)
library(skimr)
library(tidyr)

main <- function() {
  # Download the data from Google Drive

  # download_odi_data()

  # Convert to parquet for vroom vroom

  # convert_matches_innings()

  # Filter to only men's matches and matches that had a result

  complete_mens <- get_complete_mens("match_results.parquet")

  # Run ad-hoc validation test

  run_mens_validation(complete_mens)

  # Output team, inning order, remaining overs, and remaining wickets to JSON

  innings_complete_mens <- get_innings("innings_results.parquet", complete_mens)

  match_innings <- join_match_innings(
    complete_mens,
    innings_complete_mens
  )

  # skim(match_innings)

  output_intermediate <- transform_match_innings(match_innings)

  write_intermediate_output(output_intermediate)
}

download_gdrive <- function(file_id, file_name) {
  drive_deauth()

  drive_download(
    as_id(file_id),
    path = glue("{file_name}.json"),
    overwrite = TRUE
  )
}

download_odi_data <- function() {
  ## Assumes these google drive files still do not require auth

  file_ids = list(
    match_results = "19hVoi9f7n7etcmSXx7WHeiDp9pOLpQvN",
    innings_results = "1wQO9zr1VH8bY2W4Ca6cMxPdAoPOHo6X6"
  )

  walk2(file_ids, names(file_ids), download_gdrive)
}

convert_json_parquet <- function(json_path) {
  df <- fromJSON(json_path, flatten = TRUE) %>% as_tibble()
  parquet_path <- str_replace(json_path, "\\.json$", ".parquet")
  write_parquet(df, parquet_path)
}

convert_matches_innings <- function() {
  list.files(here(), pattern = "\\.json$", full.names = TRUE) |>
    walk(convert_json_parquet)
}

get_complete_mens <- function(parquet_path) {
  men_valid_results <- df_from_parquet(parquet_path) |>
    mutate(across(where(is.factor), as.character)) |>
    mutate(
      no_result = case_when(
        !is.na(outcome.result) | result == "no result" ~ TRUE,
        TRUE ~ FALSE
      )
    ) |>
    filter(!no_result, gender == "male") |>
    distinct(matchid, .keep_all = TRUE)

  men_valid_results |>
    glimpse()
}

run_mens_validation <- function(complete_mens_df) {
  # Can do a test here to make sure gender is all male
  # Another test to make sure if outcome.winner is NA, result == tie

  # Ad hoc test here. If outcome.winner is NA, all result should be tie
  # Also the last row of total should equal the number of rows

  complete_mens_df |>
    count(outcome.winner, outcome.result, result) |>
    mutate(total = cumsum(n)) |>
    print()
}

get_innings <- function(parquet_path, match_df) {
  ball_by_ball <- df_from_parquet("innings_results.parquet") |>
    filter(matchid %in% match_df$matchid) |>
    glimpse()
}

join_match_innings <- function(df_match, df_innings) {
  full_df <- df_match |>
    left_join(df_innings, by = "matchid") |>
    select(matchid, team, innings, over, overs, runs.total, wicket.kind)
}

transform_match_innings <- function(df_full) {
  df_full |>
    separate(over, into = c("over", "delivery"), sep = "\\.", convert = TRUE) |>
    mutate(
      remaining_overs = overs - over,
      remaining_wickets = 10 - cumsum(!is.na(wicket.kind))
    ) |>
    glimpse()
}

write_intermediate_output <- function(intermediate_df) {
  # Writing the df to JSON
  write_json(intermediate_df, "intermediate_output.json", pretty = TRUE)

  # Also parquet file for vroom vroom
  df_to_parquet(data = intermediate_df, "intermediate_output.parquet")
}

main()
