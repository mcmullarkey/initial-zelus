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

main <- function() {
  # Download the data from Google Drive

  # download_odi_data()

  # Convert to parquet for vroom vroom

  # convert_matches_innings()

  # Filter to only men's matches and matches that had a result

  get_complete_mens()

  # Output team, inning order, remaining overs, and remaining wickets to JSON
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

get_complete_mens <- function() {
  files <- c("match_results.json", "innings_results.json")

  # Read in the JSON files and glimpse the data
  # files |>
  #   map(~fromJSON(.)) |>
  #   walk(~glimpse(.))

  # We only have one team from this df if we do distinct matchid
  # Well need to see if that messes up the join with the ball-by-ball data

  men_valid_results <- df_from_parquet("match_results.parquet") |>
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

  # Can do a test here to make sure gender is all male
  # Another test to make sure if outcome.winner is NA, result == tie

  # Ad hoc test here. If outcome.winner is NA, all result should be tie
  # Also the last row of total should equal the number of rows

  men_valid_results |>
    count(outcome.winner, outcome.result, result) |>
    mutate(total = cumsum(n)) |>
    print()

  # Load ball by ball data

  ball_by_ball <- df_from_parquet("innings_results.parquet") |>
    filter(matchid %in% men_valid_results$matchid)

  full_df <- men_valid_results |>
    left_join(ball_by_ball, by = "matchid") |>
    select(team, innings, over, runs.total, wicket.kind) |>
    glimpse()

  # mutate(
  #   has_result = case_when(
  #     (result != "no result") | is.na(result) | outcome.result == "tie" ~
  #       TRUE,
  #     TRUE ~ FALSE
  #   )
  # ) |>
  # filter(
  #   gender == "male",
  #   has_result
  # ) |>
  # distinct(matchid, .keep_all = TRUE) |>
  # glimpse()

  # match_results |>
  #   filter(result != "tie" | outcome.result != "tie" | is.na(outcome.result)) |>
  #   count(outcome.winner) |>
  #   mutate(total = cumsum(n)) |>
  #   print()

  # match_results |>
  #   filter(is.na(outcome.winner), is.na(outcome.result)) |>
  #   print()
  #
  # match_results |>
  #   count(result) |>
  #   print()

  skim(full_df)

  # match_results |>
  #   filter(is.na(outcome.winner)) |>
  #   count(result, outcome.method, sort = TRUE) |>
  #   print()
  #
  # match_results |>
  #   filter(is.na(outcome.winner), is.na(result)) |>
  #   print()
  #
  # match_results |>
  #   filter(is.na(outcome.winner)) |>
  #   print()
  #
  # ball_by_ball <- fromJSON("innings_results.json")
  #
  # full_df <- match_results |>
  #   left_join(ball_by_ball, by = "matchid")
  #
  # skim(full_df)

  # Also need to double-check for "no result" matches in ball-by-ball data
  # Need to look for non-completed innings, but only when the team batting
  # second doesn't complete by scoring more than the team how batted first
  # Even more so now that even after filtering out "no result" outcome.winner
  # still has
}

main()
