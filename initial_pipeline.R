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
library(ggplot2)

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

  output_intermediate |>
    distinct(remaining_wickets) |>
    print()

  output_intermediate |>
    filter(remaining_wickets <= 0) |>
    arrange(matchid, innings, over, delivery) |>
    glimpse()

  output_intermediate |>
    count(wicket.kind) |>
    print()

  output_intermediate |>
    count(matchid, innings) |>
    print()

  output_intermediate |>
    group_by(matchid, innings, over, delivery) |>
    summarize(n = n(), .groups = "drop") |>
    filter(n > 1) |>
    print()

  output_intermediate |>
    select(over) |>
    distinct() |>
    print(n = 50)

  output_intermediate |>
    filter(matchid == "1000887", innings == 1, over == 42, delivery == 6) |>
    glimpse()

  # write_intermediate_output(output_intermediate)

  plot_overall_avg(output_intermediate)

  # plot_team_avg(output_intermediate)
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
    distinct(matchid, .keep_all = TRUE) |>
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
    select(
      matchid,
      city,
      venue,
      neutral_venue,
      dates,
      team,
      innings,
      over,
      overs,
      runs.total,
      wicket.kind
    )
}

transform_match_innings <- function(df_full) {
  df_full |>
    separate(over, into = c("over", "delivery"), sep = "\\.", convert = TRUE) |>
    mutate(
      remaining_overs = overs - over
    ) |>
    distinct(matchid, innings, over, delivery, .keep_all = TRUE) |>
    arrange(matchid, innings, over, delivery) |>
    group_by(matchid, team, innings) |>
    mutate(remaining_wickets = 10 - cumsum(!is.na(wicket.kind))) |>
    ungroup() |>
    glimpse()
}

# Would be good to write a test that confirms remaining wickets are between
# 0 and 10 while all overs are between 0-50 (Deliveries can be >6 because
# of penalties I think)

write_intermediate_output <- function(intermediate_df) {
  write_json(intermediate_df, "intermediate_output.json", pretty = TRUE)

  df_to_parquet(data = intermediate_df, "intermediate_output.parquet")
}

plot_overall_avg <- function(df) {
  df |>
    group_by(matchid, over, team) |>
    summarize(total_runs = sum(runs.total, na.rm = TRUE), .groups = "drop") |>
    group_by(over) |>
    summarize(avg_runs_over = mean(total_runs, na.rm = TRUE)) |>
    ggplot(aes(x = over, y = avg_runs_over)) +
    geom_col(alpha = 0.7) +
    theme_minimal() +
    labs(x = "Over", y = "Average Runs", title = "Average Runs Scored Per Over")
}

plot_team_avg <- function(df) {
  df |>
    group_by(over, team) |>
    summarize(avg_runs = mean(runs.total, na.rm = TRUE)) |>
    ggplot(aes(team, avg_runs, color = team)) +
    geom_col(alpha = 0.7) +
    theme_minimal()
}

main()
