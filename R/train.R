library(googledrive)
library(purrr)
library(glue)
library(jsonlite)
library(arrow)
library(here)
library(dplyr)
library(duckplyr)
library(stringr)
library(tidyr)
library(ggplot2)
library(tidymodels)
library(glmnet)
library(readr)
library(butcher)
library(logger)

main <- function() {
  log_directory <- "logs"
  if (!dir.exists(log_directory)) dir.create(log_directory)
  log_file <- file.path(log_directory, "model_training.log")
  log_appender(appender_file(log_file))

  log_info("Starting training script")

  log_info("Started downloading data from Google Drive")

  log_info("Started downloading data from Google Drive")
  download_odi_data()
  log_info("Finished downloading data from Google Drive")

  log_info("Finished downloading data from Google Drive")

  log_info("Started converting data from JSON to parquet")

  convert_matches_innings()

  log_info("Finished converting data from JSON to parquet")

  log_info("Started transforming match data for men's complete matches")

  complete_mens <- get_complete_mens(here("data/raw/match_results.parquet"))

  log_info("Finished transforming match data for men's complete matches")

  log_info("Started transforming delivery data for men's complete matches")

  innings_complete_mens <- get_innings(
    here("data/raw/innings_results.parquet"),
    complete_mens
  )

  log_info("Finished transforming delivery data for men's complete matches")

  log_info("Started joining match + delivery data for men's complete matches")

  match_innings <- join_match_innings(complete_mens, innings_complete_mens)

  log_info("Finished joining match + delivery data for men's complete matches")

  log_info("Started transforming match + delivery data to intermediate output")

  output_intermediate <- transform_match_innings(match_innings)

  log_info("Finished transforming match + delivery data to intermediate output")

  log_info("Started writing intermediate output")

  write_intermediate_output(output_intermediate)

  log_info("Finished writing intermediate output")

  log_info("Started training the model")

  model_artifacts <- run_modeling(output_intermediate)

  log_info("Finished training the model")

  log_info("Started saving model artifacts")

  save_model(
    model_artifacts[["train_test_list"]],
    model_artifacts[["workflow"]],
    model_artifacts[["resamples"]]
  )

  log_info("Finished saving model artifacts")
}

download_gdrive <- function(file_id, file_name) {
  drive_deauth()
  drive_download(
    as_id(file_id),
    path = here("data/raw", glue("{file_name}.json")),
    overwrite = TRUE
  )
}

download_odi_data <- function() {
  file_ids <- list(
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
  list.files(here("data/raw"), pattern = "\\.json$", full.names = TRUE) |>
    walk(convert_json_parquet)
}

write_intermediate_output <- function(intermediate_df) {
  write_json(
    intermediate_df,
    here("data/processed/intermediate_output.json"),
    pretty = TRUE
  )
  df_to_parquet(
    intermediate_df,
    here("data/processed/intermediate_output.parquet")
  )
}

check_required_columns <- function(df, required_cols) {
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop(
      sprintf(
        "Missing required columns: %s",
        paste(missing_cols, collapse = ", ")
      )
    )
  }
}

get_complete_mens <- function(parquet_path) {
  if (!file.exists(parquet_path)) {
    stop("Parquet file not found: ", parquet_path)
  }
  tryCatch(
    {
      men_valid_results <- df_from_parquet(parquet_path) |>
        mutate(across(where(is.factor), as.character)) |>
        mutate(
          no_result = case_when(
            !is.na(outcome.result) | result == "no result" ~ TRUE,
            TRUE ~ FALSE
          )
        )

      log_debug(
        "Column names: {paste(names(men_valid_results), collapse = ', ')}"
      )

      check_required_columns(
        men_valid_results,
        c("gender", "no_result", "matchid")
      )

      filtered_results <- men_valid_results |>
        filter(!no_result, gender == "male") |>
        distinct(matchid, .keep_all = TRUE)

      if (any(filtered_results$gender != "male")) {
        stop(
          "Validation failed: Found non-male gender values in filtered results"
        )
      }

      inconsistent_ties <- filtered_results |>
        filter(
          (is.na(outcome.winner) & result != "tie") |
            (!is.na(outcome.winner) & result == "tie")
        )

      if (nrow(inconsistent_ties) > 0) {
        log_error(
          "Found {nrow(inconsistent_ties)} rows with inconsistent tie results"
        )
        stop("Validation failed: Inconsistent tie results found")
      }

      result_summary <- filtered_results |>
        count(outcome.winner, outcome.result, result) |>
        mutate(total = cumsum(n))

      log_debug("Result summary:\n{capture.output(print(result_summary))}")

      filtered_results
    },
    error = function(e) {
      log_error("Error processing match data: {e$message}")
      stop("Error processing match data: ", e$message)
    }
  )
}

get_innings <- function(parquet_path, match_df) {
  ball_by_ball <- df_from_parquet(here("data/raw/innings_results.parquet")) |>
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
  check_required_columns(
    df_full,
    c("matchid", "innings", "over", "overs", "wicket.kind", "team")
  )
  tryCatch(
    {
      transformed_df <- df_full |>
        separate(
          over,
          into = c("over", "delivery"),
          sep = "\\.",
          convert = TRUE
        ) |>
        mutate(
          remaining_overs = overs - over
        ) |>
        distinct(matchid, innings, over, delivery, .keep_all = TRUE) |>
        arrange(matchid, innings, over, delivery) |>
        group_by(matchid, team, innings) |>
        mutate(remaining_wickets = 10 - cumsum(!is.na(wicket.kind))) |>
        ungroup()

      if (
        any(
          transformed_df$remaining_wickets < 0 |
            transformed_df$remaining_wickets > 10
        )
      ) {
        stop("Invalid remaining wickets values detected")
      }
      if (
        any(
          transformed_df$remaining_overs < 0 |
            transformed_df$remaining_overs > 50
        )
      ) {
        stop("Invalid remaining overs values detected")
      }

      duplicates <- transformed_df |>
        group_by(matchid, innings, over, delivery) |>
        summarize(n = n(), .groups = "drop") |>
        filter(n > 1)

      if (nrow(duplicates) > 0) {
        duplicate_details <- duplicates |>
          arrange(desc(n)) |>
          head(5)

        error_msg <- sprintf(
          "Duplicate entries found in transformed data for %d match-innings-over-delivery combinations. First %d examples:\n%s",
          nrow(duplicates),
          min(5, nrow(duplicates)),
          paste(capture.output(print(duplicate_details)), collapse = "\n")
        )
        stop(error_msg)
      }

      transformed_df
    },
    error = function(e) {
      log_error("Error transforming match innings: ", e$message)
      stop("Error transforming match innings: ", e$message)
    }
  )
}

plot_overall_avg <- function(df) {
  df |>
    group_by(matchid, over, innings) |>
    summarize(total_runs = sum(runs.total, na.rm = TRUE), .groups = "drop") |>
    group_by(over) |>
    summarize(avg_runs_over = mean(total_runs, na.rm = TRUE)) |>
    ggplot(aes(x = over, y = avg_runs_over)) +
    geom_col(alpha = 0.7) +
    theme_minimal() +
    labs(x = "Over", y = "Average Runs", title = "Average Runs Scored Per Over")
}

create_runs_df <- function(df) {
  check_required_columns(df, c("matchid", "team", "innings"))

  tryCatch(
    {
      df |>
        group_by(matchid) |>
        mutate(
          batting_team = team,
          all_teams = toString(sort(unique(team))),
          bowling_team = str_trim(
            case_when(
              team == str_extract(all_teams, "^[^,]+") ~
                str_extract(all_teams, "[^,]+$"),
              TRUE ~ str_extract(all_teams, "^[^,]+")
            )
          )
        ) |>
        select(-all_teams) |>
        ungroup() |>
        select(
          matchid,
          batting_team,
          bowling_team,
          venue,
          dates,
          innings,
          over,
          runs.total
        )
    },
    error = function(e) {
      log_error("Error creating runs dataframe: ", e$message)
      stop("Error creating runs dataframe: ", e$message)
    }
  )
}

create_current_runs <- function(df) {
  df |>
    group_by(
      matchid,
      batting_team,
      bowling_team,
      dates,
      innings,
      over
    ) |>
    summarize(runs_in_over = sum(runs.total, na.rm = TRUE), .groups = "drop") |>
    glimpse()
}

create_hist_runs <- function(df) {
  df |>
    group_by(matchid, over, innings) |>
    summarize(total_runs = sum(runs.total, na.rm = TRUE), .groups = "drop") |>
    group_by(over) |>
    summarize(avg_runs_over = mean(total_runs, na.rm = TRUE)) |>
    glimpse()
}

create_train_test <- function(df, hist_df) {
  set.seed(33)
  split <- initial_split(df, prop = 0.8, strata = runs_in_over)
  train <- training(split)
  test <- testing(split)

  train_hist_avg <- train |>
    left_join(hist_df, by = "over") |>
    glimpse()

  test_hist_avg <- test |>
    left_join(hist_df, by = "over") |>
    glimpse()

  return(list(train = train_hist_avg, test = test_hist_avg))
}

create_recipe <- function(df) {
  recipe <- recipe(runs_in_over ~ ., data = df) |>
    update_role(matchid, , new_role = "ID") |>
    step_mutate(
      dates = as.Date(dates),
      across(where(is.character), as.factor)
    ) |>
    step_date(dates, features = c("year", "month", "doy", "dow")) |>
    step_rm(dates) |>
    step_dummy(all_nominal_predictors(), one_hot = TRUE) |>
    step_normalize(all_numeric_predictors())
}

create_model_spec <- function() {
  elnet_spec <-
    linear_reg(
      penalty = tune(),
      mixture = tune()
    ) |>
      set_engine("glmnet", validation = 0.2) |>
      set_mode("regression")
}

run_modeling <- function(df) {
  tryCatch(
    {
      log_info("Creating initial runs dataframe")
      df_initial <- create_runs_df(df)

      log_info("Creating current runs summary")
      runs_over_current <- create_current_runs(df_initial)

      log_info("Creating historical runs summary")
      runs_over_hist <- create_hist_runs(df_initial)

      log_info("Creating train-test split")
      train_test_list = create_train_test(runs_over_current, runs_over_hist)

      log_info("Creating recipe")
      basic_recipe <- create_recipe(train_test_list[["train"]])

      log_info("Creating model specification")
      elnet_spec <- create_model_spec()

      log_info("Creating workflow")
      elnet_wf <- workflow() |>
        add_recipe(basic_recipe) |>
        add_model(elnet_spec)

      log_info("Setting up parallel processing")
      doParallel::registerDoParallel()

      log_info("Creating cross-validation folds")
      set.seed(33)
      cv_folds <- vfold_cv(
        train_test_list[["train"]],
        v = 5,
        strata = runs_in_over
      )

      log_info("Tuning model")
      elnet_rs <- tune_grid(elnet_wf, resamples = cv_folds, grid = 5)

      metrics_df = show_best(elnet_rs, metric = "rmse")
      dir.create("models/metrics", recursive = TRUE)
      write.csv(
        metrics_df,
        "models/metrics/model_cv_performance.csv",
        row.names = FALSE
      )

      log_info("Model CV performance saved to models/metrics/ directory")

      return(
        list(
          train_test_list = train_test_list,
          workflow = elnet_wf,
          resamples = elnet_rs
        )
      )
    },
    error = function(e) {
      log_error("Error in model training pipeline: {e$message}")
      stop(e)
    }
  )
}

save_model <- function(train_test_list, workflow, resamples) {
  full_data <- bind_rows(train_test_list[["train"]], train_test_list[["test"]])

  final_wf <- workflow |>
    finalize_workflow(select_best(resamples, metric = "rmse"))

  full_fit <- final_wf |>
    fit(full_data)

  minimal_model <- full_fit |>
    butcher()

  dir.create("models/", showWarnings = FALSE)

  dir.create("models/metrics/", showWarnings = FALSE)

  saveRDS(minimal_model, "models/runs-avg-elnet-rds.rds")

  log_info("Model saved to models/ directory")

  saveRDS(resamples, "models/metrics/full_tuning_grid.rds")

  log_info("Tuning grid saved to models/metrics/ directory")
}

if (sys.nframe() == 0L) {
  main()
}
