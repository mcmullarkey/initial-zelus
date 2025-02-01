library(testthat)
library(mockery)
library(dplyr)
library(recipes)
library(tidyr)

# Set up logging for tests
logger::log_threshold(logger::FATAL)

# Get the script path relative to the test directory
script_path <- normalizePath(file.path("../../R", "train.R"))
if (!file.exists(script_path)) {
  stop(
    sprintf(
      "Could not find train.R at %s. Current working directory: %s",
      script_path,
      getwd()
    )
  )
}

# Source the script containing the functions we want to test
test_env <- new.env()
sys.source(script_path, test_env)
list2env(as.list(test_env), environment())

# Test setup
context("train.R Tests")

# Helper function for tests
create_test_data <- function() {
  list(
    basic_match = tibble(
      matchid = c(1, 2, 3),
      gender = c("male", "female", "male"),
      no_result = c(FALSE, FALSE, TRUE),
      outcome.result = c(NA, NA, "abandoned"),
      result = c("completed", "tie", "no result")
    ),
    basic_innings = tibble(
      matchid = rep(1, 3),
      innings = rep(1, 3),
      over = c("0.1", "0.2", "0.3"),
      overs = rep(50, 3),
      wicket.kind = c(NA, "bowled", NA),
      team = rep("England", 3),
      runs.total = c(1, 0, 4)
    ),
    basic_runs = tibble(
      matchid = 1:3,
      batting_team = rep("England", 3),
      bowling_team = rep("Bangladesh", 3),
      dates = rep(as.Date("2024-01-01"), 3),
      innings = rep(1, 3),
      over = 1:3,
      runs.total = c(6, 4, 2),
      venue = rep("Lords", 3)
    )
  )
}

test_that("get_complete_mens filters correctly", {
  test_data <- create_test_data()
  mock_df_from_parquet <- mock(test_data$basic_match)
  with_mock(
    df_from_parquet = mock_df_from_parquet,
    {
      tf <- tempfile(fileext = ".parquet")
      file.create(tf)
      on.exit(unlink(tf))
      result <- get_complete_mens(tf)
      expect_equal(nrow(result), 1)
      expect_equal(result$matchid, 1)
    }
  )
})

test_that("get_complete_mens handles missing files", {
  nonexistent_file <- tempfile(fileext = ".parquet")
  expect_error(
    get_complete_mens(nonexistent_file),
    "Parquet file not found:"
  )
})

test_that("transform_match_innings calculates remaining values correctly", {
  test_innings <- tibble(
    matchid = 1,
    innings = 1,
    over = "0.1",
    overs = 50,
    wicket.kind = NA,
    team = "England",
    runs.total = 1
  )

  result <- transform_match_innings(test_innings)
  expect_equal(result$remaining_overs[1], 50)
  expect_equal(result$remaining_wickets[1], 10)
})

test_that("transform_match_innings validates overs and wickets", {
  invalid_overs_df <- tibble(
    matchid = 1,
    innings = 1,
    over = "51.1",
    overs = 50,
    wicket.kind = NA,
    team = "England",
    runs.total = 0
  )

  print(invalid_overs_df)

  expect_error(
    transform_match_innings(invalid_overs_df),
    "Invalid remaining overs values detected"
  )
})

test_that("create_current_runs aggregates runs correctly", {
  test_data <- create_test_data()
  result <- create_current_runs(test_data$basic_runs)
  expect_equal(result$runs_in_over[1], 6)
  expect_named(
    result,
    c(
      "matchid",
      "batting_team",
      "bowling_team",
      "dates",
      "innings",
      "over",
      "runs_in_over"
    )
  )
})

test_that("create_hist_runs calculates averages correctly", {
  test_df <- tibble(
    matchid = c(1, 2),
    over = c(1, 1),
    innings = c(1, 1),
    runs.total = c(4, 4),
    dates = rep(as.Date("2024-01-01"), 2)
  )
  result <- create_hist_runs(test_df)
  expect_equal(result$avg_runs_over[1], 4)
})
