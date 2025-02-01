# Load required packages
library(testthat)
library(dplyr)
library(mockery)

# Get the script path relative to the test directory
script_path <- normalizePath(file.path("../../R", "predict.R"))
if (!file.exists(script_path)) {
  stop(
    sprintf(
      "Could not find predict.R at %s. Current working directory: %s",
      script_path,
      getwd()
    )
  )
}

# Source the script containing the functions we want to test (aka don't run main)
sys.source(script_path, new.env())

# Test setup
context("Predict.R Tests")

# Create mock data helper function

create_mock_data <- function() {
  test_dates <- as.Date(c("2014-01-01", "2014-01-15", "2014-01-30"))

  tibble(
    matchid = rep(1:3, each = 6),
    team = rep(c(rep("England", 9), rep("Bangladesh", 9))),
    dates = rep(test_dates, each = 6),
    innings = rep(1:2, each = 9),
    over = rep(0:8, 2),
    runs.total = sample(0:20, 18, replace = TRUE)
  )
}

describe("filter_team_data", {
  test_that("handles basic batting team case correctly", {
    mock_data <- create_mock_data()
    result <- filter_team_data(mock_data, "England", 3, "earliest", "batting")

    expect_equal(nrow(result), 3) # Should return 3 overs
    expect_true(all(result$batting_team == "England"))
    expect_true(all(result$over <= 3))
    expect_equal(result$dates[1], as.Date("2014-01-01"))
  })

  test_that("handles basic bowling team case correctly", {
    mock_data <- create_mock_data()
    result <- filter_team_data(mock_data, "England", 3, "earliest", "bowling")

    expect_equal(nrow(result), 3)
    expect_true(all(result$bowling_team == "England"))
    expect_true(all(result$over <= 3))
  })

  test_that("handles 'latest' order correctly", {
    mock_data <- create_mock_data()
    result <- filter_team_data(mock_data, "England", 8, "latest", "batting")
    expect_equal(nrow(result), 8)
    expect_equal(result$dates[1], as.Date("2014-01-15"))
  })

  test_that("handles edge case of requested overs > available overs", {
    mock_data <- create_mock_data()
    result <- filter_team_data(mock_data, "England", 10, "earliest", "batting")

    expect_true(nrow(result) <= 9)
  })

  test_that("handles case of team not in dataset", {
    mock_data <- create_mock_data()
    result <- filter_team_data(
      mock_data,
      "Nonexistent Team",
      3,
      "earliest",
      "batting"
    )

    expect_equal(nrow(result), 0)
  })
})

describe("print_predictions", {
  test_that("selects correct columns", {
    input_data <- tibble(
      dates = rep(Sys.Date(), 3),
      batting_team = rep("England", 3),
      bowling_team = rep("Bangladesh", 3),
      over = 1:3,
      predicted_runs = runif(3),
      extra_col = letters[1:3]
    )

    output <- capture.output(result <- print_predictions(input_data))
    expect_false(any(grepl("extra_col", output)))
  })
})
