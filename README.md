# Michael Mullarkey's Responses to Zelus Analytics Take Home

## Question #1

### My proposed architecture

Since the predictions need to be delivered overnight, not in real time, we're 
going to architect a batch ML deployment. I'm going to focus on the ML 
infrastructure and leave out other important processes such as security (how are IAM roles configured?) 
and analytics engineering (do we use dbt or SQLMesh to make the feature store data more accessible to end users?). 

The batch ML deployment will use the following steps:

#### Step 1 
Kick off the entire process in the early morning local time after all games should be complete using 
EventBridge scheduler (CRON expression) with an AWS Batch target. (DOUBLE CHECK IF ALL HAPPEN IN SAME PLACE OR DIFFERENT TIME ZONES)
The remaining steps are jobs inside of AWS Batch, which we will track with AWS Step Functions for lightweight orchestration.

#### Step 1 Reasoning: 
While we could use an orchestrator like Airflow or Dagster those tools 
add unnecessary overhead for getting from 0 - 1. If we identify that this process 
could benefit from orchestration that can be added later. Since this style of 
cricket is limited to "one day" play via limiting the number of overs, we know 
roughly how much data we're getting each day + when we'll get it. 
If we were dealing with international test matches that can last up to five days 
(but also could end at any point within those five days), an orchestrator might be more appropriate. 
AWS Batch + AWS Steps also allows us to implement the necessary sequencing, 
retries, etc. for each of the following steps. One tradeoff is there is a bit of 
vendor lock-in from using AWS Steps vs. an open-source orchestrator. However, 
since our jobs primarily consist of Docker containers they will be portable to 
other services, and changing cloud providers is already a large lift.

#### Step 2
A Dockerized R script on AWS Batch provisioned with 8 cores + large memory. 
Reads the data from its source (let’s assume S3, and performs initial raw data 
validation using the pointblank package in R. We'll start with simple checks 
such as no duplicate keys, no null keys, no excessive null values, no massive outliers, etc. 
All validation results will be logged to Cloud Watch, and any failures will 
initiate a retry before ultimately failing if all tests are not passed after three 
retries. If this failure happens Cloud Watch can trigger an SNS alert which in 
turn sends a Slack notification so the team can debug. We will also have included 
unit tests from testthat during the development process to help ensure the quality of the architecture.

#### Step 2 Reasoning: 
pointblank allows us to get clear pass/fail decisions on raw data quality that are easily logged. Initiating a small number of retries allows us to have some fault tolerance while not infinitely looping in case of consistent data validation failure. We also use AWS Batch along with the future and furrr packages in R to make sure this process can happen in parallel for different games at the same time to reduce runtime. However, if we the 8 core, large memory compute can't be used for the feature validation + engineering section of model training we will have to use alternate resources that could bottleneck the process (e.g., Dockerized plumber API deployed on Fargate, ideally multiple cores so we can scale up and down API instances with Valve) Using AWS Batch also helps us reduce costs by giving us the ability to provision larger compute and memory resources on the spot rather than leaving, for example, large EC2 instances running.

#### Step 3
Send the validated raw data to another Dockerized R script and run on AWS Batch with 8 cores + large memory provisioned. Here we create the necessary play by play features using data transformation tools in R, then use inequality + rolling joins in R to approximate "as of" style joins. These joins send the features to a feature store database (Postgres, so on AWS we use Redshift) + use metadata to version the database so we have clear "point in time" data for making predictions. In other words, make sure the features from games day-before-yesterday are updated to have the labels generated by yesterday's model predictions. At the same time, we make sure today's games have predictive features but no labels yet. We'll include pointblank data validation tests to ensure these joins are performing as expected.

#### Step 3 Reasoning
A good batch ML deployment needs a feature store, and with R to my knowledge we have to "roll our own" rather than using ones provided by AWS Sagemaker or Hopworks (which have support for this feature store pattern through Python + other non-R languages). We want to make sure we have up to date labels for yesterday's data for retraining the model, while also ensuring we don't accidentally provide labels to feature sets that haven't received predictions yet. We're defaulting to using a Postgres database (Redshift) as our feature store. However, if we wanted to simplify this process while likely increasing costs + vendor lock-in we could use Sagemaker Feature Store.

#### Step 4
Use a Dockerized R script with 8 cores and large memory usage provisioned via AWS Batch to retrain our labeling model using our updated feature store data. Crucially, we do not include today's feature set data, which is held out for later prediction. We'll use the tidymodels framework + 10-fold cross-validation and grid search to find the optimal hyperparameters. We'll likely ensemble different model predictions using workflowsets + stacks packages as long as time and compute permits. We will log model performance metrics along with the optimal hyperparameters to Cloud Watch. Then, we'll save this model version via vetiver to an S3 board, which automatically versions + updates the model metadata. We'll also compare the model's performance on cross-validation vs. previous iterations of the model. If the model performs outside of a predefined fault tolerance range we will still save the model version, but will roll back to the last model version that passed fault tolerance. This result should also trigger an SNS alert + Slack message.

#### Step 4 Reasoning
The tidymodels framework provides the most robust, actively developed infrastructure for ML in R. Vetiver allows us to get similar model versioning we would from, for example, MLFlow Model Registry used with Python. We also log the performance of models + their ensemble to identify any potential issues or changes over time in model training. It’s also important to have roll back procedures if for whatever reason the model’s performance is outside of an acceptable range. We also need automated notifications of this roll back so we aren’t relying on people to manually inspect logs for issues.

#### Step 5
Pull the most recent model that’s inside fault tolerance from S3 using a Dockerized R script provisioned with 1 core and 4 GB of memory from AWS Batch. Create labels for the feature sets game by game, and send those predictions back to the feature store database with the necessary data to identify "point in time" prediction (likely via inequality or rolling joins). In other words, we recognize that these labels now exist from the model trained today, but these labels weren't available prior to the model retraining.These inequality or rolling joins will be validated with the pointblank package.

#### Step 5 Reasoning
This process uses the resource constraints as provided and updates the feature store database. We're using the most recent fault tolerant version of our model via vetiver, and we keep the feature store straight by using metadata to make clear which "point in time" we're talking about, We also want to make sure we provision enough resources here, since while technically an AWS Lambda can have 1 core and 4GB of memory its timeout is 15 minutes max. Since we have up to an hour of compute time we’ll want to make sure that’s part of how we provision as well.

## Question 2

From 1 to 5 I'm a 1 on cricket knowledge. My only knowledge comes from the 
[Wikipedia page](https://en.wikipedia.org/wiki/One_Day_International) in the problem statement, 
[this Youtube explainer](https://www.youtube.com/watch?v=EWpbtLIxYBk) I remembered going around a while back, 
and posts on Bluesky when the USA upset Pakistan last year.

