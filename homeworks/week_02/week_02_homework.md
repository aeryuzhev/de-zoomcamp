# Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation.

## Question 1. Load January 2020 data

**Question:**

>Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.
>
>How many rows does that dataset have?

**Solution:**

```bash
prefect deployment build etl_web_to_gcs.py:etl_parent_flow \
  -n "etl_taxi_green_q_1" -a 
prefect deployment run etl-parent-flow/etl_taxi_green_q_1 \
  --params '{"year": 2020, "months": [1], "color": "green"}'
```

The last part of the output from Prefect Agent with the number of processed rows:

```bash
21:54:53.032 | INFO    | Flow run 'mustard-fulmar' - Number of processed rows: 447770
21:54:53.104 | INFO    | Flow run 'mustard-fulmar' - Finished in state Completed('All states completed.')
21:54:54.328 | INFO    | prefect.infrastructure.process - Process 'mustard-fulmar' exited cleanly.
```

**Files:**

[etl_web_to_gcs.py](https://github.com/aeryuzhev/de-zoomcamp/tree/master/homeworks/week_02/etl_web_to_gcs.py)

**Answer:**

`447,770`

## Question 2. Scheduling with Cron

**Question:**

>Cron is a common scheduling specification for workflows.
>
>Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

**Solution:**

```bash
prefect deployment build etl_web_to_gcs.py:etl_parent_flow \
  -n "etl_taxi_green_q_2" --cron "0 5 1 * *" -a
```

![cron](/homeworks/week_02/images/cron.png)

**Files:**

[etl_web_to_gcs.py](https://github.com/aeryuzhev/de-zoomcamp/tree/master/homeworks/week_02/etl_web_to_gcs.py)

**Answer:**

`0 5 1 * *`

## Question 3. Loading data to BigQuery

**Question:**

>Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).
>
>The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.
>
>Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.
>
>Make any other necessary changes to the code for it to function as required.
>
>Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).
>
>Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

**Solution:**

```bash
prefect deployment build etl_gcs_to_bq.py:etl_parent_flow \
  -n "etl_taxi_green_q_3" -a
prefect deployment run etl-parent-flow/etl_taxi_green_q_1 \
  --params '{"year": 2019, "months": [2, 3], "color": "yelllow"}'
```

The last part of the output from Prefect Agent with the number of processed rows:

```bash
23:16:23.239 | INFO    | Flow run 'bizarre-dove' - Number of processed rows: 14851920
23:16:23.285 | INFO    | Flow run 'bizarre-dove' - Finished in state Completed('All states completed.')
23:16:24.485 | INFO    | prefect.infrastructure.process - Process 'bizarre-dove' exited cleanly.
```

**Files:**

[etl_gcs_to_bq.py](https://github.com/aeryuzhev/de-zoomcamp/tree/master/homeworks/week_02/etl_gcs_to_bq.py)

**Answer:**

`14,851,920`

## Question 4. Github Storage Block

**Question:**

>Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.
>
>Note that you will have to push your code to GitHub, Prefect will not push it for you.
>
>Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.
>
>How many rows were processed by the script?

**Answer:**

`88,605`

## Question 5. Email or Slack notifications

**Question:**

>Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.
>
>The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur.
>
>Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up.
>
>Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.
>
>Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook.
>
>Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days.
>
>In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: <https://hooks.slack.com/services/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>
>
>Test the functionality.
>
>Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create.
>
>How many rows were processed by the script?

**Solution:**

```bash
# Login on app.prefect.cloud and create an automation on app.prefect.cloud
prefect cloud login

prefect deployment build etl_web_to_gcs.py:etl_parent_flow -n "etl_taxi_green_q_5" -a
prefect deployment run etl-parent-flow/etl_taxi_green_q_5 --params '{"year": 2019, "months": [4], "color": "green"}'
```

**Answer:**

`514,392`

## Question 6. Secrets

**Question:**

>Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

**Solution:**

![secret_block](/homeworks/week_02/images/secret_block.png)

**Answer:**

`8`
