# nodejs-bigquery-storage-crash-reprod

![Crash screenshot](/docs/crash.png)

This repository is a minimal reproduction of a crash of using BigQuery Storage API with Node.js.


# Prerequisites

- Google Cloud ADC (Application Default Credentials) must be configured for accessing Google Cloud resources.
- Node.js Active LTS (v22 at this moment)
- BigQuery Table must be created in advance. See below for example table creation SQL.


### Configuring ADC

Google Cloud SDK (gcloud cli) must be installed and configured to use Application Default Credentials (ADC).
Refer to [Google Cloud documentation](https://cloud.google.com/sdk/docs/install-sdk#installing_the_latest_version) for installation and configuration.

After Google Cloud SDK installation, run the following command to configure ADC:

```bash
$ gcloud auth application-default login
```

### Preparing write destination table

Run below SQL to create a BigQuery dataset and a table for testing.
Replace `YOUR_PROJECT_ID` with your actual Google Cloud project ID.

```bigquery
CREATE SCHEMA `YOUR_PROJECT_ID.example_dataset`;

CREATE TABLE `YOUR_PROJECT_ID.example_dataset.example_table`
(
  event_id STRING,
  event_timestamp TIMESTAMP,
  payload JSON
)
PARTITION BY TIMESTAMP_TRUNC(event_timestamp, HOUR)
OPTIONS(
  require_partition_filter=true
);
```


# Running

Make sure you have installed the required dependencies by running:

```bash
$ npm ci
```

Then, you can run the following command to start the application for reproducing the crash.
Make sure to replace `YOUR_PROJECT_ID` with your actual Google Cloud project ID.

```bash
$ env PROJECT_ID="YOUR_PROJECT_ID" \
    STREAM_ID="projects/YOUR_PROJECT_ID/datasets/example_dataset/tables/example_table/streams/_default" \
    node index.js
```

If you need to see timestamped logs, install [ets](https://github.com/zmwangx/ets) and run the following command:
```bash
$ ets --utc -c 'time env PROJECT_ID="YOUR_PROJECT_ID" STREAM_ID="projects/YOUR_PROJECT_ID/datasets/example_dataset/tables/example_table/streams/_default" node index.js' 
```
