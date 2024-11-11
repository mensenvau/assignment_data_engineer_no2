## Spark ML Logs Transformer

As a newly hired data engineer, your first task is to implement a simple ETL process on a logs stream, which is generated by data scientists during their machine learning experiments.

## Data Description

The logs data is provided in JSONL format (`app/logs.jsonl`), with one JSON object per line. Each log object consists of the following fields:

-   **logId**: A unique string identifier for the log.
-   **expId**: An integer identifier for the experiment associated with this log.
-   **metricId**: An integer identifier for the metric represented in this log.
-   **valid**: A boolean field (deprecated) indicating if the log is valid.
-   **createdAt**: The timestamp indicating when the log was created.
-   **ingestedAt**: The timestamp indicating when the log was written to the stream.
-   **step**: A sequential integer identifier within the experiment.
-   **value**: A floating-point value representing the metric.

The corresponding experiment names are available in `app/experiments.csv`, and metric names are predefined as follows:

-   `0`: Loss
-   `1`: Accuracy

## Tasks

Your task is to implement an ETL pipeline, which reads the logs, merges additional information, filters invalid logs, computes statistics, and saves the results in a specified format.

### Part 1: Data Loaders

1. **load_logs**: Load logs from a JSONL file into a Spark DataFrame.
2. **load_experiments**: Load experiment names from a CSV file into a Spark DataFrame.
3. **load_metrics**: Create a DataFrame with predefined metric names and identifiers.

### Part 2: Merge Data Sources

Implement a function to join the `logs`, `experiments`, and `metrics` DataFrames into a single DataFrame using `expId` and `metricId` as foreign keys.

### Part 3: Filter Invalid Logs

Create a function to filter out logs that were ingested too late. Specifically, remove logs where the difference between `ingestedAt` and `createdAt` exceeds a specified number of hours.

### Part 4: Calculate Experiment's Statistics

Calculate final metrics for each experiment and metric, including the minimum and maximum values of `value`. The resulting DataFrame should include:

-   **expName**: Experiment name
-   **metricName**: Metric name
-   **maxValue**: Maximum value of the metric in the experiment
-   **minValue**: Minimum value of the metric in the experiment

### Part 5: Save Transformed Data

Save the statistics as a Parquet file, partitioned by `metricId`.