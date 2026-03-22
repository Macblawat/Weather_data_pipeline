

Weather Data Pipeline
This project is a functional ETL pipeline I built to handle real-time weather data. The goal was to move data from a live API all the way to a structured SQL database while keeping everything automated.

1. Fetching the Data (API to Blob)
First, I wrote scripts to hit a weather API and pull down the latest metrics.I put the raw data into Blob storage first. 

You can find the extraction scripts in the /scripts or /src folder.

2. Transformation & SQL Loading
Once the data is sitting in the blob, I run a transformation layer. This cleans up the JSON response, handles any missing values, and formats everything so it’s ready for a relational schema. After that, the cleaned data is inserted into a SQL database in Azure for long-term storage and querying.

The transformation logic and SQL schemas are located in the /transform and /sql folders.

3. Orchestration with Apache Airflow
To make sure this actually runs on its own every day (or hour), I used Apache Airflow. I wrote DAGs to schedule and monitor each step—making sure the transformation doesn't start until the API fetch is successful.

All the pipeline logic and scheduling are in the /dags folder.

4. Docker Environment
To avoid "it works on my machine" issues, the whole Airflow setup is hosted on Docker. This makes the entire pipeline portable and easy to deploy anywhere without messing with local dependencies.

The docker-compose.yaml and Docker configuration are in the root directory.
