## Step 1: Extract Data Using Python and Uplode to Snowflake database

## data_ingestion folder

### Requirements

To install the required packages, run:

```sh
pip install -r requirements
```

```sh
python data_ingestion.py
```

### Connecting dbt to Snowflake


### Step 2: Connect dbt to Snowflake

### Initialize dbt
### To initialize a new dbt project, run:


```sh
dbt init
```


## In dbt_demo folder

### test dbt connectivity with snowflake

```sh
dbt debug
```

### after writing sql scripts in your_project_name/models/sql

```sh
run dbt run
```

### Improvements for the Future

- **Using Python Functions:** Enhance the data extraction and loading processes by encapsulating them in reusable Python functions.
- **Using Airflow:** Automate the ELT pipeline seamlessly by integrating Apache Airflow to manage and schedule data workflows.
- **Using dbt Tests:** Implement dbt tests for data quality to ensure the integrity and reliability of your data models.
