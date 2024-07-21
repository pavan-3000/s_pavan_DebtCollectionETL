## Step 1: Extract Data Using Python and Uplode to Snowflake database

### Requirements

To install the required packages, run:

```sh
pip install -r requirements
```

```
python data_ingestion.py
```


### Connecting dbt to Snowflake

```markdown
## Step 2: Connect dbt to Snowflake

### Initialize dbt
To initialize a new dbt project, run:
```sh
dbt init
```
your_project_name:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account.snowflakecomputing.com
      user: your_user
      password: your_password
      role: your_role
      database: your_database
      warehouse: your_warehouse
      schema: your_schema

### test dbt connectivity with snowflake

```sh
dbt debug
```

### after writing sql scripts in your_project_name/models/sql

```sh
run dbt run
```# s_pavan_DebtCollectionETL
