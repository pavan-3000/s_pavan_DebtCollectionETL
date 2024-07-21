from datetime import datetime
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# Load the CSV file
borrow = pd.read_csv('data_ingestion/5k_borrowers_data.csv', quotechar='"')

# Clean column names by replacing spaces with underscores
borrow.columns = [
    'Name', 'Date_of_Birth', 'Gender', 'Marital_Status', 'Phone_Number',
    'Email_Address', 'Mailing_Address', 'Language_Preference',
    'Geographical_Location', 'Credit_Score', 'Loan_Type', 'Loan_Amount',
    'Loan_Term', 'Interest_Rate', 'Loan_Purpose', 'EMI', 'IP_Address',
    'Geolocation', 'Repayment_History', 'Days_Left_to_Pay_Current_EMI',
    'Delayed_Payment'
]

# Convert data types where necessary
borrow['Date_of_Birth'] = pd.to_datetime(borrow['Date_of_Birth'], format='%d-%m-%Y')


borrow['Date_of_Birth'] = pd.to_datetime(borrow['Date_of_Birth'], format='%d-%m-%Y')
borrow['Credit_Score'] = borrow['Credit_Score'].astype(int)
borrow['Loan_Amount'] = borrow['Loan_Amount'].astype(float)
borrow['Interest_Rate'] = borrow['Interest_Rate'].astype(float)
borrow['EMI'] = borrow['EMI'].astype(float)
borrow['Days_Left_to_Pay_Current_EMI'] = borrow['Days_Left_to_Pay_Current_EMI'].astype(int)


def read_data():
    borrow = pd.read_csv("data_ingestion/5k_borrowers_data.csv")
    return borrow
    

conn = snowflake.connector.connect(
      account= 'bf06713.ap-southeast-1',
      database= 'dbt_db',
      password= 'Pavan@2002',
      role= 'accountadmin',
      schema= 'public',
      user= 'pavan',
      warehouse= 'dbt_wh',
    )
cur = conn.cursor()

print(borrow.columns)
print(borrow.info())

create_table_query = """
CREATE OR REPLACE TABLE BORROWERS (
    Name VARCHAR(100),
    Date_of_Birth DATE,
    Gender VARCHAR(10),
    Marital_Status VARCHAR(20),
    Phone_Number VARCHAR(20),
    Email_Address VARCHAR(100),
    Mailing_Address VARCHAR(255),
    Language_Preference VARCHAR(50),
    Geographical_Location VARCHAR(100),
    Credit_Score INTEGER,
    Loan_Type VARCHAR(50),
    Loan_Amount FLOAT,
    Loan_Term INTEGER,
    Interest_Rate FLOAT,
    Loan_Purpose VARCHAR(255),
    EMI FLOAT,
    IP_Address VARCHAR(50),
    Geolocation VARCHAR(50),
    Repayment_History VARCHAR(50),
    Days_Left_to_Pay_Current_EMI INTEGER,
    Delayed_Payment VARCHAR(50)
);
"""

# Execute the query
cur.execute(create_table_query)


try:
    cur.execute("SHOW TABLES LIKE 'BORROWERS'")
    tables = cur.fetchall()
    if tables:
        print("Table BORROWERS exists.")
    else:
        print("Table BORROWERS does not exist.")
finally:
    print("happy")


# Load the cleaned CSV data into Snowflake
try:
    write_pandas(conn, borrow, table_name="BORROWERS")
    print("Data loaded successfully.")
except Exception as e:
    print(f"Error loading data: {e}")

borrow.to_csv('borrow_data.csv', index=False)

cur.execute(f"""
    CREATE OR REPLACE STAGE my_stage;
""")

cur.execute(f"""
    PUT file:///home/snow/Desktop/data-enginner/borrow_data.csv @my_stage;
""")

# Load data into Snowflake table
try:
    cur.execute(f"""
        COPY INTO BORROWERS
        FROM @my_stage/borrow_data.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    """)
    print("Data loaded successfully.")
except Exception as e:
    print(f"Error loading data: {e}")

# Close cursor and connection
cur.close()
conn.close()
