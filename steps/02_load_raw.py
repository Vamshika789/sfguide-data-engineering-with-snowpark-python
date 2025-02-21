#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       02_load_raw.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
#------------------------------------------------------------------------------

import time
from snowflake.snowpark import Session

# Define connection parameters explicitly
connection_parameters = {
    "account": "al55734.us-east-2.aws",
    "user": "kiranss777",
    "password": "Workdaykiran123$",  # ⚠️ Storing passwords in code is risky
    "role": "HOL_ROLE",
    "warehouse": "HOL_WH",
    "database": "HOL_DB",
    "schema": "ANALYTICS"
}

POS_TABLES = ['country', 'franchise', 'location', 'menu', 'truck', 'order_header', 'order_detail']
CUSTOMER_TABLES = ['customer_loyalty']
TABLE_DICT = {
    "pos": {"schema": "RAW_POS", "tables": POS_TABLES},
    "customer": {"schema": "RAW_CUSTOMER", "tables": CUSTOMER_TABLES}
}

# Function to load raw tables from S3 into Snowflake
def load_raw_table(session, tname=None, s3dir=None, year=None, schema=None):
    session.use_schema(schema)
    if year is None:
        location = f"@external.frostbyte_raw_stage/{s3dir}/{tname}"
    else:
        print(f'\tLoading year {year}')
        location = f"@external.frostbyte_raw_stage/{s3dir}/{tname}/year={year}"
    
    # Load parquet data into Snowflake
    df = session.read.option("compression", "snappy").parquet(location)
    df.copy_into_table(tname)
    
    # Add metadata to the table
    comment_text = '''{"origin":"sf_sit-is","name":"snowpark_101_de","version":{"major":1, "minor":0},"attributes":{"is_quickstart":1, "source":"sql"}}'''
    sql_command = f"COMMENT ON TABLE {tname} IS '{comment_text}';"
    session.sql(sql_command).collect()

# Function to load all raw tables
def load_all_raw_tables(session):
    session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    for s3dir, data in TABLE_DICT.items():
        tnames = data['tables']
        schema = data['schema']
        for tname in tnames:
            print(f"Loading {tname}")
            if tname in ['order_header', 'order_detail']:
                for year in ['2019', '2020', '2021']:
                    load_raw_table(session, tname=tname, s3dir=s3dir, year=year, schema=schema)
            else:
                load_raw_table(session, tname=tname, s3dir=s3dir, schema=schema)

    session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL").collect()

# Function to validate raw tables
def validate_raw_tables(session):
    for tname in POS_TABLES:
        print(f'{tname}: \n\t{session.table(f"RAW_POS.{tname}").columns}\n')

    for tname in CUSTOMER_TABLES:
        print(f'{tname}: \n\t{session.table(f"RAW_CUSTOMER.{tname}").columns}\n')

# Main execution
if __name__ == "__main__":
    try:
        # Create a Snowpark session manually
        with Session.builder.configs(connection_parameters).create() as session:
            print("✅ Snowpark session created successfully!")
            load_all_raw_tables(session)
            # validate_raw_tables(session)  # Uncomment to validate table schema
    except Exception as e:
        print(f"❌ Error creating Snowpark session: {e}")
