import pyodbc
import pandas as pd
import os
import json
from fastapi.responses import Response
from fastapi import APIRouter, Request

router = APIRouter()

# Function to establish database connection
def get_connection(db_config):
    conn = None
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['DB_HOST']};"
            f"DATABASE={db_config['DB_CONNECTION']};"
            f"UID={db_config['DB_USERNAME']};"
            f"PWD={db_config['DB_PASSWORD']}"
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
    return conn

# Function to create COMPANY table in Madin Warehouse
def create_COMPANY_table(db_config):
    try:
        # Establish connection to Madin Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='COMPANY', tableType='TABLE').fetchone():
                # SQL query to create COMPANY table
                create_table_query = """
                CREATE TABLE COMPANY (
                    ID INT PRIMARY KEY IDENTITY,
                    CPY_0 VARCHAR(255) UNIQUE,
                    CPYNAM_0 VARCHAR(255),
                    ROWID INT
                )
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("COMPANY table created successfully.")
            else:
                print("COMPANY table already exists.")
            return True
        else:
            print("Failed to connect to the database.")
            return False
    except Exception as e:
        print(f"Error creating COMPANY table: {e}")
        return False
    finally:
        if cnxn:
            cnxn.close()

@router.post("/madin/warehouse/create-table-company")
async def create_COMPANY_table_handler(request: Request):
    # Load Madin Warehouse database connection config
    madin_warehouse_db_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'madinWdb_Connection.json')
    with open(madin_warehouse_db_config_path) as file:
        madin_warehouse_db_config = json.load(file)

    # Create COMPANY table in Madin Warehouse
    if create_COMPANY_table(madin_warehouse_db_config):
        return Response(status_code=201, content="Table created successfully.")
    else:
        return Response(status_code=500, content="Failed to create table.")


# Function to retrieve data from Sage X3
def retrieve_data_from_sagex3():
    # Load Sage X3 database connection config from JSON
    sage_DB_connection_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'sageX3db_Connection.json')
    with open(sage_DB_connection_path) as file:
        sagex3_db = json.load(file)

    # Establish connection to Sage X3 database
    cnxn = get_connection(sagex3_db)
    if cnxn:
        try:
            source_query = "SELECT [CPY_0], [CPYNAM_0], [ROWID] FROM [x3v12src].[SEED].[COMPANY]"
            data = pd.read_sql(source_query, cnxn)
            return data
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            cnxn.close()
    else:
        print("Failed to connect to the source database.")
        return None

def insert_data_into_COMPANY(data):
    # Load Madin Warehouse database connection config
    madin_warehouse_DB_connection_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'madinWdb_Connection.json')
    with open(madin_warehouse_DB_connection_path) as file:
        madin_warehouse_db = json.load(file)

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Get the current maximum ROWID in the COMPANY table
            cursor.execute("SELECT MAX(ROWID) FROM COMPANY")
            max_rowid_result = cursor.fetchone()[0]
            max_rowid = max_rowid_result if max_rowid_result is not None else 0

            # Insert new data into COMPANY table starting from the next ROWID
            starting_rowid = max_rowid + 1
            rows_inserted = 0
            for row in data:
                if row[2] > max_rowid:  # Assuming ROWID is at index 2 in each row
                    cursor.execute("INSERT INTO COMPANY (CPY_0, CPYNAM_0, ROWID) VALUES (?, ?, ?)",
                                   (row[0], row[1], row[2]))
                    rows_inserted += 1

            cnxn.commit()
            
            if rows_inserted == 0:
                print("No modifications exist. No rows were inserted.")
            else:
                print(f"{rows_inserted} rows inserted into the target database.")
            return True
        except Exception as e:
            print(f"Error inserting data into target database: {e}")
            return False
        finally:
            cnxn.close()
    else:
        print("Failed to connect to the target database.")
        return False


# Function to insert data into COMPANY table in Madin Warehouse
def insert_data_into_COMPANY_sync(data):
    # Load Madina Warehouse database connection config
    madin_warehouse_DB_connection_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'madinWdb_Connection.json')
    with open(madin_warehouse_DB_connection_path) as file:
        madin_warehouse_db = json.load(file)

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Truncate COMPANY table before inserting new data to ensure synchronization
            cursor.execute("TRUNCATE TABLE COMPANY")

            # Insert new data into COMPANY table
            for row in data:
                cursor.execute("INSERT INTO COMPANY (CPY_0, CPYNAM_0, ROWID) VALUES (?, ?, ?)",
                               (row[0], row[1], row[2]))

            cnxn.commit()
            print("Data synchronized successfully.")
            return True
        except Exception as e:
            print(f"Error inserting data into target database: {e}")
            return False
        finally:
            cnxn.close()
    else:
        print("Failed to connect to the target database.")
        return False
    

# Function to retrieve data from COMPANY table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madina Warehouse database connection config
    madin_warehouse_DB_connection_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'madinWdb_Connection.json')
    with open(madin_warehouse_DB_connection_path) as file:
        madin_warehouse_db = json.load(file)

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "SELECT [CPY_0], [CPYNAM_0], [ROWID] FROM [dw_madin].[dbo].[COMPANY]"
            data = pd.read_sql(source_query, cnxn)
            return data
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            cnxn.close()
    else:
        print("Failed to connect to the source database.")
        return None


# Function to retrieve data from COMPANY table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madina Warehouse database connection config
    madin_warehouse_DB_connection_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'madinWdb_Connection.json')
    with open(madin_warehouse_DB_connection_path) as file:
        madin_warehouse_db = json.load(file)

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "SELECT [CPY_0], [CPYNAM_0], [ROWID] FROM [dw_madin].[dbo].[COMPANY]"
            data = pd.read_sql(source_query, cnxn)
            return data
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            cnxn.close()
    else:
        print("Failed to connect to the source database.")
        return None
# Function to compare data between source and target databases and synchronize if needed
def synchronize_data():
    source_data = retrieve_data_from_sagex3()
    if source_data is None:
        return False
    
    target_data = retrieve_data_from_target()
    if target_data is None:
        return False

    if source_data.equals(target_data):
        print("Data in target database matches data in source database.")
        return True
    else:
        print("Data in target database does not match data in source database. Synchronizing...")
        return insert_data_into_COMPANY_sync(source_data.values.tolist())




@router.post("/madin/warehouse/insert-data-company")
async def insert_data_into_COMPANY_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")
    
    if insert_data_into_COMPANY(sagex3_data.values.tolist()):
        return Response(status_code=201, content="Data inserted into COMPANY table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into COMPANY table.")


@router.get("/sage/company")
async def retrieve_data_from_sage_customers(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage customers.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict


@router.post("/madin/warehouse/synchronize")
async def synchronize_company_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")






## Get the current data in the COMPANY table in Madin Warehouse
#            cursor.execute("SELECT * FROM COMPANY")
#            target_data = cursor.fetchall()

            # Compare data from the source database with data in the target database
#            if len(data) != len(target_data):
#                print("Data mismatch between source and target databases.")
#                return False

#            for i, row in enumerate(data):
#                if row != target_data[i]:
#                    print("Data mismatch between source and target databases.")
#                    return False

#            print("Data in target database matches data in source database.")
#            return True