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

# Function to load the Madin Warehouse database connection configuration from a JSON file
def load_madin_warehouse_db_config():
    madin_warehouse_db_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'madinWdb_Connection.json')
    with open(madin_warehouse_db_config_path) as file:
        madin_warehouse_db_config = json.load(file)
    return madin_warehouse_db_config

# Function to load sagex3 database connection configuration from a JSON file
def load_sage_x3_db_config():
    sage_db_connection_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'sageX3db_Connection.json')
    with open(sage_db_connection_path) as file:
        sagex3_db_config = json.load(file)
    return sagex3_db_config

# Function to create BPCUSTOMER table in Madin Warehouse
def create_BPCUSTOMER_table(db_config):
    try:
        # Establish connection to Madin Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='BPCUSTOMER', tableType='TABLE').fetchone():
                # SQL query to create BPCUSTOMER table
                create_table_query = """
                CREATE TABLE BPCUSTOMER (
                    ID INT PRIMARY KEY IDENTITY,
                    BPCNUM_0 VARCHAR(255),
                    BPCNAM_0 VARCHAR(255),
                    BCGCOD_0 VARCHAR(255),
                    BCGCOD_NAME_0 VARCHAR(255),
                    TSCCOD_0  VARCHAR(255),
                    TSCCOD_NAME_0 VARCHAR(255),
                    TSCCOD_1 VARCHAR(255),
                    TSCCOD_NAME_1 VARCHAR(255),
                    TSCCOD_2 VARCHAR(255),
                    TSCCOD_NAME_2 VARCHAR(255),
                    ROWID INT
                )
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("BPCUSTOMER table created successfully.")
            else:
                print("BPCUSTOMER table already exists.")
            return True
        else:
            print("Failed to connect to the database.")
            return False
    except Exception as e:
        print(f"Error creating BPCUSTOMER table: {e}")
        return False
    finally:
        if cnxn:
            cnxn.close()

@router.post("/madin/warehouse/create-table-customers")
async def create_BPCUSTOMER_table_handler(request: Request):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Create BPCUSTOMER table in Madin Warehouse
    if create_BPCUSTOMER_table(madin_warehouse_db):
        return Response(status_code=201, content="Table created successfully.")
    else:
        return Response(status_code=500, content="Failed to create table.")


# Function to retrieve data from Sage X3
def retrieve_data_from_sagex3():
    try:
        # Load Sage X3 database connection config from JSON
        sagex3_db = load_sage_x3_db_config()

        # Establish connection to Sage X3 database
        cnxn = get_connection(sagex3_db)
        if cnxn:
            source_query = """
                           SELECT BPCNUM_0,
                           BPCNAM_0,
                           BCGCOD_0,
                           (SELECT TEXTE_0 from [x3v12src].[SEED].[ATEXTRA] WHERE ZONE_0 = 'DESAXX' and CODFIC_0 ='BPCCATEG' AND LANGUE_0 ='FRA' AND IDENT1_0=BCGCOD_0) AS BCGCOD_NAME_0,
                           TSCCOD_0,
                           (SELECT TEXTE_0  from [x3v12src].[SEED].[ATEXTRA] where ZONE_0  like '%LNGDES%' AND CODFIC_0 ='ATABDIV' and LANGUE_0 ='FRA'and IDENT1_0 =30 AND IDENT2_0 =TSCCOD_0) AS TSCCOD_NAME_0,
                           TSCCOD_1,
                           (SELECT TEXTE_0  from [x3v12src].[SEED].[ATEXTRA] where ZONE_0  like '%LNGDES%' AND CODFIC_0 ='ATABDIV' and LANGUE_0 ='FRA'and IDENT1_0 =31 AND IDENT2_0 =TSCCOD_1) AS TSCCOD_NAME_1,
                           TSCCOD_2,
                           (SELECT TEXTE_0  from [x3v12src].[SEED].[ATEXTRA] where ZONE_0  like '%LNGDES%' AND CODFIC_0 ='ATABDIV' and LANGUE_0 ='FRA'and IDENT1_0 =32 AND IDENT2_0 =TSCCOD_2) AS TSCCOD_NAME_2,
                           ROWID
                           FROM [x3v12src].[SEED].[BPCUSTOMER]
                           """
            data = pd.read_sql(source_query, cnxn)
            return data  # Return DataFrame directly
        else:
            print("Failed to connect to the source database.")
            return None
    except Exception as e:
        print(f"Error retrieving data from Sage X3: {e}")
        return None
    finally:
        if cnxn:
            cnxn.close()



# Function to insert data into BPCUSTOMER table in Madin Warehouse
def insert_data_into_BPCUSTOMER(data, clear_table=False):
    # Load Madin Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madina Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Get the current maximum ROWID in the BPCUSTOMER table
            cursor.execute("SELECT MAX(ROWID) FROM BPCUSTOMER")
            max_rowid_result = cursor.fetchone()[0]
            max_rowid = max_rowid_result if max_rowid_result is not None else 0

            # Insert new data into BPCCUSTOMER table starting from the next ROWID
            starting_rowid = max_rowid + 1
            rows_inserted = 0
            for row in data.itertuples(index=False, name=None):  # Use itertuples to iterate over DataFrame rows as tuples
                if row[10] > max_rowid:  # Assuming ROWID is at index 10 in each row
                    cursor.execute("INSERT INTO BPCUSTOMER (BPCNUM_0, BPCNAM_0, BCGCOD_0, BCGCOD_NAME_0,TSCCOD_0, TSCCOD_NAME_0, TSCCOD_1, TSCCOD_NAME_1,TSCCOD_2, TSCCOD_NAME_2, ROWID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))
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

# Function to insert data into BPCUSTOMER table in Madin Warehouse
def insert_data_into_BPCUSTOMER_sync(data):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Truncate BPCUSTOMER table before inserting new data to ensure synchronization
            cursor.execute("TRUNCATE TABLE BPCUSTOMER")

            # Insert new data into BPCUSTOMER table
            for row in data:
                cursor.execute("INSERT INTO BPCUSTOMER (BPCNUM_0, BPCNAM_0, BCGCOD_0, BCGCOD_NAME_0,TSCCOD_0, TSCCOD_NAME_0, TSCCOD_1, TSCCOD_NAME_1,TSCCOD_2, TSCCOD_NAME_2, ROWID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))

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


# Function to retrieve data from BPCUSTOMER table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()


    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "SELECT BPCNUM_0, BPCNAM_0, BCGCOD_0, BCGCOD_NAME_0,TSCCOD_0, TSCCOD_NAME_0, TSCCOD_1, TSCCOD_NAME_1,TSCCOD_2, TSCCOD_NAME_2, ROWID FROM [dw_madin].[dbo].[SALES]"
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
        return insert_data_into_BPCUSTOMER_sync(source_data.values.tolist())

@router.post("/madin/warehouse/insert-data-customers")
async def insert_data_into_BPCUSTOMER_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")

    if insert_data_into_BPCUSTOMER(sagex3_data):
        return Response(status_code=201, content="Data inserted into BPCUSTOMER table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into BPCUSTOMER table.")



@router.get("/sage/customers")
async def retrieve_data_from_sage_customers(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage customers.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict

@router.post("/madin/warehouse/synchronize_customers")
async def synchronize_company_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")



