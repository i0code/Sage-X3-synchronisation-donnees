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

# Function to create ITMMASTER table in Madin Warehouse
def create_ITMMASTER_table(db_config):
    try:
        # Establish connection to Madin Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='ITMMASTER', tableType='TABLE').fetchone():
                # SQL query to create ITMMASTER table
                create_table_query = """
                CREATE TABLE ITMMASTER (
                    ID INT PRIMARY KEY IDENTITY,
                    ITMREF_0 VARCHAR(255),
                    ITMDES_0 VARCHAR(255),
                    TCLCOD_0 VARCHAR(255),
                    TSICOD_0 VARCHAR(255),
                    TSICOD_NAME_0 VARCHAR(255),
                    TSICOD_1 VARCHAR(255),
                    TSICOD_NAME_1 VARCHAR(255),
                    TSICOD_2 VARCHAR(255),
                    TSICOD_NAME_2 VARCHAR(255),
                    TSICOD_3 VARCHAR(255), 
                    TSICOD_NAME_3 VARCHAR(255),
                    TSICOD_4 VARCHAR(255), 
                    TSICOD_NAME_4 VARCHAR(255),
                    ROWID INT
                )
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("ITMMASTER table created successfully.")
            else:
                print("ITMMASTER table already exists.")
            return True
        else:
            print("Failed to connect to the database.")
            return False
    except Exception as e:
        print(f"Error creating ITMMASTER table: {e}")
        return False
    finally:
        if cnxn:
            cnxn.close()





# Function to retrieve data from Sage X3
def retrieve_data_from_sagex3():
    # Load Sage X3 database connection config from JSON
    sagex3_db = load_sage_x3_db_config()

    # Establish connection to Sage X3 database
    cnxn = get_connection(sagex3_db)
    if cnxn:
        try:
            source_query = """  
                           SELECT ITMREF_0, 
                           ITMDES1_0 + ' , ' + ITMDES2_0 + ' , ' + ITMDES3_0 AS ITMDES_0, 
                           TCLCOD_0,
                           TSICOD_0,
                           (SELECT TEXTE_0 FROM [x3v12src].[SEED].[ATEXTRA] WHERE ZONE_0 LIKE '%LNGDES%' AND CODFIC_0 = 'ATABDIV' AND LANGUE_0 = 'FRA' AND IDENT1_0 = 20 AND IDENT2_0 = TSICOD_0) AS TSICOD_NAME_0,
                           TSICOD_1,
                           (SELECT TEXTE_0 FROM [x3v12src].[SEED].[ATEXTRA] WHERE ZONE_0 LIKE '%LNGDES%' AND CODFIC_0 = 'ATABDIV' AND LANGUE_0 = 'FRA' AND IDENT1_0 = 21 AND IDENT2_0 = TSICOD_1) AS TSICOD_NAME_1,
                           TSICOD_2,
                           (SELECT TEXTE_0 FROM [x3v12src].[SEED].[ATEXTRA] WHERE ZONE_0 LIKE '%LNGDES%' AND CODFIC_0 = 'ATABDIV' AND LANGUE_0 = 'FRA' AND IDENT1_0 = 22 AND IDENT2_0 = TSICOD_2) AS TSICOD_NAME_2,
                           TSICOD_3,
                           (SELECT TEXTE_0 FROM [x3v12src].[SEED].[ATEXTRA] WHERE ZONE_0 LIKE '%LNGDES%' AND CODFIC_0 = 'ATABDIV' AND LANGUE_0 = 'FRA' AND IDENT1_0 = 23 AND IDENT2_0 = TSICOD_3) AS TSICOD_NAME_3,
                           TSICOD_4,
                           (SELECT TEXTE_0 FROM [x3v12src].[SEED].[ATEXTRA] WHERE ZONE_0 LIKE '%LNGDES%' AND CODFIC_0 = 'ATABDIV' AND LANGUE_0 = 'FRA' AND IDENT1_0 = 24 AND IDENT2_0 = TSICOD_4) AS TSICOD_NAME_4,
                           ROWID
                           FROM [x3v12src].[SEED].[ITMMASTER]
                          """
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
    
# Function to insert data into ITMMASTER table in Madin Warehouse
def insert_data_into_ITMMASTER(data, clear_table=False):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madina Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Get the current maximum ROWID in the ITMMASTER table
            cursor.execute("SELECT MAX(ROWID) FROM ITMMASTER")
            max_rowid_result = cursor.fetchone()[0]
            max_rowid = max_rowid_result if max_rowid_result is not None else 0

            # Insert new data into ITMMASTER table starting from the next ROWID
            starting_rowid = max_rowid + 1
            rows_inserted = 0
            for row in data:
                if row[13] > max_rowid:  
                    cursor.execute("INSERT INTO ITMMASTER (ITMREF_0, ITMDES_0, TCLCOD_0, TSICOD_0, TSICOD_NAME_0, TSICOD_1, TSICOD_NAME_1, TSICOD_2, TSICOD_NAME_2, TSICOD_3, TSICOD_NAME_3, TSICOD_4, TSICOD_NAME_4, ROWID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   (row[0], row[1], row[2],row[3], row[4], row[5],row[6], row[7], row[8],row[9], row[10], row[11],row[12], row[13]))
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


 # Function to insert data into ITMMASTER table in Madin Warehouse
def insert_data_into_ITMMASTER_sync(data):
    # Load Madin Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Truncate ITMMASTER table before inserting new data to ensure synchronization
            cursor.execute("TRUNCATE TABLE ITMMASTER")

            # Insert new data into ITMMASTER table
            for row in data:
                cursor.execute("INSERT INTO ITMMASTER (ITMREF_0, ITMDES_0, TCLCOD_0, TSICOD_0, TSICOD_NAME_0, TSICOD_1, TSICOD_NAME_1, TSICOD_2, TSICOD_NAME_2, TSICOD_3, TSICOD_NAME_3, TSICOD_4, TSICOD_NAME_4, ROWID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   (row[0], row[1], row[2],row[3], row[4], row[5],row[6], row[7], row[8],row[9], row[10], row[11],row[12], row[13]))

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
       

# Function to retrieve data from ITMMASTER table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madin Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "SELECT ITMREF_0, ITMDES_0, TCLCOD_0, TSICOD_0, TSICOD_NAME_0, TSICOD_1, TSICOD_NAME_1, TSICOD_2, TSICOD_NAME_2, TSICOD_3, TSICOD_NAME_3, TSICOD_4, TSICOD_NAME_4, ROWID FROM [dw_madin].[dbo].[ITMMASTER]"
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
        return insert_data_into_ITMMASTER_sync(source_data.values.tolist())
    


@router.post("/madin/warehouse/create-table-itmmaster")
async def create_ITMMASTER_table_handler(request: Request):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Create ITMMASTER table in Madin Warehouse
    if create_ITMMASTER_table(madin_warehouse_db):
        return Response(status_code=201, content="Table created successfully.")
    else:
        return Response(status_code=500, content="Failed to create table.")

@router.post("/madin/warehouse/insert-data-itmmaster")
async def insert_data_into_ITMMASTER_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")
    
    if insert_data_into_ITMMASTER(sagex3_data.values.tolist()):
        return Response(status_code=201, content="Data inserted into ITMMASTER table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into ITMMASTER table.")


@router.get("/sage/itmmaster")
async def retrieve_data_from_sage_ITMMASTER(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage ITMMASTER.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict


@router.post("/madin/warehouse/synchronize-itmmaster")
async def synchronize_ITMMASTER_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")