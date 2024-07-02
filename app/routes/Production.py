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

# Function to create PRODUCTION table in Madin Warehouse
def create_PRODUCTION_table(db_config):
    try:
        # Establish connection to Madina Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='PRODUCTION', tableType='TABLE').fetchone():
                # SQL query to create PRODUCTION table
                create_table_query = """
                CREATE TABLE PRODUCTION (
                    ID INT PRIMARY KEY IDENTITY,
                    trackingnum INT,
                    codearticle INT,
                    company VARCHAR(255),
                    quantiterealise INT,
                    daterealisation DATE,
                )
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("PRODUCTION table created successfully.")
            else:
                print("PRODUCTION table already exists.")
            return True
        else:
            print("Failed to connect to the database.")
            return False
    except Exception as e:
        print(f"Error creating PRODUCTION table: {e}")
        return False
    finally:
        if cnxn:
            cnxn.close()

# Function to retrieve data from Sage X3
def retrieve_data_from_sagex3():
    sagex3_db = load_sage_x3_db_config()
    # Establish connection to Sage X3 database
    cnxn = get_connection(sagex3_db)
    if cnxn:
        try:
            source_query = "select ITMREF_0 as codearticle ,LEGCPY_0 as company ,CPLQTY_0 as quantiterealise,IPTDAT_0 as daterealisation from SEED .MFGITMTRK inner join SEED.FACILITY on FACILITY .FCY_0 =MFGFCY_0"
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

# Function to insert data into PRODUCTION table in Madina Warehouse
def insert_data_into_PRODUCTION(data):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()
    # Establish connection to Madina Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()
            # Get the current maximum ROWID in the PRODUCTION table
            cursor.execute("SELECT MAX(trackingnum) FROM PRODUCTION")
            max_rowid_result = cursor.fetchone()
            max_rowid = max_rowid_result[0] if max_rowid_result is not None else 0

            # Insert new data into PRODUCTION table starting from the next ROWID
            rows_inserted = 0
            for row in data:
                if row[0] > max_rowid:
                    cursor.execute("INSERT INTO PRODUCTION (trackingnum ,codearticle ,company ,quantiterealise ,daterealisation ) VALUES (?, ?, ?, ?, ?)",
                                   (row[0], row[1], row[2],row[3],row[4]))
                    rows_inserted += 1

            cnxn.commit()
            
            if rows_inserted == 0:
                print("No modifications exist. No rows were inserted.")
            else:
                print(f"{rows_inserted} rows inserted into the target database.")
            return True
        except pyodbc.Error as db_err:
            print(f"Database error: {db_err}")
            return False
        except Exception as e:
            print(f"Error inserting data into target database: {e}")
            return False
        finally:
            cnxn.close()
    else:
        print("Failed to connect to the target database.")
        return False

# Function to update data in PRODUCTION table in Madina Warehouse
def insert_data_into_PRODUCTION_sync(data):
    # Load Madin Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

             # Iterate over the data and perform upsert
            for row in data:
                cursor.execute("""
                    MERGE INTO PRODUCTION AS target
                    USING (VALUES (?, ?, ?, ?, ?)) AS source (trackingnum, codearticle, company, quantiterealise, daterealisation)
                    ON target.trackingnum = source.trackingnum
                    WHEN MATCHED THEN
                        UPDATE SET
                            codearticle = source.codearticle,
                            company = source.company,
                            quantiterealise = source.quantiterealise,
                            daterealisation = source.daterealisation
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (trackingnum, codearticle, company, quantiterealise, daterealisation)
                        VALUES (source.trackingnum, source.codearticle, source.company, source.quantiterealise, source.daterealisation);
                """, (row['trackingnum'], row['codearticle'], row['company'], row['quantiterealise'], row['daterealisation']))

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

# Function to retrieve data from PRODUCTION table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "select ITMREF_0 as codearticle ,LEGCPY_0 as company ,CPLQTY_0 as quantiterealise,IPTDAT_0 as daterealisation from SEED .MFGITMTRK inner join SEED.FACILITY on FACILITY .FCY_0 =MFGFCY_0"
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
        return insert_data_into_PRODUCTION_sync(source_data.values.tolist())

@router.post("/madin/warehouse/insert-data-production")
async def insert_data_into_PRODUCTION_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")
    
    if insert_data_into_PRODUCTION(sagex3_data.values.tolist()):
        return Response(status_code=201, content="Data inserted into PRODUCTION table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into PRODUCTION table.")

@router.get("/sage/production")
async def retrieve_data_from_sage_production(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage production.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict

@router.post("/madin/warehouse/create-table-production")
async def create_production_table_handler(request: Request):
    # Load Madin Warehouse database connection config
    madin_warehouse_db_config = load_madin_warehouse_db_config()

    # Create SALESQUOTE table in Madin Warehouse
    if create_PRODUCTION_table(madin_warehouse_db_config):
        return Response(status_code=201, content="Table created successfully.")
    else:
        return Response(status_code=500, content="Failed to create table.")

@router.post("/madin/warehouse/synchronize_production")
async def synchronize_PRODUCTION_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")