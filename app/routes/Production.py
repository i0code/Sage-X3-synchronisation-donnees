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
                    numerosuivi NVARCHAR(255) NOT NULL,
                    codearticle NVARCHAR(255) NOT NULL,
                    company NVARCHAR(255) NOT NULL,
                    quantiterealise FLOAT NOT NULL,
                    daterealisation DATETIME NOT NULL,
                    UNIQUE(numerosuivi)
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
            source_query = "select MFGTRKNUM_0 as numerosuivi ,ITMREF_0 as codearticle ,LEGCPY_0 as company ,CPLQTY_0 as quantiterealise,IPTDAT_0 as daterealisation from [x3v12src].[SEED].[MFGITMTRK] inner join [x3v12src].[SEED].[FACILITY] on FACILITY .FCY_0 =MFGFCY_0"
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
            
            rows_inserted = 0
            for row in data:
                # Check if the tracking number already exists in the table
                cursor.execute("SELECT COUNT(1) FROM PRODUCTION WHERE numerosuivi = ?", (row[0],))
                exists = cursor.fetchone()[0]
                if not exists:
                    cursor.execute("INSERT INTO PRODUCTION (numerosuivi, codearticle, company, quantiterealise, daterealisation) VALUES (?, ?, ?, ?, ?)",
                                   (row[0], row[1], row[2], row[3], row[4]))
                    rows_inserted += 1

            cnxn.commit()
            
            return rows_inserted
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

            rows_inserted = 0
            rows_updated = 0

             # Iterate over the data and perform upsert
            for row in data:
                # Check if the row exists in the target table
                cursor.execute("""
                    SELECT COUNT(*) FROM PRODUCTION WHERE numerosuivi = ?
                """, (row[0],))
                exists = cursor.fetchone()[0]

                # Perform the upsert
                
                cursor.execute("""
                    MERGE INTO PRODUCTION AS target
                    USING (VALUES (?, ?, ?, ?, ?)) AS source (numerosuivi, codearticle, company, quantiterealise, daterealisation)
                    ON target.numerosuivi = source.numerosuivi
                    WHEN MATCHED THEN
                        UPDATE SET
                            codearticle = source.codearticle,
                            company = source.company,
                            quantiterealise = source.quantiterealise,
                            daterealisation = source.daterealisation
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (numerosuivi, codearticle, company, quantiterealise, daterealisation)
                        VALUES (source.numerosuivi, source.codearticle, source.company, source.quantiterealise, source.daterealisation);
                """, (row[0], row[1], row[2], row[3], row[4]))

                # Update the counters based on existence
                if exists:
                    rows_updated += 1
                else:
                    rows_inserted += 1

            cnxn.commit()
            print(f"Data synchronized successfully. Rows inserted: {rows_inserted}, Rows updated: {rows_updated}")
            return {"rows_inserted": rows_inserted, "rows_updated": rows_updated}
        
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
            source_query = "SELECT numerosuivi, codearticle, company, quantiterealise, daterealisation FROM PRODUCTION"
            data = pd.read_sql(source_query, cnxn)
            return data
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            cnxn.close()
    else:
        print("Failed to connect to the target database.")
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
    
    rows_inserted = insert_data_into_PRODUCTION(sagex3_data.values.tolist())
    
    if rows_inserted > 0:
        return Response(status_code=201, content="Data inserted into PRODUCTION table successfully.")
    elif rows_inserted == 0:
        return Response(status_code=200, content="No modifications exist. No rows were inserted.")
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

    # Create PRODUCTION table in Madin Warehouse
    try:
        table_created = create_PRODUCTION_table(madin_warehouse_db_config)
        if table_created:
            # Check if the table already exists
            cnxn = get_connection(madin_warehouse_db_config)
            if cnxn:
                cursor = cnxn.cursor()
                if cursor.tables(table='PRODUCTION', tableType='TABLE').fetchone():
                    return Response(status_code=200, content="PRODUCTION table already exists.")
                else:
                    return Response(status_code=201, content="PRODUCTION table created successfully.")
            else:
                return Response(status_code=500, content="Failed to connect to the database.")
        else:
            return Response(status_code=500, content="Failed to create table.")
    except Exception as e:
        return Response(status_code=500, content=f"An error occurred: {e}")


@router.post("/madin/warehouse/synchronize_production")
async def synchronize_PRODUCTION_data(request: Request):
    sync_result = synchronize_data()
    
    if isinstance(sync_result, dict) and sync_result.get("rows_inserted") == 0 and sync_result.get("rows_updated") == 0:
        return Response(status_code=200, content="Data already synchronized. No changes were made.")
    elif sync_result:
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")