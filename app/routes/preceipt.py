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

# Function to create PRECEIPT table in Madin Warehouse
def create_PRECEIPT_table(db_config):
    try:
        # Establish connection to Madin Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='PRECEIPT', tableType='TABLE').fetchone():
                # SQL query to create PRECEIPT table
                create_table_query = """
                CREATE TABLE PRECEIPT (
                    ID INT PRIMARY KEY IDENTITY,
                    ROWID INT,
                    CRY_0 VARCHAR(255),
                    numReception VARCHAR(255),
                    codeFournisseur VARCHAR(255),
                    dateReception DATE,
                    codeArticle VARCHAR(255),
                    quantite INT,
                    montantHT FLOAT,


                )
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("PRECEIPT table created successfully.")
            else:
                print("PRECEIPT table already exists.")
            return True
        else:
            print("Failed to connect to the database.")
            return False
    except Exception as e:
        print(f"Error creating PRECEIPT table: {e}")
        return False
    finally:
        if cnxn:
            cnxn.close()

# Function to retrieve data from Sage X3
def retrieve_data_from_sagex3():
    try:
        # Load Sage X3 database connection config from JSON
        sagex3_db = load_sage_x3_db_config()

        # Establish connection to Sage X3 database
        cnxn = get_connection(sagex3_db)
        if cnxn:
            source_query = """
                            SELECT PRECEIPT.ROWID ,
                                   PRECEIPT .CPY_0 ,
                                   PRECEIPT .PTHNUM_0 as numReception,
                                   PRECEIPT .BPSNUM_0 as codeFournisseur,
                                   PRECEIPT .RCPDAT_0 as dateReception,
                                   PRECEIPTD .ITMREF_0 as codeArticle,
                                   (QTYSTU_0-RTNQTYSTU_0) as quantite,
                                   NETPRI_0 * CHGCOE_0 * QTYSTU_0 as montanTH 
                                   from [x3v12src].[SEED].[PRECEIPT]  INNER join [x3v12src].[SEED].[PRECEIPTD] on [x3v12src].[SEED].[PRECEIPT] .PTHNUM_0 =[x3v12src].[SEED].[PRECEIPTD].PTHNUM_0
  

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

# Function to insert data into PRECEIPT table in Madin Warehouse
def insert_data_into_PRECEIPT(data, clear_table=False):
    # Load Madin Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Get the current maximum ROWID in the PRECEIPT table
            cursor.execute("SELECT MAX(ROWID) FROM PRECEIPT")
            max_rowid_result = cursor.fetchone()[0]
            max_rowid = max_rowid_result if max_rowid_result is not None else 0

            # Insert new data into PRECEIPT table starting from the next ROWID
            starting_rowid = max_rowid + 1
            rows_inserted = 0
            for row in data:
                if row[0] > max_rowid:  # Assuming ROWID is at index 2 in each row
                    cursor.execute("INSERT INTO PRECEIPT (ROWID,CRY_0,numReception,codeFournisseur,dateReception,codeArticle,quantite,montantHT) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                   (row[0], row[1], row[2],row[3], row[4], row[5],row[6], row[7]))
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

# Function to insert data into PRECEIPT table in Madin Warehouse
def insert_data_into_PRECEIPT_sync(data):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Truncate PRECEIPT table before inserting new data to ensure synchronization
            cursor.execute("TRUNCATE TABLE PRECEIPT")

            # Insert new data into PRECEIPT table
            for _, row in data.iterrows():
                cursor.execute("INSERT INTO PRECEIPT (ROWID,CRY_0,numReception,codeFournisseur,dateReception,codeArticle,quantite,montantHT) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                               tuple(row))

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


# Function to retrieve data from PRECEIPT table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "SELECT ROWID,CRY_0,numReception,codeFournisseur,dateReception,codeArticle,quantite,montantHT FROM [dw_madin].[dbo].[PRECEIPT]"
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
        return insert_data_into_PRECEIPT_sync(source_data)

    
@router.post("/madin/warehouse/create-table-preceipt")
async def create_PRECEIPT_table_handler(request: Request):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Create PRECEIPT table in Madin Warehouse
    if create_PRECEIPT_table(madin_warehouse_db):
        return Response(status_code=201, content="Table created successfully.")
    else:
        return Response(status_code=500, content="Failed to create table.")
    

@router.post("/madin/warehouse/insert-data-preceipt")
async def insert_data_into_PRECEIPT_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")

    # Convert DataFrame to list of tuples
    sagex3_data_tuples = [tuple(x) for x in sagex3_data.values]

    if insert_data_into_PRECEIPT(sagex3_data_tuples):
        return Response(status_code=201, content="Data inserted into PRECEIPT table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into PRECEIPT table.")


@router.get("/sage/preceipt")
async def retrieve_data_from_sage_PRECEIPT(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage PRECEIPT.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict

@router.post("/madin/warehouse/synchronize_preceipt")
async def synchronize_PRECEIPT_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")
