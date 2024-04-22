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

# Function to create SALESORDER table in Madin Warehouse
def create_SALESORDER_table(db_config):
    try:
        # Establish connection to Madina Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='SALESORDER', tableType='TABLE').fetchone():
                # SQL query to create SALESORDER table
                create_table_query = """
                CREATE TABLE SALESORDER (
                    ID INT PRIMARY KEY IDENTITY,
                    CPY_0 VARCHAR(255),
                    ROWID INT,
                    numcommande VARCHAR(255),
                    codeclient VARCHAR(255),
                    datecommande DATE,
                    codearticle VARCHAR(255),
                    quantité FLOAT,
                    montantHT FLOAT,
                    montantTTC FLOAT,
                    MontantPrixRevi FLOAT
                    
                )
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("SALESORDER table created successfully.")
            else:
                print("SALESORDER table already exists.")
            return True
        else:
            print("Failed to connect to the database.")
            return False
    except Exception as e:
        print(f"Error creating SALESORDER table: {e}")
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
            source_query = "SELECT SORDER.CPY_0, SORDER.ROWID,SORDER. SOHNUM_0 as numcommande,SORDER .BPCORD_0 as codeclient ,SORDER.ORDDAT_0 as datecommande,SORDERQ.ITMREF_0 as codearticle,QTY_0 as quantité,NETPRI_0*CHGRAT_0*QTY_0 as montantHT,NETPRIATI_0*CHGRAT_0*QTY_0 as montantTTC  ,CPRPRI_0*CHGRAT_0 *QTY_0 as MontantPrixRevi from [x3v12src].[SEED].[SORDER] inner join [x3v12src].[SEED].[SORDERQ] ON SORDERQ .SOHNUM_0=SORDER .SOHNUM_0 inner join [x3v12src].[SEED].[SORDERP] ON SORDER.SOHNUM_0 =SORDERP .SOHNUM_0"
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

# Function to insert data into SALESORDER table in Madina Warehouse
def insert_data_into_SALESORDER(data, clear_table=False):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()
    # Establish connection to Madina Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Get the current maximum ROWID in the SALESORDER table
            cursor.execute("SELECT MAX(ROWID) FROM SALESORDER")
            max_rowid_result = cursor.fetchone()[0]
            max_rowid = max_rowid_result if max_rowid_result is not None else 0

            # Insert new data into SALESORDER table starting from the next ROWID
            starting_rowid = max_rowid + 1
            rows_inserted = 0
            for row in data:
                if row[1] > max_rowid:
                    cursor.execute("INSERT INTO SALESORDER (CPY_0,ROWID, numcommande, codeclient, datecommande, codearticle, quantité, montantHT, montantTTC,MontantPrixRevi) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?)",
                                   (row[0], row[1], row[2],row[3], row[4], row[5],row[6], row[7], row[8], row[9]))
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


# Function to retrieve data from SALESORDER table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "SELECT CPY_0, ROWID, numcommande, codeclient, datecommande, codearticle, quantité, montantHT, montantTTC,MontantPrixRevi FROM [dw_madin].[dbo].[SALESORDER]"
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
    

# Function to insert data into SALESORDER table in Madin Warehouse
def insert_data_into_SALESORDER_sync(data):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Truncate SALESORDER table before inserting new data to ensure synchronization
            cursor.execute("TRUNCATE TABLE SALESORDER")

            # Insert new data into SALESORDER table
            for row in data:
                cursor.execute("INSERT INTO SALESORDER (CPY_0, ROWID, numcommande, codeclient, datecommande, codearticle, quantité, montantHT, montantTTC,MontantPrixRevi) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?)",
                                   (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))

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
        return insert_data_into_SALESORDER_sync(source_data.values.tolist())




@router.post("/madin/warehouse/insert-data-salesorder")
async def insert_data_into_SALESORDER_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")
    
    if insert_data_into_SALESORDER(sagex3_data.values.tolist()):
        return Response(status_code=201, content="Data inserted into SALESORDER table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into SALESORDER table.")


@router.get("/sage/salesorder")
async def retrieve_data_from_sage_customers(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage customers.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict

@router.post("/madin/warehouse/create-table-salesorder")
async def create_SALESORDER_table_handler(request: Request):
    # Load Madin Warehouse database connection config
    madin_warehouse_db_config = load_madin_warehouse_db_config()

    # Create SALESORDER table in Madin Warehouse
    if create_SALESORDER_table(madin_warehouse_db_config):
        return Response(status_code=201, content="Table created successfully.")
    else:
        return Response(status_code=500, content="Failed to create table.")

@router.post("/madin/warehouse/synchronize_salesorder")
async def synchronize_salesorder_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")