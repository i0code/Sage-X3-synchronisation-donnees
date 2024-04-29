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

# Function to create SINVOICE table in Madin Warehouse
def create_SALESINVOICE_table(db_config):
    try:
        # Establish connection to Madina Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='SALESINVOICE', tableType='TABLE').fetchone():
                # SQL query to create SALESINVOICE table
                create_table_query = """
                CREATE TABLE SALESINVOICE (
                    ID INT PRIMARY KEY IDENTITY,
                    rowID INT,
                    societe VARCHAR(255),
                    numFacture VARCHAR(255),
                    ligneFacture INT,
                    codeClient VARCHAR(255),
                    dateFacture DATE,
                    codeArticle VARCHAR(255),
                    quantite FLOAT,
                    montantHT FLOAT,
                    montantTTC FLOAT,
                    representant VARCHAR(255),
                    montantPrixRevi FLOAT,
                    marge FLOAT
                    
                )
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("SALESINVOICE table created successfully.")
            else:
                print("SALESINVOICE table already exists.")
            return True
        else:
            print("Failed to connect to the database.")
            return False
    except Exception as e:
        print(f"Error creating SALESINVOICE table: {e}")
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
            source_query = "select SINVOICED.ROWID as rowID,SINVOICE.CPY_0 as societe,SINVOICE.NUM_0 as numFacture,SINVOICED .SIDLIN_0 as ligneFacture,SINVOICE.BPR_0  as codeClient ,SINVOICE.ACCDAT_0 as dateFacture,SINVOICED.ITMREF_0 as codeArticle,QTY_0 as quantite,NETPRI_0 *QTY_0*SNS_0 *RATMLT_0 as montantHT ,NETPRIATI_0 *SNS_0 *QTY_0*RATMLT_0 as montantTTC,(select YREP_0 from [x3v12src].[dbo].YREPRE where YBPCNUM_0=BPR_0 and YCPY_0 =SINVOICE.CPY_0 ) as representant,CPRPRI_0 *SNS_0 *RATMLT_0*QTY_0 as MontantPrixRevi,NETPRI_0 *QTY_0*SNS_0 *RATMLT_0 - CPRPRI_0 *SNS_0 *RATMLT_0*QTY_0 as marge  from [x3v12src].[SEED].[SINVOICE] inner join [x3v12src].[SEED].[SINVOICED] ON SINVOICE .NUM_0=SINVOICED .NUM_0" 
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

# Function to insert data into SALESINVOICE table in Madina Warehouse
def insert_data_into_SALESINVOICE(data, clear_table=False):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()
    # Establish connection to Madina Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Get the current maximum ROWID in the SINVOICE table
            cursor.execute("SELECT MAX(rowID) FROM SALESINVOICE")
            max_rowid_result = cursor.fetchone()[0]
            max_rowid = max_rowid_result if max_rowid_result is not None else 0

            # Insert new data into SALESINVOICE table starting from the next ROWID
            starting_rowid = max_rowid + 1
            rows_inserted = 0
            for row in data:
                if row[0] > max_rowid:
                    cursor.execute("INSERT INTO SALESINVOICE (rowID,societe,numFacture,ligneFacture,codeClient,dateFacture,codeArticle,quantite,montantHT,montantTTC,representant,montantPrixRevi,marge) VALUES (?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?,?,?)",
                                   (row[0], row[1], row[2],row[3], row[4], row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12]))
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


# Function to retrieve data from SALESINVOICE table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "SELECT rowID,societe,numFacture,ligneFacture,codeClient,dateFacture,codeArticle,quantite,montantHT,montantTTC,representant,montantPrixRevi,marge FROM [dw_madin].[dbo].[SALESINVOICE]"
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
    

# Function to insert data into SALESINVOICE table in Madin Warehouse
def insert_data_into_SALESINVOICE_sync(data):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Truncate SALESINVOICE table before inserting new data to ensure synchronization
            cursor.execute("TRUNCATE TABLE SALESINVOICE")

            # Insert new data into SALESINVOICE table
            for row in data:
                cursor.execute("INSERT INTO SALESINVOICE (rowID,societe,numFacture,ligneFacture,codeClient,dateFacture,codeArticle,quantite,montantHT,montantTTC,representant,montantPrixRevi,marge) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))

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
        return insert_data_into_SALESINVOICE_sync(source_data.values.tolist())




@router.post("/madin/warehouse/insert-data-salesinvoice")
async def insert_data_into_SALESINVOICE_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")
    
    if insert_data_into_SALESINVOICE(sagex3_data.values.tolist()):
        return Response(status_code=201, content="Data inserted into SALESINVOICE table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into SALESINVOICE table.")


@router.get("/sage/salesinvoice")
async def retrieve_data_from_sage_customers(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage customers.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict

@router.post("/madin/warehouse/create-table-salesinvoice")
async def create_SALESINVOICE_table_handler(request: Request):
    # Load Madin Warehouse database connection config
    madin_warehouse_db_config = load_madin_warehouse_db_config()

    # Create SALESINVOICE table in Madin Warehouse
    if create_SALESINVOICE_table(madin_warehouse_db_config):
        return Response(status_code=201, content="Table created successfully.")
    else:
        return Response(status_code=500, content="Failed to create table.")

@router.post("/madin/warehouse/synchronize_salesinvoice")
async def synchronize_SALESINVOICE_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")