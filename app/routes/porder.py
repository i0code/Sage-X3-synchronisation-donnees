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

# Function to create PORDER table in Madin Warehouse
def create_PORDER_table(db_config):
    try:
        # Establish connection to Madin Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='PORDER', tableType='TABLE').fetchone():
                # SQL query to create PORDER table
                create_table_query = """
                CREATE TABLE PORDER (
                    ID INT PRIMARY KEY IDENTITY,
                    ROWID INT,
                    CRY_0 VARCHAR(255),
                    numCommande VARCHAR(255),
                    codeFournisseur VARCHAR(255),
                    dateCommande DATE,
                    codeArticle VARCHAR(255),
                    quantite INT,
                    montantHT FLOAT,


                )
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("PORDER table created successfully.")
            else:
                print("PORDER table already exists.")
            return True
        else:
            print("Failed to connect to the database.")
            return False
    except Exception as e:
        print(f"Error creating PORDER table: {e}")
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
                            select PORDER.ROWID,
                               PORDER.CPY_0 ,
                               PORDER. POHNUM_0 as numCommande,
                               PORDER .BPSNUM_0  as codeFournisseur ,
                               PORDER.ORDDAT_0 as dateCommande,
                               PORDERQ.ITMREF_0 as codeArticle,
                               QTYSTU_0 as quantite,
                               NETPRI_0*CHGCOE_0*QTYSTU_0 as montantHT  
	                           from [x3v12src].[SEED].[PORDER] inner join [x3v12src].[SEED].[PORDERQ] ON PORDERQ .POHNUM_0=PORDER .POHNUM_0 inner join [x3v12src].[SEED].[PORDERP] ON PORDER.POHNUM_0 =PORDERP .POHNUM_0  

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

# Function to insert data into PORDER table in Madin Warehouse
def insert_data_into_PORDER(data, clear_table=False):
    # Load Madin Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Get the current maximum ROWID in the PORDER table
            cursor.execute("SELECT MAX(ROWID) FROM PORDER")
            max_rowid_result = cursor.fetchone()[0]
            max_rowid = max_rowid_result if max_rowid_result is not None else 0

            # Insert new data into PORDER table starting from the next ROWID
            starting_rowid = max_rowid + 1
            rows_inserted = 0
            for row in data:
                if row[0] > max_rowid:  # Assuming ROWID is at index 2 in each row
                    cursor.execute("INSERT INTO PORDER (ROWID,CRY_0,numCommande,codeFournisseur,dateCommande,codeArticle,quantite,montantHT) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
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

# Function to insert data into PORDER table in Madin Warehouse
def insert_data_into_PORDER_sync(data):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Truncate PORDER table before inserting new data to ensure synchronization
            cursor.execute("TRUNCATE TABLE PORDER")

            # Insert new data into PORDER table
            for _, row in data.iterrows():
                cursor.execute("INSERT INTO PORDER (ROWID,CRY_0,numCommande,codeFournisseur,dateCommande,codeArticle,quantite,montantHT) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
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


# Function to retrieve data from PORDER table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "SELECT ROWID,CRY_0,numCommande,codeFournisseur,dateCommande,codeArticle,quantite,montantHT FROM [dw_madin].[dbo].[PORDER]"
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
        return insert_data_into_PORDER_sync(source_data)

    
@router.post("/madin/warehouse/create-table-porder")
async def create_PORDER_table_handler(request: Request):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Create PORDER table in Madin Warehouse
    if create_PORDER_table(madin_warehouse_db):
        return Response(status_code=201, content="Table created successfully.")
    else:
        return Response(status_code=500, content="Failed to create table.")
    

@router.post("/madin/warehouse/insert-data-porder")
async def insert_data_into_PORDER_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")

    # Convert DataFrame to list of tuples
    sagex3_data_tuples = [tuple(x) for x in sagex3_data.values]

    if insert_data_into_PORDER(sagex3_data_tuples):
        return Response(status_code=201, content="Data inserted into PORDER table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into PORDER table.")


@router.get("/sage/porder")
async def retrieve_data_from_sage_porder(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage porder.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict

@router.post("/madin/warehouse/synchronize_porder")
async def synchronize_porder_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")
