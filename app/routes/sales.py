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

# Function to create BPCUSTOMER table in Madin Warehouse
def create_SALESREP_table(db_config):
    try:
        # Establish connection to Madina Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='SALESREP', tableType='TABLE').fetchone():
                # SQL query to create SALESREP table
                create_table_query = """
                CREATE TABLE SALESREP (
                    ID INT PRIMARY KEY IDENTITY,
                    REPNUM_0 VARCHAR(255) UNIQUE,
                    REPNAM_0 VARCHAR(255),
                    ROWID INT
                    
                )
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("SALESREP table created successfully.")
            else:
                print("SALESREP table already exists.")
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

@router.post("/madin/warehouse/create-table-sales")
async def create_SALESREP_table_handler(request: Request):
    # Load Madina Warehouse database connection config
    madin_warehouse_db_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'madinWdb_Connection.json')
    with open(madin_warehouse_db_config_path) as file:
        madin_warehouse_db_config = json.load(file)

    # Create BPCUSTOMER table in Madin Warehouse
    if create_SALESREP_table(madin_warehouse_db_config):
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
            source_query = "SELECT [REPNUM_0], [REPNAM_0],[ROWID] FROM [x3v12src].[SEED].[SALESREP]"
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

# Function to insert data into SALESREP table in Madina Warehouse
def insert_data_into_SALESREP(data, clear_table=False):
    # Load Madina Warehouse database connection config
    madin_warehouse_DB_connection_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'madinWdb_Connection.json')
    with open(madin_warehouse_DB_connection_path) as file:
        madin_warehouse_db = json.load(file)

    # Establish connection to Madina Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Get the current maximum ROWID in the SALESREP table
            cursor.execute("SELECT MAX(ROWID) FROM SALESREP")
            max_rowid_result = cursor.fetchone()[0]
            max_rowid = max_rowid_result if max_rowid_result is not None else 0

            # Insert new data into SALESREP table starting from the next ROWID
            starting_rowid = max_rowid + 1
            rows_inserted = 0
            for row in data:
                if row[2] > max_rowid:  # Assuming ROWID is at index 2 in each row
                    cursor.execute("INSERT INTO SALESREP (REPNUM_0, REPNAM_0, ROWID) VALUES (?, ?, ?)",
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






@router.post("/madin/warehouse/insert-data-sales")
async def insert_data_into_SALESREP_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")
    
    if insert_data_into_SALESREP(sagex3_data.values.tolist()):
        return Response(status_code=201, content="Data inserted into SALESREP table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into SALESREP table.")


@router.get("/sage/sales")
async def retrieve_data_from_sage_customers(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage customers.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict




#SELECT SORDER.ROWID,SORDER. SOHNUM_0 as numcommande,SORDER .BPCORD_0 as codeclient ,SORDER.ORDDAT_0 as datecommande,SORDERQ.ITMREF_0 as codearticle,QTY_0 as quantité,NETPRI_0*CHGRAT_0*QTY_0 as montantHT,NETPRIATI_0*CHGRAT_0*QTY_0 as montantTTC  from [x3v12src].[SEED].[SORDER] inner join [x3v12src].[SEED].[SORDERQ] ON SORDERQ .SOHNUM_0=SORDER .SOHNUM_0 inner join [x3v12src].[SEED].[SORDERP] ON SORDER.SOHNUM_0 =SORDERP .SOHNUM_0