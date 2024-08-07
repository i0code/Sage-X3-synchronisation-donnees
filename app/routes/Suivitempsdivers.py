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

# Function to retrieve data from Sage X3
def retrieve_data_from_sagex3():
    sagex3_db = load_sage_x3_db_config()
    # Establish connection to Sage X3 database
    cnxn = get_connection(sagex3_db)
    if cnxn:
        try:
            source_query = "SELECT MFGTRKNUM_0 AS numerosuivi, LEGCPY_0 AS company, CPLQTY_0 AS quantite, REJCPLQTY_0 AS quantiterejet, CPLWST_0 AS posterealise, CPLLAB_0 AS morealise, CASE WHEN TIMUOMCOD_0 = 2 THEN CPLSETTIM_0 / 60.0 ELSE CPLSETTIM_0 END AS tempsreglage, CASE WHEN TIMUOMCOD_0 = 2 THEN CPLOPETIM_0 / 60.0 ELSE CPLOPETIM_0 END AS tempsopérealise, MSGNUM_0 AS message, IPTDAT_0 AS dateimputation, TIMTYP_0 AS Time_type, TIMUOMCOD_0 AS Time_unit FROM SEED.MFGOPETRK INNER JOIN SEED.FACILITY ON FACILITY.FCY_0 = MFGFCY_0 WHERE TIMTYP_0 = 3"
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

# Function to create SUIVITEMPSDIVERS table in Madin Warehouse
def create_SUIVITEMPSDIVERS_table(db_config):
    try:
        # Establish connection to Madina Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            if not cursor.tables(table='SUIVITEMPSDIVERS', tableType='TABLE').fetchone():
                # SQL query to create SUIVITEMPSDIVERS table
                create_table_query = """
                CREATE TABLE SUIVITEMPSDIVERS (
                numerosuivi VARCHAR(50) PRIMARY KEY,
                company VARCHAR(50),
                quantite FLOAT,
                quantiterejet FLOAT,
                posterealise VARCHAR(50),
                morealise VARCHAR(50),
                tempsreglage FLOAT,
                tempsoperealise FLOAT,  
                message INT,
                dateimputation DATETIME,
                Time_type INT,        -- Adding [Time type] column
                Time_unit INT
                )"""
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("SUIVITEMPSDIVERS table created successfully.")
                return "created"
            else:
                print("SUIVITEMPSDIVERS table already exists.")
                return "exists"
        else:
            print("Failed to connect to the database.")
            return "connection_failed"
    except Exception as e:
        print(f"Error creating SUIVITEMPSDIVERS table: {e}")
        return "error"
    finally:
        if cnxn:
            cnxn.close()

# Function to insert data into SUIVITEMPSDIVERS table in Madina Warehouse
def insert_data_into_SUIVITEMPSDIVERS(data):
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()
    # Establish connection to Madina Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()
            
            rows_inserted = 0
            for row in data:
                print(f"Inserting row: {row}")  # Debugging line to check the row data
                # Check if the tracking number already exists in the table
                cursor.execute("SELECT COUNT(1) FROM SUIVITEMPSDIVERS WHERE numerosuivi = ?", (row[0],))
                exists = cursor.fetchone()[0]
                if not exists:
                    # Insert into SUIVITEMPSDIVERS table
                    cursor.execute("""
                    INSERT INTO SUIVITEMPSDIVERS (
                        numerosuivi, company, quantite, quantiterejet, posterealise, 
                        morealise, tempsreglage, tempsoperealise, message, dateimputation, Time_type, Time_unit
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row[0], row[1], row[2], row[3], 
                    row[4], row[5], row[6], row[7], 
                    row[8], row[9], row[10], row[11]
                ))
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
    
# Function to update data in SUIVITEMPSDIVERS table in Madina Warehouse
def insert_data_into_SUIVITEMPSDIVERS_sync(data):
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
                        MERGE INTO SUIVITEMPSDIVERS AS target
                        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
                            numerosuivi, company, quantite, quantiterejet, posterealise, 
                            morealise, tempsreglage, tempsoperealise, message, dateimputation, 
                            Time_type, Time_unit
                        )
                        ON target.numerosuivi = source.numerosuivi
                        WHEN MATCHED THEN
                            UPDATE SET
                                company = source.company,
                                quantite = source.quantite,
                                quantiterejet = source.quantiterejet,
                                posterealise = source.posterealise,
                                morealise = source.morealise,
                                tempsreglage = source.tempsreglage,
                                tempsoperealise = source.tempsoperealise,
                                message = source.message,
                                dateimputation = source.dateimputation,
                                Time_type = source.Time_type,  -- Added
                                Time_unit = source.Time_unit   -- Added
                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT (numerosuivi, company, quantite, quantiterejet, posterealise, 
                                    morealise, tempsreglage, tempsoperealise, message, dateimputation, 
                                    Time_type, Time_unit)
                            VALUES (source.numerosuivi, source.company, source.quantite, source.quantiterejet, 
                                    source.posterealise, source.morealise, source.tempsreglage, source.tempsoperealise, 
                                    source.message, source.dateimputation, 
                                    source.Time_type, source.Time_unit);  
                    """, (
                        row[0], row[1], row[2], row[3], 
                        row[4], row[5], row[6], row[7], 
                        row[8], row[9], row[10], row[11]
                    ))
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

# Function to retrieve data from SUIVITEMPSDIVERS table in Madin Warehouse
def retrieve_data_from_target():
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()

    # Establish connection to Madin Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            source_query = "SELECT numerosuivi, company, quantite, quantiterejet, posterealise, morealise, tempsreglage, tempsopérealise, message, dateimputation, Time_type, Time_unit FROM SUIVITEMPSDIVERS;"
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
        return insert_data_into_SUIVITEMPSDIVERS_sync(source_data.values.tolist())
    
@router.get("/sage/SUIVITEMPSDIVERS")
async def retrieve_data_from_sage_SUIVITEMPSDIVERS(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage SUIVITEMPSDIVERS.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict
    
@router.post("/madin/warehouse/create-table-SUIVITEMPSDIVERS")
async def create_SUIVITEMPSDIVERS_table_handler(request: Request):
    # Load Madin Warehouse database connection config
    madin_warehouse_db_config = load_madin_warehouse_db_config()

    # Create SUIVITEMPSDIVERS table in Madina Warehouse
    result = create_SUIVITEMPSDIVERS_table(madin_warehouse_db_config)
    if result == "created":
        return Response(status_code=201, content="Table created successfully.")
    elif result == "exists":
        return Response(status_code=200, content="Table already exists.")
    elif result == "connection_failed":
        return Response(status_code=500, content="Failed to connect to the database.")
    else:
        return Response(status_code=500, content="Failed to create table.")
    
@router.post("/madin/warehouse/insert-data-SUIVITEMPSDIVERS")
async def insert_data_into_SUIVITEMPSDIVERS_handler(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()
    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage X3.")
     # Print the data for debugging
    print(sagex3_data)
    
    if insert_data_into_SUIVITEMPSDIVERS(sagex3_data.values.tolist()):
        return Response(status_code=201, content="Data inserted into SUIVITEMPSDIVERS table successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Failed to insert data into SUIVITEMPSDIVERS table.")
    
@router.post("/madin/warehouse/synchronize_SUIVITEMPSDIVERS")
async def synchronize_SUIVITEMPSDIVERS_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")