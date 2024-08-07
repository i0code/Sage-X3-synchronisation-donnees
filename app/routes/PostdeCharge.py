import pyodbc
import pandas as pd
import os
import json
from fastapi.responses import Response
from fastapi import APIRouter, HTTPException, Request
from datetime import datetime

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
             # Query for WORKSTATIO data
            workstatio_query = """
                SELECT WST_0 as poste, TWD_0 as [schema], WSTDES_0 as designationPoste, 
                       LEGCPY_0 as company 
                FROM SEED.WORKSTATIO 
                INNER JOIN SEED.FACILITY ON FACILITY.FCY_0 = WCRFCY_0
            """
            workstatio_data = pd.read_sql(workstatio_query, cnxn)
            
            # Query for TABWEEDIA data
            tabweedia_query = """
                SELECT TWD_0 as [schema], DAYCAP_0 as Lundi, DAYCAP_1 as Mardi, 
                       DAYCAP_2 as Mercredi, DAYCAP_3 as Jeudi, DAYCAP_4 as Vendredi, 
                       DAYCAP_5 as Samedi, DAYCAP_6 as Dimanche
                FROM SEED.TABWEEDIA
            """
            tabweedia_data = pd.read_sql(tabweedia_query, cnxn)
            
            # Merge the data on schema
            merged_data = pd.merge(workstatio_data, tabweedia_data, on='schema')
            
            start_date = datetime(2016, 1, 1)
            end_date = datetime.today()
            date_range = pd.date_range(start_date, end_date)
            
            final_data = pd.DataFrame()
            
            for _, row in merged_data.iterrows():
                temp_df = pd.DataFrame(date_range, columns=['dateschema'])
                temp_df['poste'] = row['poste']
                temp_df['schema'] = row['schema']
                temp_df['designationPoste'] = row['designationPoste']
                temp_df['company'] = row['company']
                temp_df['day_of_week'] = temp_df['dateschema'].dt.day_name()
                
                day_cap_mapping = {
                    'Monday': row['Lundi'],
                    'Tuesday': row['Mardi'],
                    'Wednesday': row['Mercredi'],
                    'Thursday': row['Jeudi'],
                    'Friday': row['Vendredi'],
                    'Saturday': row['Samedi'],
                    'Sunday': row['Dimanche']
                }
                
                temp_df['tempstheorique'] = temp_df['day_of_week'].map(day_cap_mapping)
                final_data = pd.concat([final_data, temp_df], ignore_index=True)
                
            final_data = final_data[['poste', 'schema', 'designationPoste', 'company', 'dateschema', 'tempstheorique']]
            print("Retrieved Data Type:", type(final_data))  # Debugging line
            print("Retrieved Data Head:\n", final_data.head())  # Debugging line
            return final_data
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            cnxn.close()
    else:
        print("Failed to connect to the source database.")
        return None
    
# Function to create POSTEDECHARGE table in Madin Warehouse
def create_POSTEDECHARGE_table(db_config):
    try:
        # Establish connection to Madina Warehouse database
        cnxn = get_connection(db_config)
        if cnxn:
            cursor = cnxn.cursor()
            # Check if the table already exists
            table_exists = cursor.tables(table='POSTEDECHARGE', tableType='TABLE').fetchone()
            if table_exists:
                print("POSTEDECHARGE table already exists.")
                return "exists"
            else:
                # SQL query to create POSTEDECHARGE table
                create_table_query = """
                CREATE TABLE POSTEDECHARGE (
                poste VARCHAR(50),
                [schema] VARCHAR(50),
                designationPoste VARCHAR(255),
                company VARCHAR(50),
                dateschema DATE,
                tempstheorique FLOAT
            );
                """
                # Execute the query
                cursor.execute(create_table_query)
                # Commit changes
                cnxn.commit()
                print("POSTEDECHARGE table created successfully.")
            return True
        else:
            print("Failed to connect to the database.")
            return False
    except Exception as e:
        print(f"Error creating POSTEDECHARGE table: {e}")
        return False
    finally:
        if cnxn:
            cnxn.close()

# Function to insert data into POSTEDECHARGE table in Madina Warehouse
def insert_data_into_POSTEDECHARGE(data, clear_table=False, batch_size=1000):
    if not isinstance(data, pd.DataFrame):
        print(f"Received data type: {type(data)}")  # Debugging line to check the data type
        raise ValueError("Data must be a pandas DataFrame")

    madin_warehouse_db = load_madin_warehouse_db_config()
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()
            rows_inserted = 0
            
            if clear_table:
                cursor.execute("TRUNCATE TABLE POSTEDECHARGE")
                cnxn.commit()
            
            # Sort the data to ensure it is inserted in an organized manner
            data_sorted = data[['poste', 'schema', 'designationPoste', 'company', 'dateschema', 'tempstheorique']].drop_duplicates()
            data_sorted['dateschema'] = pd.to_datetime(data_sorted['dateschema'])  # Ensure dates are in datetime format
            data_sorted = data_sorted.sort_values(by=['poste', 'schema', 'dateschema'])

            insert_query = """
                INSERT INTO POSTEDECHARGE (poste, [schema], designationPoste, company, dateschema, tempstheorique)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            
            # Insert in batches
            for start in range(0, len(data_sorted), batch_size):
                batch = data_sorted.iloc[start:start + batch_size]
                try:
                    cursor.executemany(insert_query, batch.values.tolist())
                    rows_inserted += len(batch)
                except pyodbc.Error as db_err:
                    print(f"Database error on batch starting at row {start}: {db_err}")
                    print(batch)
            
            cnxn.commit()
            
            if rows_inserted == 0:
                print("No rows were inserted.")
            else:
                print(f"{rows_inserted} rows inserted into POSTEDECHARGE table.")
            return True

        except pyodbc.Error as db_err:
            print(f"Database error: {db_err}")
            return False
        
        except Exception as e:
            print(f"Error inserting data into POSTEDECHARGE table: {e}")
            return False
        
        finally:
            cnxn.close()
    
    else:
        print("Failed to connect to the database.")
        return False

# Function to sync data in POSTEDECHARGE table in Madina Warehouse
def sync_data_with_POSTEDECHARGE(data, batch_size=1000):
    if not isinstance(data, pd.DataFrame):
        print(f"Received data type: {type(data)}")   # Debugging line to check the data type
        raise ValueError("Data must be a pandas DataFrame")

    madin_warehouse_db = load_madin_warehouse_db_config()
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            cursor = cnxn.cursor()

            # Create a temporary table
            cursor.execute("""
                IF OBJECT_ID('tempdb..#TempPosteDeCharge') IS NOT NULL
                    DROP TABLE #TempPosteDeCharge;
                
                CREATE TABLE #TempPosteDeCharge (
                    poste NVARCHAR(255),
                    [schema] NVARCHAR(255),
                    designationPoste NVARCHAR(255),
                    company NVARCHAR(255),
                    dateschema DATE,
                    tempstheorique NVARCHAR(255)
                )
            """)
            cnxn.commit()

            # Insert data into the temporary table in batches
            data_sorted = data[['poste', 'schema', 'designationPoste', 'company', 'dateschema', 'tempstheorique']].drop_duplicates()
            data_sorted['dateschema'] = pd.to_datetime(data_sorted['dateschema'])
            data_sorted = data_sorted.sort_values(by=['poste', 'schema', 'dateschema'])

            insert_temp_query = """
                INSERT INTO #TempPosteDeCharge (poste, [schema], designationPoste, company, dateschema, tempstheorique)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            
            for start in range(0, len(data_sorted), batch_size):
                batch = data_sorted.iloc[start:start + batch_size]
                cursor.executemany(insert_temp_query, batch.values.tolist())
            cnxn.commit()

            # Update existing rows and insert new rows
            update_query = """
                UPDATE p
                SET p.designationPoste = t.designationPoste,
                    p.company = t.company,
                    p.tempstheorique = t.tempstheorique
                FROM POSTEDECHARGE p
                INNER JOIN #TempPosteDeCharge t
                ON p.poste = t.poste AND p.[schema] = t.[schema] AND p.dateschema = t.dateschema
            """
            cursor.execute(update_query)
            cnxn.commit()
            rows_updated = cursor.rowcount

            insert_query = """
                INSERT INTO POSTEDECHARGE (poste, [schema], designationPoste, company, dateschema, tempstheorique)
                SELECT t.poste, t.[schema], t.designationPoste, t.company, t.dateschema, t.tempstheorique
                FROM #TempPosteDeCharge t
                LEFT JOIN POSTEDECHARGE p
                ON t.poste = p.poste AND t.[schema] = p.[schema] AND t.dateschema = p.dateschema
                WHERE p.poste IS NULL
            """
            cursor.execute(insert_query)
            cnxn.commit()
            rows_inserted = cursor.rowcount

            print(f"{rows_updated} rows updated and {rows_inserted} rows inserted into POSTEDECHARGE table.")
            return True

        except pyodbc.Error as db_err:
            print(f"Database error: {db_err}")
            return False
        
        except Exception as e:
            print(f"Error syncing data with POSTEDECHARGE table: {e}")
            return False
        
        finally:
            cnxn.close()
    
    else:
        print("Failed to connect to the database.")
        return False

# Function to retrieve data from POSTEDECHARGE table in Madina Warehouse
def retrieve_data_from_postedecharge():
    # Load Madina Warehouse database connection config
    madin_warehouse_db = load_madin_warehouse_db_config()
    
    # Establish connection to Madina Warehouse database
    cnxn = get_connection(madin_warehouse_db)
    if cnxn:
        try:
            query = """
                SELECT poste, [schema], designationPoste, company, dateschema, tempstheorique
                FROM POSTEDECHARGE
            """
            data = pd.read_sql(query, cnxn)
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
    # Retrieve data from the source database
    source_data = retrieve_data_from_sagex3()
    if source_data is None:
        print("Failed to retrieve data from source database.")
        return False

    # Retrieve data from the target database
    target_data = retrieve_data_from_postedecharge()
    if target_data is None:
        print("Failed to retrieve data from target database.")
        return False

    # Compare the source and target data
    if source_data.equals(target_data):
        print("Data in target database matches data in source database.")
        return True
    else:
        print("Data in target database does not match data in source database. Synchronizing...")
        # Synchronize data by inserting into the target database
        return sync_data_with_POSTEDECHARGE(source_data)








@router.get("/sage/POSTEDECHARGE")
async def retrieve_data_from_sage_post(request: Request):
    # Retrieve data from Sage X3
    sagex3_data = retrieve_data_from_sagex3()  

    if sagex3_data is None:
        return Response(status_code=500, content="Failed to retrieve data from Sage post.")
    else:
        # Convert data to dictionary format
        data_dict = sagex3_data.to_dict(orient="records")
        return data_dict
    
@router.post("/madin/warehouse/create-table-POSTEDECHARGE")
async def create_POSTEDECHARGE_table_handler(request: Request):
    # Load Madin Warehouse database connection config
    madin_warehouse_db_config = load_madin_warehouse_db_config()

    # Create POSTEDECHARGE table in Madin Warehouse
    result = create_POSTEDECHARGE_table(madin_warehouse_db_config)
    if result is True:
        return Response(status_code=201, content="Table created successfully.")
    elif result == "exists":
        return Response(status_code=200, content="POSTEDECHARGE table already exists.")
    else:
        return Response(status_code=500, content="Failed to create table.")

    
@router.post("/madin/warehouse/insert-data-POSTEDECHARGE")
async def insert_data_into_POSTEDECHARGE_handler(request: Request):
    data = retrieve_data_from_sagex3()
    if data is not None:
        print("Data Type before Insert:", type(data))  # Debugging line
        if isinstance(data, pd.DataFrame):
            result = insert_data_into_POSTEDECHARGE(data)
            if result:
                return {"message": "Data inserted successfully"}
            else:
                raise HTTPException(status_code=500, detail="Failed to insert data into POSTEDECHARGE")
        else:
            raise HTTPException(status_code=500, detail="Retrieved data is not a DataFrame")
    else:
        raise HTTPException(status_code=500, detail="Failed to retrieve data from Sage X3")
    
@router.post("/madin/warehouse/synchronize-POSTEDECHARGE")
async def synchronize_POSTEDECHARGE_data(request: Request):
    if synchronize_data():
        return Response(status_code=200, content="Data synchronized successfully.")
    else:
        return Response(status_code=500, content="Internal Server Error - Data synchronization failed.")
