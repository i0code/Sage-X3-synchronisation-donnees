from fastapi import APIRouter, Depends
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import sqlalchemy
import os
import json
from sqlalchemy import inspect
router = APIRouter()

def generate_dates(start_year, end_year):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime.now()
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)

def get_semester(month):
    return (month - 1) // 6 + 1

def create_date_table(engine_target):
    with engine_target.connect() as conn:
        inspector = inspect(engine_target)
        if 'Date' not in inspector.get_table_names():
            create_table_query = """
            CREATE TABLE [Date] (
                id INT IDENTITY(1,1) PRIMARY KEY,
                Day INT,
                Month INT,
                Year INT,
                Week INT,
                Semester INT
            )
            """
            conn.execute(sqlalchemy.text(create_table_query))
            conn.commit()
        else:
            print("Table 'Date' already exists.")


def insert_data_into_table(engine_target):
    with engine_target.connect() as conn:
        # Fetch existing dates from the table
        existing_dates = set()
        select_existing_query = """
        SELECT Day, Month, Year FROM [Date]
        """
        existing_dates_result = conn.execute(text(select_existing_query))
        for row in existing_dates_result:
            existing_dates.add((row[0], row[1], row[2]))

        # Generate and insert new dates
        for date in generate_dates(2013, datetime.now().year):
            day = date.day
            month = date.month
            year = date.year
            week = date.isocalendar()[1]
            semester = get_semester(month)

            # Check if the date already exists in the table
            if (day, month, year) not in existing_dates:
                insert_query = """
                INSERT INTO [Date] (Day, Month, Year, Week, Semester) 
                VALUES (:day, :month, :year, :week, :semester)
                """
                bind_params = {
                    "day": day,
                    "month": month,
                    "year": year,
                    "week": week,
                    "semester": semester
                }

                # Insert data into the table
                conn.execute(text(insert_query), bind_params)
        
        conn.commit()





def get_engine_from_json():
    # Load Madina Warehouse database connection config from JSON
    madin_warehouse_DB_connection_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'madinWdb_Connection.json')
    with open(madin_warehouse_DB_connection_path) as file:
        madin_warehouse_db = json.load(file)
    
    # Create engine from JSON config
    engine = create_engine(
        f"mssql+pyodbc://{madin_warehouse_db['DB_USERNAME']}:{madin_warehouse_db['DB_PASSWORD']}@{madin_warehouse_db['DB_HOST']}/{madin_warehouse_db['DB_CONNECTION']}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return engine

@router.post("/generate-dates")
async def generate_and_insert_dates(engine_target: sqlalchemy.engine.base.Engine = Depends(get_engine_from_json)):
    create_date_table(engine_target)
    insert_data_into_table(engine_target)
    return {"message": "Dates generated and inserted successfully."}

@router.get("/get-dates")
async def get_dates(engine_target: sqlalchemy.engine.base.Engine = Depends(get_engine_from_json)):
    select_query = """
    SELECT * FROM [Date]
    """
    with engine_target.connect() as conn:
        result = conn.execute(text(select_query))
        dates = []
        for row in result:
            # Convert the row to a dictionary manually
            row_dict = {
                "id": row[0],
                "Day": row[1],
                "Month": row[2],
                "Year": row[3],
                "Week": row[4],
                "Semester": row[5]
            }
            dates.append(row_dict)
        return dates

