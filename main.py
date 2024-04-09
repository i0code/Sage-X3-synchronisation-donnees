from fastapi import FastAPI

app = FastAPI()

# Import and include your route definitions
from app.routes  import customers, sales, date,company,itmmaster


app.include_router(date.router) 
app.include_router(customers.router) 
app.include_router(sales.router) 
app.include_router(company.router) 
app.include_router(itmmaster.router) 