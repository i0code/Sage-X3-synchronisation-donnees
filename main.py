from fastapi import FastAPI

app = FastAPI()

# Import and include your route definitions
from app.routes  import customers, sales, date,company,itmmaster,salesOrder,salesDelivery,salesInvoice,salesQuote,fournisseur,porder,preceipt,Production,SuivitempsOF,Suivitempsdivers,PostdeCharge

app.include_router(date.router) 
app.include_router(customers.router) 
app.include_router(sales.router) 
app.include_router(company.router) 
app.include_router(itmmaster.router)
app.include_router(salesOrder.router) 
app.include_router(salesDelivery.router)
app.include_router(salesInvoice.router)
app.include_router(salesQuote.router)
app.include_router(fournisseur.router)    
app.include_router(porder.router)    
app.include_router(preceipt.router)   
app.include_router(Production.router)
app.include_router(SuivitempsOF.router)
app.include_router(Suivitempsdivers.router)
app.include_router(PostdeCharge.router)
