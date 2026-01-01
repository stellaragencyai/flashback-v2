from fastapi import FastAPI
from app.api.subaccounts_api import router as subaccounts_router
from app.api.governance_api import router as governance_router

app = FastAPI(title='Flashback Cockpit API')

@app.get('/')
def root():
    return {'status': 'cockpit online'}

app.include_router(subaccounts_router, prefix='/api')
app.include_router(governance_router, prefix='/api')

