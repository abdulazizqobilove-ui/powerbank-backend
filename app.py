from fastapi import FastAPI

from database import engine, Base
from routers.auth import router as auth_router
from routers.stations import router as stations_router
from routers.rentals import router as rentals_router

app = FastAPI()

app.include_router(auth_router)
app.include_router(stations_router)
app.include_router(rentals_router)

Base.metadata.create_all(bind=engine)


@app.get("/")
def home():
    return {"message": "Powerbank API running"}