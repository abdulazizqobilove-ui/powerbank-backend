from fastapi import APIRouter
from database import SessionLocal
from models import Station

router = APIRouter()


@router.get("/stations")
def get_stations():

    db = SessionLocal()

    stations = db.query(Station).all()

    result = []

    for station in stations:
        result.append({
            "id": station.id,
            "name": station.name,
            "powerbanks": station.powerbanks
        })

    db.close()

    return result


@router.post("/stations")
def create_station(name: str, powerbanks: int):

    db = SessionLocal()

    station = Station(
        name=name,
        powerbanks=powerbanks
    )

    db.add(station)
    db.commit()
    db.refresh(station)

    db.close()

    return station