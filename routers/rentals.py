from fastapi import APIRouter, Depends
from datetime import datetime

from database import SessionLocal
from models import Station, Rental
from auth_utils import get_user_from_token

router = APIRouter()


@router.post("/rent")
def rent_powerbank(station_id: int, user_id: int = Depends(get_user_from_token)):

    db = SessionLocal()

    active_rental = db.query(Rental).filter(
        Rental.user_id == user_id,
        Rental.status == "active"
    ).first()

    if active_rental:
        return {"error": "User already has an active rental"}

    station = db.query(Station).filter(
        Station.id == station_id
    ).first()

    if not station:
        return {"error": "Station not found"}

    if station.powerbanks <= 0:
        return {"error": "No powerbanks available"}

    station.powerbanks -= 1

    rental = Rental(
        user_id=user_id,
        station_id=station_id,
        status="active",
        start_time=datetime.now()
    )

    db.add(rental)
    db.commit()
    db.refresh(rental)

    db.close()

    return {"rental_id": rental.id}


@router.post("/return")
def return_powerbank(user_id: int = Depends(get_user_from_token)):

    db = SessionLocal()

    rental = db.query(Rental).filter(
        Rental.user_id == user_id,
        Rental.status == "active"
    ).first()

    if not rental:
        return {"error": "no active rental"}

    rental.status = "returned"
    rental.end_time = datetime.now()

    duration = rental.end_time - rental.start_time
    minutes = duration.total_seconds() / 60

    price_per_minute = 0.1
    cost = minutes * price_per_minute

    rental.cost = cost

    station = db.query(Station).filter(
        Station.id == rental.station_id
    ).first()

    if station:
        station.powerbanks += 1

    db.commit()
    db.close()

    return {
        "status": "returned",
        "minutes_used": round(minutes, 2),
        "cost": round(cost, 2)
    }

@router.get("/rentals")
def get_rentals(token: str):

    user_id = get_user_from_token(token)

    if not user_id:
        return {"error": "invalid token"}

    db = SessionLocal()

    rentals = db.query(Rental).filter(
        Rental.user_id == user_id
    ).all()

    result = []

    for r in rentals:
        result.append({
            "id": r.id,
            "station_id": r.station_id,
            "status": r.status,
            "start_time": r.start_time
        })

    db.close()

    return result