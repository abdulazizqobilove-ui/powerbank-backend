from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request
from pydantic import BaseModel
from datetime import datetime
import uuid

from database import SessionLocal, engine
from models import Base, User, Rental

app = FastAPI()

telegram_users = {}
login_tokens = {}

connections = {}

# =========================
# 📦 MODELS
# =========================

class RentRequest(BaseModel):
    station_id: int
    user_id: int

class ReturnRequest(BaseModel):
    rental_id: int

class CardRequest(BaseModel):
    user_id: int
    number: str

# =========================
# 🔋 STATIONS
# =========================

stations = [
    {
        "id": 1,
        "name": "Street Game Club",
        "powerbanks": 5,
        "charged": 5,
        "empty_slots": 2,
        "address": "ул. Табиби",
        "lat": 41.3111,
        "lng": 69.2797
    }
]

@app.get("/")
def home():
    return {"message": "Powerbank backend works"}

@app.get("/stations")
def get_stations():
    return stations

# =========================
# 🔋 RENT → DB
# =========================

@app.post("/rent")
def rent_powerbank(data: RentRequest):
    db = SessionLocal()

    existing = db.query(Rental).filter(
        Rental.user_id == data.user_id,
        Rental.status == "active"
    ).first()

    if existing:
        raise HTTPException(status_code=400, detail="Already has active rental")

    rental = Rental(
        user_id=data.user_id,
        station_id=data.station_id,
        status="active",
        start_time=datetime.now()
    )

    db.add(rental)
    db.commit()
    db.refresh(rental)

    return {
        "id": rental.id,
        "user_id": rental.user_id,
        "station_id": rental.station_id,
        "status": rental.status
    }

# =========================
# 📜 RENTALS FROM DB
# =========================

@app.get("/rentals/{user_id}")
def get_rentals(user_id: int):
    db = SessionLocal()

    rentals = db.query(Rental).filter(Rental.user_id == user_id).all()

    return [
        {
            "id": r.id,
            "user_id": r.user_id,
            "station_id": r.station_id,
            "status": r.status,
            "start_time": r.start_time,
            "end_time": r.end_time,
            "cost": r.cost
        }
        for r in rentals
    ]

# =========================
# 📱 TELEGRAM LOGIN → DB
# =========================

@app.get("/create_token")
def create_token():
    token = str(uuid.uuid4())
    login_tokens[token] = None
    return {"token": token}

@app.post("/telegram_webhook")
async def telegram_webhook(request: Request):
    data = await request.json()

    try:
        message = data.get("message", {})
        chat_id = message["chat"]["id"]
        text = message.get("text", "")

        if text.startswith("/start"):
            parts = text.split(" ")

            if len(parts) > 1:
                token = parts[1]

                if token in login_tokens:

                    db = SessionLocal()

                    existing_user = db.query(User).filter(
                        User.chat_id == str(chat_id)
                    ).first()

                    if existing_user:
                        user_id = existing_user.id
                    else:
                        new_user = User(chat_id=str(chat_id))
                        db.add(new_user)
                        db.commit()
                        db.refresh(new_user)

                        user_id = new_user.id

                    telegram_users[chat_id] = user_id
                    login_tokens[token] = user_id

                    print(f"✅ LOGIN SUCCESS: {user_id}")

    except Exception as e:
        print("TG ERROR:", e)

    return {"ok": True}

@app.get("/check_token/{token}")
def check_token(token: str):
    user_id = login_tokens.get(token)

    if not user_id:
        return {"status": "waiting"}

    return {"status": "ok", "user_id": user_id}

# =========================
# 🔌 WEBSOCKET
# =========================

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    connections[user_id] = websocket

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connections.pop(user_id, None)

# =========================
# 🗄 DB INIT
# =========================

Base.metadata.create_all(bind=engine)