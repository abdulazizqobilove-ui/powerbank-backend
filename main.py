from fastapi import FastAPI, HTTPException, WebSocket
from pydantic import BaseModel
from datetime import datetime
import json
import random

BOT_TOKEN = "8172622865:AAFGX-nF9ECX_pZrQLy8McSCUhvYt19jbT0"

telegram_users = {}
login_tokens = {}
user_id_counter = 1
users = []
app = FastAPI()

# =========================
# 💾 STORAGE (CARDS)
# =========================
active_rentals = {}
connections = {}
cards = []
card_id_counter = 1
payments = []
payment_id_counter = 1

def save_cards():
    print("💾 SAVING FILE")
    with open("cards.json", "w") as f:
        json.dump(cards, f)

def save_rentals():
    with open("rentals.json", "w") as f:
        json.dump(rentals, f, default=str)

def load_rentals():
    global rentals, rental_id_counter
    try:
        with open("rentals.json") as f:
            rentals = json.load(f)
            if rentals:
                rental_id_counter = max(r["id"] for r in rentals) + 1
    except:
        rentals = []

def load_cards():
    global cards, card_id_counter
    try:
        with open("cards.json") as f:
            cards = json.load(f)
            if cards:
                card_id_counter = max(c["id"] for c in cards) + 1
    except:
        cards = []

users = []

def save_users():
    with open("users.json", "w") as f:
        json.dump(users, f)

def load_users():
    global users, user_id_counter
    try:
        with open("users.json") as f:
            users = json.load(f)
            if users:
                user_id_counter = max(u["id"] for u in users) + 1
    except:
        users = []

# =========================
# 📦 MODELS
# =========================

class RentRequest(BaseModel):
    station_id: int
    user_id: int

class ReturnRequest(BaseModel):
    rental_id: int

class VerifyRequest(BaseModel):
    phone: str
    code: str

class PhoneRequest(BaseModel):
    phone: str

class CardRequest(BaseModel):
    user_id: int
    number: str

# =========================
# 💳 CARDS API
# =========================

@app.post("/cards/add")
def add_card(data: CardRequest):
    global card_id_counter

    for c in cards:
        if c["user_id"] == data.user_id:
            c["is_active"] = False

    card = {
        "id": card_id_counter,
        "user_id": data.user_id,
        "brand": "VISA",
        "last4": data.number[-4:],
        "is_active": True
    }

    cards.append(card)
    card_id_counter += 1

    save_cards()
    return card

@app.get("/cards/{user_id}")
def get_cards(user_id: int):
    return [c for c in cards if c["user_id"] == user_id]

class SelectCardRequest(BaseModel):
    card_id: int
    user_id: int

@app.post("/cards/select")
def select_card(data: SelectCardRequest):
    for c in cards:
        if c["user_id"] == data.user_id:
            c["is_active"] = False

    for c in cards:
        if c["id"] == data.card_id:
            c["is_active"] = True

    save_cards()
    return {"status": "ok"}

@app.delete("/cards/{card_id}")
def delete_card(card_id: int):
    global cards
    cards = [c for c in cards if c["id"] != card_id]
    save_cards()
    return {"status": "deleted"}

# =========================
# 🔋 RENT SYSTEM
# =========================

rentals = []
rental_id_counter = 1

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

@app.post("/rent")
def rent_powerbank(data: RentRequest):
    global rental_id_counter

    for r in rentals:
        if r["user_id"] == data.user_id and r["status"] == "active":
            raise HTTPException(status_code=400, detail="Already has active rental")

    for station in stations:
        if station["id"] == data.station_id:

            if station["charged"] == 0:
                raise HTTPException(status_code=400, detail="No charged powerbanks")

            station["powerbanks"] -= 1
            station["charged"] -= 1

            rental = {
                "id": rental_id_counter,
                "user_id": data.user_id,
                "station_id": data.station_id,
                "status": "active",
                "start_time": datetime.now()
            }

            rentals.append(rental)
            save_rentals()
            rental_id_counter += 1

            return rental

    raise HTTPException(status_code=404, detail="Station not found")

# =========================
# 📱 TELEGRAM LOGIN FIXED
# =========================

from fastapi import Request
import uuid

@app.get("/create_token")
def create_token():
    token = str(uuid.uuid4())
    login_tokens[token] = None
    return {"token": token}

@app.post("/telegram_webhook")
async def telegram_webhook(request: Request):
    global user_id_counter

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

    from database import SessionLocal
    from models import User

    db = SessionLocal()

    existing_user = db.query(User).filter(User.chat_id == str(chat_id)).first()

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

from fastapi import WebSocket, WebSocketDisconnect

connections = {}

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    connections[user_id] = websocket

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connections.pop(user_id, None)

@app.get("/check_token/{token}")
def check_token(token: str):
    user_id = login_tokens.get(token)

    if not user_id:
        return {"status": "waiting"}

    return {"status": "ok", "user_id": user_id}


from database import engine
from models import Base

Base.metadata.create_all(bind=engine)

# ✅ FIX (только это добавлено правильно в конец)
load_users()
load_cards()
load_rentals()