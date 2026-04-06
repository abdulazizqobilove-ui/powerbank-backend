from fastapi import FastAPI, HTTPException, WebSocket
from pydantic import BaseModel
from datetime import datetime
import json
import random

app = FastAPI()

# =========================
# 💾 STORAGE (CARDS)
# =========================
active_rentals = {}
connections = {}
cards = []
card_id_counter = 1

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

    # выключаем старые карты
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

            if station["powerbanks"] == 0:
                raise HTTPException(status_code=400, detail="No powerbanks")

            station["powerbanks"] -= 1

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

@app.post("/return")
async def return_powerbank(data: ReturnRequest):

    for rental in rentals:
        if rental["id"] == data.rental_id and rental["status"] == "active":

            rental["status"] = "returned"

            # ✅ фикс времени
            end_time = datetime.now()
            rental["end_time"] = end_time

            # ✅ правильный расчёт
            duration = end_time - rental["start_time"]
            minutes = int(duration.total_seconds() / 60)
            hours = minutes / 60

            if hours <= 1:
                cost = 6
            elif hours <= 24:
                cost = 12
            else:
                extra_days = int((hours - 24) / 24) + 1
                cost = 12 + (extra_days * 12)

            rental["cost"] = cost

            # ✅ сначала обновляем станцию
            for station in stations:
                if station["id"] == rental["station_id"]:
                    station["powerbanks"] += 1

            # ✅ сохраняем всё (уже с end_time)
            save_rentals()

            # ✅ безопасный WebSocket
            ws = connections.get(rental["user_id"])
            if ws:
                try:
                    await ws.send_json({
                        "type": "rental_finished",
                        "cost": cost
                    })
                except:
                    connections.pop(rental["user_id"], None)

            return {
                "status": "returned",
                "cost": cost
            }

    raise HTTPException(status_code=404, detail="Active rental not found")

@app.get("/rentals/{user_id}")
def get_user_rentals(user_id: int):
    return [r for r in rentals if r["user_id"] == user_id]

# ==========================
# 📱 SMS AUTH
# ==========================

sms_codes = {}

def normalize(phone: str):
    phone = phone.strip()
    if not phone.startswith("+"):
        phone = "+" + phone
    return phone


@app.post("/send_sms")
def send_sms(data: PhoneRequest):
    phone = normalize(data.phone)

    code = str(random.randint(1000, 9999))
    sms_codes[phone] = code

    print(f"SMS CODE: {code} for {phone}")

    return {"status": "ok"}


users = {}
user_id_counter = 1

@app.post("/verify_code")
def verify_code(data: VerifyRequest):
    global user_id_counter

    phone = normalize(data.phone)
    saved_code = sms_codes.get(phone)

    if saved_code != data.code:
        raise HTTPException(status_code=400, detail="Invalid code")

    if phone not in users:
        users[phone] = user_id_counter
        user_id_counter += 1

    return {"status": "ok", "user_id": users[phone]}

load_cards()
load_rentals()

import asyncio

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(ws: WebSocket, user_id: int):
    await ws.accept()
    connections[user_id] = ws

    print(f"WS CONNECTED: {user_id}")

    try:
        while True:
            # держим соединение живым
            await ws.send_json({"type": "ping"})
            await asyncio.sleep(20)

    except Exception as e:
        print("WS CLOSED:", e)
        connections.pop(user_id, None)