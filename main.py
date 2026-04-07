from fastapi import FastAPI, HTTPException, WebSocket
from pydantic import BaseModel
from datetime import datetime
import json
import random

BOT_TOKEN = "8172622865:AAFGX-nF9ECX_pZrQLy8McSCUhvYt19jbT0"

telegram_users = {}
login_tokens = {}   # 👈 ВОТ ЭТО ДОБАВЬ
user_id_counter = 1

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
# 💰 PAYMENTS
# =========================

class PaymentRequest(BaseModel):
    rental_id: int


@app.post("/payment/create")
def create_payment(data: PaymentRequest):
    global payment_id_counter

    rental = next((r for r in rentals if r["id"] == data.rental_id), None)

    if not rental:
        raise HTTPException(404, "Rental not found")

    if rental["status"] != "returned":
        raise HTTPException(400, "Rental not finished")

    if "cost" not in rental:
        raise HTTPException(400, "No cost")

    payment = {
        "id": payment_id_counter,
        "rental_id": data.rental_id,
        "amount": rental["cost"],
        "status": "pending"
    }

    payments.append(payment)
    payment_id_counter += 1

    return payment


class ConfirmPaymentRequest(BaseModel):
    payment_id: int


@app.post("/payment/confirm")
def confirm_payment(data: ConfirmPaymentRequest):

    payment = next((p for p in payments if p["id"] == data.payment_id), None)

    if not payment:
        raise HTTPException(404, "Payment not found")

    payment["status"] = "paid"

    # 🔥 найти аренду
    rental = next((r for r in rentals if r["id"] == payment["rental_id"]), None)

    if rental:
        rental["payment_status"] = "paid"

    return {"status": "paid"}

@app.post("/return")
async def return_powerbank(data: ReturnRequest):

    global payment_id_counter

    for rental in rentals:
        if rental["id"] == data.rental_id and rental["status"] == "active":

            rental["status"] = "returned"

            # время
            end_time = datetime.now()
            rental["end_time"] = end_time

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

            # обновляем станцию
            for station in stations:
                if station["id"] == rental["station_id"]:
                    station["powerbanks"] += 1
                    station["charged"] += 1

            # 🔥 НАЙТИ АКТИВНУЮ КАРТУ
            user_cards = [c for c in cards if c["user_id"] == rental["user_id"]]
            active_card = next((c for c in user_cards if c["is_active"]), None)

            if not active_card:
                raise HTTPException(400, "No active card")

            # 🔥 АВТОПЛАТЁЖ
            payment = {
                "id": payment_id_counter,
                "rental_id": rental["id"],
                "user_id": rental["user_id"],
                "card_id": active_card["id"],
                "amount": rental["cost"],
                "status": "paid"
            }

            payments.append(payment)
            payment_id_counter += 1

            rental["payment_status"] = "paid"

            # сохранить
            save_rentals()

            # websocket
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
                "cost": cost,
                "card": active_card["last4"]
            }

    raise HTTPException(status_code=404, detail="Active rental not found")

@app.get("/rentals/{user_id}")
def get_user_rentals(user_id: int):
    return [r for r in rentals if r["user_id"] == user_id]

# ==========================
# 📱 TELEGRAM LOGIN FULL
# ==========================

from fastapi import Request
import uuid

# 🔹 1. Создать токен (Flutter вызывает)
@app.get("/create_token")
def create_token():
    token = str(uuid.uuid4())
    login_tokens[token] = None
    return {"token": token}


# 🔹 2. Telegram webhook (бот ловит /start)
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
                    user_id = user_id_counter
                    user_id_counter += 1

                    telegram_users[chat_id] = user_id
                    login_tokens[token] = user_id

                    print(f"✅ LOGIN SUCCESS: {user_id}")

    except Exception as e:
        print("TG ERROR:", e)

    return {"ok": True}


# 🔹 3. Проверка логина (Flutter опрашивает)
@app.get("/check_token/{token}")
def check_token(token: str):
    user_id = login_tokens.get(token)

    if not user_id:
        return {"status": "waiting"}

    return {"status": "ok", "user_id": user_id}