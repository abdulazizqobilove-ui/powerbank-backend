from fastapi import FastAPI, HTTPException, WebSocket, Request
from pydantic import BaseModel
from datetime import datetime
import uuid

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker

from sqladmin import Admin, ModelView
from starlette.requests import Request

import os

app = FastAPI()

# 🔥 DATABASE
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
print("DATABASE:", DATABASE_URL)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# 🔥 ADMIN (после engine!)
from sqladmin import Admin

admin = Admin(app=app, engine=engine)

class Card(Base):
    __tablename__ = "cards"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    brand = Column(String)
    last4 = Column(String)
    is_active = Column(Integer)

class Rental(Base):
    __tablename__ = "rentals"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    station_id = Column(Integer)
    status = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime, nullable=True)
    cost = Column(Float, default=0)
    payment_status = Column(String, default="none")

from datetime import datetime

class Payment(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key=True)
    rental_id = Column(Integer)
    amount = Column(Float)
    status = Column(String)

    created_at = Column(DateTime, default=datetime.utcnow)  # 🔥 ВОТ ЭТО

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True)
    phone = Column(String, nullable=True)
    name = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

# =========================
# ⚙️ APP
# =========================

connections = {}
login_tokens = {}

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

class PaymentRequest(BaseModel):
    rental_id: int

class ConfirmPaymentRequest(BaseModel):
    payment_id: int

# =========================
# 📍 STATIONS
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

@app.get("/stations")
def get_stations():
    return stations

# =========================
# 💳 CARDS
# =========================

@app.post("/cards/add")
def add_card(data: CardRequest):
    db = SessionLocal()

    db.query(Card).filter(Card.user_id == data.user_id).update({"is_active": 0})

    card = Card(
        user_id=data.user_id,
        brand="VISA",
        last4=data.number[-4:],
        is_active=1
    )

    db.add(card)
    db.commit()
    db.refresh(card)

    return {
        "id": card.id,
        "brand": card.brand,
        "last4": card.last4
    }

@app.get("/cards/{user_id}")
def get_cards(user_id: int):
    db = SessionLocal()
    cards = db.query(Card).filter(Card.user_id == user_id).all()

    return [
        {
            "id": c.id,
            "brand": c.brand,
            "last4": c.last4,
            "is_active": c.is_active
        } for c in cards
    ]

@app.post("/cards/select")
def select_card(data: dict):
    db = SessionLocal()

    db.query(Card).filter(Card.user_id == data["user_id"]).update({"is_active": 0})
    db.query(Card).filter(Card.id == data["card_id"]).update({"is_active": 1})

    db.commit()
    return {"status": "ok"}

@app.delete("/cards/{card_id}")
def delete_card(card_id: int):
    db = SessionLocal()
    db.query(Card).filter(Card.id == card_id).delete()
    db.commit()
    return {"status": "deleted"}

# =========================
# 🔋 RENT
# =========================

@app.post("/rent")
def rent_powerbank(data: RentRequest):
    db = SessionLocal()

    active = db.query(Rental).filter(
        Rental.user_id == data.user_id,
        Rental.status == "active"
    ).first()

    if active:
        raise HTTPException(400, "Already has active rental")

    rental = Rental(
        user_id=data.user_id,
        station_id=data.station_id,
        status="active",
        start_time=datetime.now()
    )

    db.add(rental)
    db.commit()
    db.refresh(rental)

    return {"id": rental.id}

@app.get("/rentals/{user_id}")
def get_rentals(user_id: int):
    db = SessionLocal()
    rentals = db.query(Rental).filter(Rental.user_id == user_id).all()

    return [
    {
        "id": r.id,
        "status": r.status,
        "start_time": r.start_time.isoformat(),
        "end_time": r.end_time.isoformat() if r.end_time else None,
        "cost": r.cost,
        "payment_status": r.payment_status  # 👈 ВОТ ЭТО ДОБАВИЛ
    }
    for r in rentals
]

# =========================
# 🔁 RETURN
# =========================

@app.post("/return")
async def return_powerbank(data: ReturnRequest):
    db = SessionLocal()

    rental = db.query(Rental).filter(
        Rental.id == data.rental_id,
        Rental.status == "active"
    ).first()

    if not rental:
        raise HTTPException(404, "Not found")

    rental.status = "returned"
    rental.end_time = datetime.now()

    duration = rental.end_time - rental.start_time
    hours = duration.total_seconds() / 3600

    if hours <= 1:
        cost = 7
    elif hours <= 24:
        cost = 14
    else:
        extra_days = int((hours - 24) / 24) + 1
        cost = 14 + (extra_days * 14)

    rental.cost = cost
    rental.payment_status = "waiting"

    db.commit()

    # 🔥 FIXED SOCKET
    ws = connections.get(rental.user_id)
    if ws:
        await ws.send_json({
            "type": "rental_finished",
            "cost": cost
        })

    return {
        "type": "rental_finished",
        "cost": cost,
        "rental_id": rental.id
    }

# =========================
# 💰 PAYMENTS
# =========================
import requests

ALIF_API = "https://alif.shop/api/payment/create"
API_KEY = "ТВОЙ_API_KEY"

import requests

ALIF_API = "https://alif.shop/api/payment/create"
API_KEY = "ТВОЙ_API_KEY"
CALLBACK_URL = "https://powerbank-backend.onrender.com/payment/webhook"  # 👈 поменяй!

@app.post("/payment/create")
def create_payment(data: PaymentRequest):
    db = SessionLocal()

    try:
        rental = db.query(Rental).filter(Rental.id == data.rental_id).first()

        if not rental:
            raise HTTPException(404, "Rental not found")

        # ❗ если уже оплачено — не создаём заново
        if rental.payment_status == "paid":
            raise HTTPException(400, "Already paid")

        # ✅ проверяем карту
        card = db.query(Card).filter(
            Card.user_id == rental.user_id,
            Card.is_active == 1
        ).first()

        if not card:
            raise HTTPException(400, "No active card")

        # ✅ создаём платеж
        payment = Payment(
            rental_id=rental.id,
            amount=rental.cost,
            status="pending"
        )

        db.add(payment)
        db.commit()
        db.refresh(payment)

        # ✅ запрос в Alif
        res = requests.post(
            ALIF_API,
            json={
                "amount": int(rental.cost * 100),
                "order_id": str(payment.id),  # 🔥 ключевая связь
                "description": "Powerbank rent",
                "callback_url": CALLBACK_URL  # 🔥 добавили webhook
            },
            headers={
                "Authorization": f"Bearer {API_KEY}"
            }
        )

        # ❗ проверка ответа
        if res.status_code != 200:
            raise HTTPException(400, "Alif API error")

        result = res.json()
        payment_url = result.get("payment_url")

        if not payment_url:
            raise HTTPException(400, "Payment creation failed")

        return {"payment_url": payment_url}

    except Exception as e:
        print("PAYMENT ERROR:", e)
        raise

    finally:
        db.close()

@app.get("/payment/status/{rental_id}")
def payment_status(rental_id: int):
    db = SessionLocal()

    rental = db.query(Rental).filter(Rental.id == rental_id).first()

    if not rental:
        raise HTTPException(404, "Not found")

    return {
        "status": rental.payment_status
    }

@app.post("/payment/confirm")
def confirm_payment(data: ConfirmPaymentRequest):
    db = SessionLocal()

    payment = db.query(Payment).filter(Payment.id == data.payment_id).first()

    if not payment:
        raise HTTPException(404, "Payment not found")

    payment.status = "paid"

    rental = db.query(Rental).filter(Rental.id == payment.rental_id).first()
    if rental:
        rental.payment_status = "paid"

    db.commit()

    return {"status": "paid"}

# =========================
# 🔌 WEBSOCKET
# =========================

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(ws: WebSocket, user_id: int):
    await ws.accept()
    connections[user_id] = ws

    try:
        while True:
            await ws.receive_text()
    except:
        connections.pop(user_id, None)


# =========================
# 💰 WEBHOOK (ОПЛАТА)
# =========================

@app.post("/payment/webhook")
async def payment_webhook(request: Request):
    data = await request.json()

    print("WEBHOOK:", data)

    order_id = data.get("order_id")
    status = data.get("status")

    if not order_id:
        return {"ok": False}

    db = SessionLocal()

    try:
        payment = db.query(Payment).filter(Payment.id == int(order_id)).first()

        if not payment:
            return {"ok": False}

        if status == "paid":
            payment.status = "paid"

            rental = db.query(Rental).filter(Rental.id == payment.rental_id).first()
            if rental:
                rental.payment_status = "paid"

                # 🔥 ВОТ ЭТО САМОЕ ВАЖНОЕ
                ws = connections.get(rental.user_id)
                if ws:
                    await ws.send_json({
                        "type": "payment_success"
                    })

        db.commit()

    finally:
        db.close()

    return {"ok": True}

# =========================
# 📱 TELEGRAM LOGIN
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
        chat = message.get("chat", {})

        chat_id = str(chat.get("id"))
        first_name = chat.get("first_name", "")
        last_name = chat.get("last_name", "")
        full_name = f"{first_name} {last_name}".strip()

        text = message.get("text", "")

        if text.startswith("/start"):
            parts = text.split(" ")

            if len(parts) > 1:
                token = parts[1]

                if token in login_tokens:
                    db = SessionLocal()

                    user = db.query(User).filter(User.telegram_id == chat_id).first()

                    if not user:
                        user = User(
                            telegram_id=chat_id,
                            name=full_name
                        )
                        db.add(user)
                    else:
                        user.name = full_name

                    db.commit()
                    db.refresh(user)

                    login_tokens[token] = user.id

    except Exception as e:
        print("TG ERROR:", e)

    return {"ok": True}

@app.get("/check_token/{token}")
def check_token(token: str):
    user_id = login_tokens.get(token)

    if not user_id:
        return {"status": "waiting"}

    db = SessionLocal()
    user = db.query(User).filter(User.id == user_id).first()

    return {
        "status": "ok",
        "user_id": user.id,
        "name": user.name or ""
    }

# =========================
# 🔥 ADMIN PANEL
# =========================

class UserAdmin(ModelView, model=User):
    column_list = [User.id, User.telegram_id, User.name]

class CardAdmin(ModelView, model=Card):
    column_list = [Card.id, Card.user_id, Card.last4, Card.is_active]

class RentalAdmin(ModelView, model=Rental):
    column_list = [Rental.id, Rental.user_id, Rental.status, Rental.cost]

class PaymentAdmin(ModelView, model=Payment):
    column_list = [Payment.id, Payment.rental_id, Payment.amount, Payment.status]

    def amount_formatted(self, obj):
        return f"{obj.amount} сум"

    column_formatters = {
        Payment.amount: lambda m, a: f"{m.amount} сум"
    }

admin.add_view(UserAdmin)
admin.add_view(CardAdmin)
admin.add_view(RentalAdmin)
admin.add_view(PaymentAdmin)

from datetime import datetime

@app.get("/stats")
def get_stats():
    db = SessionLocal()

    payments = db.query(Payment).filter(Payment.status == "paid").all()

    total = sum(p.amount for p in payments)

    # 💥 временно
    today = total
    month = total

    unpaid = db.query(Rental).filter(Rental.payment_status == "waiting").count()

    return {
        "total_income": total,
        "today_income": today,
        "month_income": month,
        "unpaid_rentals": unpaid
    }

from sqlalchemy import text

@app.get("/fix-db")
def fix_db():
    db = SessionLocal()

    db.execute(text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='payments' AND column_name='created_at'
            ) THEN
                ALTER TABLE payments ADD COLUMN created_at TIMESTAMP;
            END IF;
        END $$;
    """))

    db.execute(text("UPDATE payments SET created_at = NOW() WHERE created_at IS NULL"))

    db.commit()

    return {"status": "fixed"}

@app.get("/stats/daily")
def stats_daily():
    db = SessionLocal()

    result = {}

    payments = db.query(Payment).filter(Payment.status == "paid").all()

    for p in payments:
        if not p.created_at:
            continue  # ❗ пропускаем пустые

        day = p.created_at.strftime("%Y-%m-%d")

        if day not in result:
            result[day] = 0

        result[day] += p.amount

    return result

from datetime import datetime, timedelta

@app.get("/stats/7days")
def stats_7days():
    db = SessionLocal()

    result = {}
    today = datetime.utcnow()

    # создаём 7 дней
    for i in range(7):
        day = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        result[day] = 0

    payments = db.query(Payment).filter(Payment.status == "paid").all()

    for p in payments:
        if not p.created_at:
            continue

        day = p.created_at.strftime("%Y-%m-%d")

        if day in result:
            result[day] += p.amount

    return result

@app.get("/stats/top-users")
def top_users():
    db = SessionLocal()

    result = {}

    payments = db.query(Payment).filter(Payment.status == "paid").all()

    for p in payments:
        rental = db.query(Rental).filter(Rental.id == p.rental_id).first()
        if not rental:
            continue

        user_id = rental.user_id

        if user_id not in result:
            result[user_id] = 0

        result[user_id] += p.amount

    return result