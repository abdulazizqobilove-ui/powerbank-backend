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

admin = Admin(app=app, engine=engine, templates_dir="templates")

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

# =========================
# ⚙️ APP
# =========================

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
    try:
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
    finally:
        db.close()

@app.get("/cards/{user_id}")
def get_cards(user_id: int):
    db = SessionLocal()
    try:
        cards = db.query(Card).filter(Card.user_id == user_id).all()

        return [
            {
                "id": c.id,
                "brand": c.brand,
                "last4": c.last4,
                "is_active": c.is_active
            } for c in cards
        ]
    finally:
        db.close()

@app.post("/cards/select")
def select_card(data: dict):
    db = SessionLocal()
    try:
        db.query(Card).filter(Card.user_id == data["user_id"]).update({"is_active": 0})
        db.query(Card).filter(Card.id == data["card_id"]).update({"is_active": 1})

        db.commit()
        return {"status": "ok"}
    finally:
        db.close()

@app.delete("/cards/{card_id}")
def delete_card(card_id: int):
    db = SessionLocal()
    try:
        card = db.query(Card).filter(Card.id == card_id).first()

        if not card:
            raise HTTPException(404, "Card not found")

        # 💣 сколько карт у пользователя
        user_cards = db.query(Card).filter(
            Card.user_id == card.user_id
        ).all()

        if len(user_cards) <= 1:
            raise HTTPException(400, "Нельзя удалить последнюю карту")

        was_active = card.is_active == 1

        db.delete(card)
        db.commit()

        # 🔥 если удалили активную → делаем другую активной
        if was_active:
            new_card = db.query(Card).filter(
                Card.user_id == card.user_id
            ).first()

            if new_card:
                new_card.is_active = 1
                db.commit()

        return {"status": "deleted"}

    finally:
        db.close()

# =========================
# 🔋 RENT
# =========================

@app.post("/rent")
def rent_powerbank(data: RentRequest):
    db = SessionLocal()
    try:
        # ❌ 1. есть активная аренда
        active = db.query(Rental).filter(
            Rental.user_id == data.user_id,
            Rental.status == "active"
        ).first()

        if active:
            raise HTTPException(400, "Already has active rental")

        # 💣 2. ЕСТЬ ДОЛГ
        debt = db.query(Rental).filter(
            Rental.user_id == data.user_id,
            Rental.payment_status == "waiting"
        ).first()

        if debt:
            raise HTTPException(400, "Сначала оплатите предыдущую аренду")

        # 💳 3. ПРОВЕРКА КАРТЫ (ВОТ ЭТО МЫ ДОБАВИЛИ)
        card = db.query(Card).filter(
            Card.user_id == data.user_id,
            Card.is_active == 1
        ).first()

        if not card:
            raise HTTPException(400, "Добавьте карту")

        # ✅ 4. создаём аренду
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

    finally:
        db.close()

@app.get("/rentals/{user_id}")
def get_rentals(user_id: int):
    db = SessionLocal()
    try:
        rentals = db.query(Rental).filter(Rental.user_id == user_id).all()

        return [
            {
                "id": r.id,
                "status": r.status,
                "start_time": r.start_time.isoformat(),
                "end_time": r.end_time.isoformat() if r.end_time else None,
                "cost": r.cost,
                "payment_status": r.payment_status
            }
            for r in rentals
        ]
    finally:
        db.close()

# =========================
# 🔁 RETURN
# =========================

@app.post("/return")
async def return_powerbank(data: ReturnRequest):
    db = SessionLocal()
    try:
        rental = db.query(Rental).filter(
            Rental.id == data.rental_id,
            Rental.status == "active"
        ).first()

        if not rental:
            raise HTTPException(404, "Not found")

        if rental.status != "active":
            raise HTTPException(400, "Уже завершено")

        if rental.payment_status == "paid":
            raise HTTPException(400, "Уже оплачено")

        rental.end_time = datetime.now()

        duration = rental.end_time - rental.start_time
        hours = duration.total_seconds() / 3600

        # 💰 расчёт стоимости
        if hours <= 1:
            cost = 7
        elif hours <= 24:
            cost = 14
        else:
            extra_days = int((hours - 24) / 24) + 1
            cost = 14 + (extra_days * 14)

        # 💣 ЛИМИТ
        MAX_COST = 150

        if cost >= MAX_COST:
            cost = MAX_COST
            rental.status = "lost"
        else:
            rental.status = "returned"

        rental.cost = cost
        rental.payment_status = "waiting"

        db.commit()

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

    finally:
        db.close()

@app.post("/force-close")
def force_close():
    db = SessionLocal()

    rentals = db.query(Rental).filter(
        Rental.status == "active"
    ).all()

    for r in rentals:
        duration = datetime.now() - r.start_time
        hours = duration.total_seconds() / 3600

        # 💰 считаем стоимость
        if hours <= 1:
            cost = 7
        elif hours <= 24:
            cost = 14
        else:
            extra_days = int((hours - 24) / 24) + 1
            cost = 14 + (extra_days * 14)

        MAX_COST = 150

        # 💣 закрываем только если дошло до лимита
        if cost >= MAX_COST:
            r.status = "lost"
            r.cost = MAX_COST
            r.payment_status = "waiting"
            r.end_time = datetime.now()

    db.commit()
    return {"status": "ok"}

# =========================
# 💰 PAYMENTS
# =========================
import requests

ALIF_API = "https://alif.shop/api/payment/create"
API_KEY = "ТВОЙ_API_KEY"

import requests
import threading
import time

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

        if rental.payment_status == "paid":
            raise HTTPException(400, "Already paid")

        payment = Payment(
            rental_id=rental.id,
            amount=rental.cost,
            status="pending"
        )

        db.add(payment)
        db.commit()
        db.refresh(payment)

        # 🔥 АВТО ОПЛАТА ЧЕРЕЗ 3 СЕК
        def fake_pay():
            time.sleep(3)

            db2 = SessionLocal()
            try:
                p = db2.query(Payment).filter(Payment.id == payment.id).first()
                if p:
                    p.status = "paid"

                    r = db2.query(Rental).filter(Rental.id == p.rental_id).first()
                    if r:
                        r.payment_status = "paid"

                db2.commit()
            finally:
                db2.close()

        threading.Thread(target=fake_pay).start()

        return {
            "payment_url": f"https://google.com?q=pay_{payment.id}"
        }

    finally:
        db.close()

@app.get("/payment/status/{rental_id}")
def payment_status(rental_id: int):
    db = SessionLocal()
    try:
        rental = db.query(Rental).filter(Rental.id == rental_id).first()

        if not rental:
            raise HTTPException(404, "Not found")

        return {
            "status": rental.payment_status
        }
    finally:
        db.close()

@app.post("/payment/confirm")
def confirm_payment(data: ConfirmPaymentRequest):
    db = SessionLocal()
    try:
        payment = db.query(Payment).filter(Payment.id == data.payment_id).first()

        if not payment:
            raise HTTPException(404, "Payment not found")

        payment.status = "paid"

        rental = db.query(Rental).filter(Rental.id == payment.rental_id).first()
        if rental:
            rental.payment_status = "paid"

        db.commit()

        return {"status": "paid"}
    finally:
        db.close()

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
        pass
    finally:
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
    db = SessionLocal()
    try:
        token = str(uuid.uuid4())

        db.add(LoginToken(token=token))
        db.commit()

        return {"token": token}
    finally:
        db.close()

@app.post("/telegram_webhook")
async def telegram_webhook(request: Request):
    data = await request.json()

    db = SessionLocal()  # 👈 ВАЖНО

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

                lt = db.query(LoginToken).filter(LoginToken.token == token).first()

                if lt:
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

                    lt.user_id = user.id
                    db.commit()

    except Exception as e:
        print("TG ERROR:", e)

    finally:
        db.close()  # 👈 ВОТ ГДЕ ДОЛЖЕН БЫТЬ

    return {"ok": True}

@app.get("/check_token/{token}")
def check_token(token: str):
    db = SessionLocal()
    try:
        lt = db.query(LoginToken).filter(LoginToken.token == token).first()

        if not lt or not lt.user_id:
            return {"status": "waiting"}

        user = db.query(User).filter(User.id == lt.user_id).first()

        return {
            "status": "ok",
            "user_id": user.id,
            "name": user.name or ""
        }
    finally:
        db.close()

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

@app.get("/stats/active")
def active_rentals():
    db = SessionLocal()

    active = db.query(Rental).filter(Rental.status == "active").count()

    return {"active": active}

from datetime import datetime, timedelta

@app.get("/dashboard")
def dashboard():
    db = SessionLocal()

    # =====================
    # 💰 ДОХОД
    # =====================
    payments = db.query(Payment).filter(Payment.status == "paid").all()

    total_income = sum(p.amount for p in payments)

    # сегодня
    today_date = datetime.utcnow().date()
    today_income = sum(
        p.amount for p in payments
        if p.created_at and p.created_at.date() == today_date
    )

    # =====================
    # 📈 7 ДНЕЙ
    # =====================
    days = {}
    for i in range(7):
        d = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
        days[d] = 0

    for p in payments:
        if not p.created_at:
            continue

        d = p.created_at.strftime("%Y-%m-%d")
        if d in days:
            days[d] += p.amount

    # =====================
    # 🔋 АКТИВНЫЕ АРЕНДЫ
    # =====================
    active = db.query(Rental).filter(Rental.status == "active").count()

    # =====================
    # 👤 ТОП ПОЛЬЗОВАТЕЛИ
    # =====================
    users = {}

    for p in payments:
        rental = db.query(Rental).filter(Rental.id == p.rental_id).first()
        if not rental:
            continue

        user_id = rental.user_id

        if user_id not in users:
            users[user_id] = 0

        users[user_id] += p.amount

    # сортировка топа
    top_users = dict(sorted(users.items(), key=lambda x: x[1], reverse=True)[:5])

    # =====================
    # 🔴 ДОЛГИ
    # =====================
    rentals = db.query(Rental).filter(Rental.payment_status == "waiting").all()

    total_debt = 0
    debt_users = {}

    for r in rentals:
        amount = r.cost or 0

        total_debt += amount

        user_id = r.user_id

        if user_id not in debt_users:
            debt_users[user_id] = 0

        debt_users[user_id] += amount
    
    # =====================
    return {
    "total_income": total_income,
    "today_income": today_income,
    "active_rentals": active,
    "daily": days,
    "top_users": top_users,
    "debts": {
        "total_debt": total_debt,
        "debtors_count": len(debt_users),
        "users": debt_users
    }
}

class LoginToken(Base):
    __tablename__ = "login_tokens"
    token = Column(String, primary_key=True)
    user_id = Column(Integer, nullable=True)

Base.metadata.create_all(bind=engine)

@app.get("/stats/debts")
def debts():
    db = SessionLocal()

    rentals = db.query(Rental).filter(Rental.payment_status == "waiting").all()

    total_debt = 0
    users = {}

    for r in rentals:
        amount = r.cost or 0

        total_debt += amount

        user_id = r.user_id

        if user_id not in users:
            users[user_id] = 0

        users[user_id] += amount

    return {
        "total_debt": total_debt,
        "debtors_count": len(users),
        "users": users
    }

from fastapi.responses import HTMLResponse

@app.get("/dashboard-ui", response_class=HTMLResponse)
def dashboard_ui():
    return """
    <html>
    <head>
        <title>Dashboard</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body { font-family: Arial; background: #111; color: white; padding: 20px; }
            .card { background: #222; padding: 20px; border-radius: 10px; margin: 10px 0; }
            .row { display: flex; gap: 20px; }
        </style>
    </head>
    <body>

        <h1>🚀 Powerbank Dashboard</h1>

        <div class="row">
            <div class="card">💰 Total: <span id="total"></span></div>
            <div class="card">📅 Today: <span id="today"></span></div>
            <div class="card">🔋 Active: <span id="active"></span></div>
        </div>

        <div class="card">
            <canvas id="chart"></canvas>
        </div>

        <!-- 👤 TOP USERS -->
        <div class="card">
            <h2>👤 Top Users</h2>
            <ul id="top-users"></ul>
        </div>

        <!-- 🔴 DEBTS -->
        <div class="card">
            <h2>🔴 Debts</h2>
            <div>Total: <span id="debt-total"></span></div>
            <ul id="debt-users"></ul>
        </div>

        <script>
            fetch('/dashboard')
                .then(res => res.json())
                .then(data => {
                    document.getElementById('total').innerText = data.total_income;
                    document.getElementById('today').innerText = data.today_income;
                    document.getElementById('active').innerText = data.active_rentals;

                    const labels = Object.keys(data.daily);
                    const values = Object.values(data.daily);

                    new Chart(document.getElementById('chart'), {
                        type: 'line',
                        data: {
                            labels: labels,
                            datasets: [{
                                label: 'Income',
                                data: values
                            }]
                        }
                    });

                    // 👤 TOP USERS
                    const topList = document.getElementById('top-users');
                    topList.innerHTML = "";

                    for (const user in data.top_users) {
                        const li = document.createElement('li');
                        li.innerText = "User " + user + " — " + data.top_users[user];
                        topList.appendChild(li);
                    }

                    // 🔴 DEBTS
                    document.getElementById('debt-total').innerText = data.debts.total_debt;

                    const debtList = document.getElementById('debt-users');
                    debtList.innerHTML = "";

                    for (const user in data.debts.users) {
                        const li = document.createElement('li');
                        li.innerText = "User " + user + " — " + data.debts.users[user];
                        debtList.appendChild(li);
                    }
                });
        </script>

    </body>
    </html>
    """