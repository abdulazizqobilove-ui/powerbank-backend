from fastapi import FastAPI, HTTPException, WebSocket, Request
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from sqladmin import Admin, ModelView
from alif import create_hold, capture_hold
from fastapi import Request

import threading
import time
import uuid
from fastapi import Header

app = FastAPI()

# =========================
# 🗄 DATABASE
# =========================

import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///./test.db"
)

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgres://",
        "postgresql://",
        1
    )

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

admin = Admin(
    app=app,
    engine=engine
)

# =========================
# 📦 DATABASE MODELS
# =========================

class Payment(Base):

    __tablename__ = "payments"

    id = Column(
        Integer,
        primary_key=True,
    )

    rental_id = Column(Integer)

    amount = Column(Integer)

    provider = Column(String)

    transaction_id = Column(String)

    status = Column(String)

    created_at = Column(
        DateTime,
        default=datetime.utcnow,
    )

class Card(Base):
    __tablename__ = "cards"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)

    brand = Column(String)
    last4 = Column(String)

    is_active = Column(Integer)
    position = Column(Integer)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)

    telegram_id = Column(String, unique=True)

    phone = Column(String, nullable=True)
    name = Column(String, nullable=True)
    is_blocked = Column(Integer, default=0)


class LoginToken(Base):
    __tablename__ = "login_tokens"

    token = Column(String, primary_key=True)
    user_id = Column(Integer, nullable=True)


class Rental(Base):

    __tablename__ = "rentals"

    id = Column(Integer, primary_key=True)

    user_id = Column(Integer)

    station_id = Column(Integer)

    status = Column(String)

    cost = Column(Integer, default=0)

    payment_status = Column(
        String,
        default="none"
    )

    hold_id = Column(String)

    hold_amount = Column(Integer)

    start_time = Column(
        DateTime,
        default=datetime.utcnow
    )

    end_time = Column(DateTime)

class UserAdmin(ModelView, model=User):
    column_list = [User.id, User.telegram_id, User.name]

class CardAdmin(ModelView, model=Card):
    column_list = [Card.id, Card.user_id, Card.last4]

class RentalAdmin(ModelView, model=Rental):
    column_list = [
        Rental.id,
        Rental.user_id,
        Rental.status,
        Rental.cost,
        Rental.payment_status
    ]

class PaymentAdmin(ModelView, model=Payment):

    column_list = [
        Payment.id,
        Payment.rental_id,
        Payment.amount,
        Payment.provider,
        Payment.transaction_id,
        Payment.status,
        Payment.created_at,
    ]

admin.add_view(UserAdmin)
admin.add_view(CardAdmin)
admin.add_view(RentalAdmin)
admin.add_view(PaymentAdmin)

# =========================
# 📦 REQUEST MODELS
# =========================

class SelectCardRequest(BaseModel):
    user_id: int
    card_id: int

class SendCodeRequest(BaseModel):
    phone: str


class VerifyCodeRequest(BaseModel):
    phone: str
    code: str

class RentRequest(BaseModel):
    station_id: int
    user_id: int


class ReturnRequest(BaseModel):
    rental_id: int


class AddCardRequest(BaseModel):
    user_id: int
    number: str


class PaymentRequest(BaseModel):
    rental_id: int


class ConfirmPaymentRequest(BaseModel):
    payment_id: int

# =========================
# 🗄 CREATE TABLES
# =========================
Base.metadata.create_all(bind=engine)
Base.metadata.create_all(engine)

from sqlalchemy import text

with engine.begin() as conn:

    queries = [

        "ALTER TABLE users ADD COLUMN balance FLOAT DEFAULT 0",

        "ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0",

        "ALTER TABLE rentals ADD COLUMN hold_amount FLOAT DEFAULT 0",

        "ALTER TABLE rentals ADD COLUMN hold_id VARCHAR",

        "ALTER TABLE rentals ADD COLUMN charged_amount FLOAT DEFAULT 0"
    ]

    for q in queries:
        try:
            conn.execute(text(q))
        except:
            pass

@app.post("/auth/send-code")
def send_code(data: SendCodeRequest):

    code = "1111"

    otp_codes[data.phone] = code

    print(f"OTP {data.phone}: {code}")

    return {
        "success": True
    }


@app.post("/auth/verify-code")
def verify_code(data: VerifyCodeRequest):

    saved = otp_codes.get(data.phone)

    if not saved:
        raise HTTPException(
            400,
            "Код не найден"
        )

    if saved != data.code:
        raise HTTPException(
            400,
            "Неверный код"
        )

    db = SessionLocal()

    try:

        user = db.query(User).filter(
            User.phone == data.phone
        ).first()

        if not user:

            user = User(
                telegram_id=str(uuid.uuid4()),
                phone=data.phone,
                name="User"
            )

            db.add(user)
            db.commit()
            db.refresh(user)

        token = str(uuid.uuid4())

        login = LoginToken(
            token=token,
            user_id=user.id
        )

        db.add(login)
        db.commit()

        return {
            "token": token,
            "user_id": user.id
        }

    finally:
        db.close()
# =========================
# 📍 STATIONS
# =========================

stations = [
    {
        "id": 1,
        "name": "Street Game Club",
        "powerbanks": 5,
        "lat": 41.3111,
        "lng": 69.2797
    }
]

@app.get("/stations")
def get_stations():
    return stations

# =========================
# 💰 PAYMENTS
# =========================

@app.post("/payment/create")
def create_payment(data: PaymentRequest):
    db = SessionLocal()

    try:
        rental = db.query(Rental).filter(
            Rental.id == data.rental_id
        ).first()

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

        def fake_pay():
            time.sleep(3)

            db2 = SessionLocal()

            try:
                p = db2.query(Payment).filter(
                    Payment.id == payment.id
                ).first()

                if p:
                    p.status = "paid"

                    rental2 = db2.query(Rental).filter(
                        Rental.id == p.rental_id
                    ).first()

                    if rental2:
                        rental2.payment_status = "paid"

                db2.commit()

            finally:
                db2.close()

        threading.Thread(target=fake_pay).start()

        return {
            "payment_url": f"https://google.com/pay/{payment.id}"
        }

    finally:
        db.close()


@app.get("/payment/status/{rental_id}")
def payment_status(rental_id: int):
    db = SessionLocal()

    try:
        rental = db.query(Rental).filter(
            Rental.id == rental_id
        ).first()

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
        payment = db.query(Payment).filter(
            Payment.id == data.payment_id
        ).first()

        if not payment:
            raise HTTPException(404, "Payment not found")

        payment.status = "paid"

        rental = db.query(Rental).filter(
            Rental.id == payment.rental_id
        ).first()

        if rental:
            rental.payment_status = "paid"

        db.commit()

        return {"status": "paid"}

    finally:
        db.close()

# =========================
# 🔌 CONNECTIONS
# =========================

connections = {}
otp_codes = {}

def get_user_by_token(token: str):

    db = SessionLocal()

    try:

        login = db.query(LoginToken).filter(
            LoginToken.token == token
        ).first()

        if not login:
            return None

        return db.query(User).filter(
            User.id == login.user_id
        ).first()

    finally:
        db.close()

# =========================
# 🔋 RENT
# =========================

@app.post("/rent")
def rent(
    data: RentRequest,
    authorization: str = Header(None)
):

    db = SessionLocal()

    try:

        user = get_user_by_token(
            authorization
        )

        if not user:
            raise HTTPException(
                401,
                "Unauthorized"
            )

        if user and user.is_blocked:
            raise HTTPException(
                403,
                "Аккаунт временно заблокирован"
            )

        station = next(
            (s for s in stations if s["id"] == data.station_id),
            None
        )

        if not station:
            raise HTTPException(
                404,
                "Station not found"
            )

        if station["powerbanks"] <= 0:
            raise HTTPException(
                400,
                "Нет powerbank"
            )

        last = db.query(Rental).filter(
            Rental.user_id == user.id
        ).order_by(Rental.id.desc()).first()

        if last and last.status == "active":
            raise HTTPException(
                400,
                "Already renting"
            )

        unpaid = db.query(Rental).filter(
            Rental.user_id == user.id,
            Rental.payment_status != "paid",
            Rental.status != "active"
        ).all()

        debt_sum = sum(r.cost or 0 for r in unpaid)

        if debt_sum > 150:
            raise HTTPException(
                400,
                "Превышен лимит долга"
            )

        # 🔥 ALIF HOLD
        hold = create_hold(
            user.id,
            20000
        )

        if not hold["success"]:
            raise HTTPException(
                400,
                "Ошибка hold оплаты"
            )

        rental = Rental(
            user_id=user.id,
            station_id=data.station_id,
            status="active",
            start_time=datetime.utcnow(),
            hold_id=hold["hold_id"],
            hold_amount=20000,
        )

        station["powerbanks"] -= 1

        db.add(rental)

        db.commit()
        db.refresh(rental)

        # 🔥 PAYMENT SAVE
        payment = Payment(
            rental_id=rental.id,
            amount=20000,
            provider="alif",
            transaction_id=hold["hold_id"],
            status="hold",
        )

        db.add(payment)

        db.commit()

        return {
            "id": rental.id
        }

    finally:
        db.close()

# =========================
# 📜 RENTALS
# =========================

@app.get("/rentals/{user_id}")
def get_rentals(
    user_id: int,
    authorization: str = Header(None)
):

    db = SessionLocal()

    try:

    user = get_user_by_token(
        authorization
    )

    if not user:
        raise HTTPException(
            401,
            "Unauthorized"
        )

    if user.id != user_id:
        raise HTTPException(
            403,
            "Forbidden"
        )

    try:

        data = db.query(Rental).filter(
            Rental.user_id == user_id
        ).all()

        return [
            {
                "id": r.id,
                "status": r.status,
                "cost": r.cost,
                "payment_status": r.payment_status,
                "start_time": r.start_time.isoformat(),
                "end_time": r.end_time.isoformat()
                if r.end_time else None
            }
            for r in data
        ]

    finally:
        db.close()

# =========================
# 🔁 RETURN
# =========================

@app.post("/return")
async def return_powerbank(
    data: ReturnRequest,
    authorization: str = Header(None)
):

    db = SessionLocal()

    try:

    user = get_user_by_token(
        authorization
    )

        if not user:
            raise HTTPException(
                401,
                "Unauthorized"
            )

        rental = db.query(Rental).filter(
            Rental.id == data.rental_id
        ).first()

        if not rental:
            raise HTTPException(404, "Not found")

        if rental.status != "active":
            return {"status": "already_closed"}

        rental.end_time = datetime.utcnow()

        hours = (
            rental.end_time - rental.start_time
        ).total_seconds() / 3600

        if hours <= 1:
            cost = 7

        elif hours <= 24:
            cost = 14

        else:
            extra_days = int((hours - 24) / 24) + 1
            cost = 14 + (extra_days * 14)

        rental.status = "returned"

        user = db.query(User).filter(
            User.id == rental.user_id
        ).first()

        if user:
            user.is_blocked = 0

        rental.cost = cost

        # 🔥 ALIF CAPTURE
        success = capture_hold(
            rental.hold_id,
            rental.cost,
        )

        if success:
            rental.payment_status = "paid"

        else:
            rental.payment_status = "failed"

        # 🔥 UPDATE PAYMENT
        payment = db.query(Payment).filter(
            Payment.transaction_id == rental.hold_id
        ).first()

        if payment:

            payment.status = rental.payment_status
            payment.amount = rental.cost

        station = next(
            (s for s in stations if s["id"] == rental.station_id),
            None
        )

        if station:
            station["powerbanks"] += 1

        db.commit()

        for ws in connections.get(rental.user_id, []):

            await ws.send_json({
                "type": "rental_finished",
                "cost": cost
            })

        return {
            "cost": cost,
            "payment_status": rental.payment_status
        }

    finally:
        db.close()

# =========================
# 💰 PAYMENT WEBHOOK
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
        payment = db.query(Payment).filter(
            Payment.id == int(order_id)
        ).first()

        if not payment:
            return {"ok": False}

        if status == "paid":

            payment.status = "paid"

            rental = db.query(Rental).filter(
                Rental.id == payment.rental_id
            ).first()

            if rental:
                rental.payment_status = "paid"

                for ws in connections.get(rental.user_id, []):
                    await ws.send_json({
                        "type": "payment_success"
                    })

        db.commit()

    finally:
        db.close()

    return {"ok": True}

# =========================
# 🔌 WEBSOCKET
# =========================

@app.websocket("/ws/{user_id}")
async def ws(ws: WebSocket, user_id: int):
    await ws.accept()

    connections.setdefault(user_id, []).append(ws)

    try:
        while True:
            await ws.receive_text()

    except:
        pass

    finally:
        connections[user_id].remove(ws)

# =========================
# 💳 CARDS
# =========================

@app.get("/cards/{user_id}")
def get_cards(user_id: int):

    db = SessionLocal()

    try:

        cards = db.query(Card).filter(
            Card.user_id == user_id
        ).order_by(
            Card.position,
            Card.id
        ).all()

        return [
            {
                "id": c.id,
                "brand": c.brand,
                "last4": c.last4,
                "is_active": c.is_active
            }
            for c in cards
        ]

    finally:
        db.close()


@app.post("/cards/add")
def add_card(
    data: AddCardRequest,
    authorization: str = Header(None)
):

    db = SessionLocal()

    try:

    user = get_user_by_token(
        authorization
    )

        if not user:
            raise HTTPException(
                401,
                "Unauthorized"
            )

        db.query(Card).filter(
            Card.user_id == data.user_id
        ).update({
            "is_active": 0
        })

        last_card = db.query(Card).filter(
            Card.user_id == data.user_id
        ).order_by(
            Card.position.desc()
        ).first()

        next_position = 1

        if last_card:
            next_position = (last_card.position or 0) + 1

        card = Card(
            user_id=user.id,
            brand="VISA",
            last4=data.number[-4:],
            is_active=1,
            position=next_position
        )

        db.add(card)

        db.commit()
        db.refresh(card)

        return {
            "id": card.id,
            "brand": card.brand,
            "last4": card.last4,
            "is_active": 1
        }

    finally:
        db.close()


@app.post("/cards/select")
def select_card(data: SelectCardRequest):

    db = SessionLocal()

    try:

        selected = db.query(Card).filter(
            Card.id == data.card_id,
            Card.user_id == data.user_id
        ).first()

        if not selected:
            raise HTTPException(
                404,
                "Card not found"
            )

        db.query(Card).filter(
            Card.user_id == data.user_id
        ).update({
            "is_active": 0
        })

        selected.is_active = 1

        db.commit()

        return {
            "success": True
        }

    finally:
        db.close()


@app.delete("/cards/{card_id}")
def delete_card(card_id: int):

    db = SessionLocal()

    try:

        card = db.query(Card).filter(
            Card.id == card_id
        ).first()

        if not card:
            raise HTTPException(
                404,
                "Card not found"
            )

        user_cards = db.query(Card).filter(
            Card.user_id == card.user_id
        ).all()

        if len(user_cards) <= 1:
            raise HTTPException(
                400,
                "Нельзя удалить последнюю карту"
            )

        was_active = card.is_active == 1

        user_id = card.user_id

        db.delete(card)

        db.commit()

        if was_active:

            new_card = db.query(Card).filter(
                Card.user_id == user_id
            ).order_by(
                Card.position,
                Card.id
            ).first()

            if new_card:
                new_card.is_active = 1
                db.commit()

        return {
            "success": True
        }

    finally:
        db.close()

@app.post("/payments/webhook")
async def alif_webhook(request: Request):

    data = await request.json()

    print("WEBHOOK:")
    print(data)

    return {
        "success": True
    }