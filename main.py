"""
PowerBank Rental Backend
FastAPI + SQLAlchemy + MQTT (Relink protocol)
"""

import os
import json
import time
import uuid
import threading
import logging
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
import requests as http_requests

from fastapi import FastAPI, HTTPException, WebSocket, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqladmin import Admin, ModelView

from alif import create_hold, capture_hold

# =========================
# 🪵 LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("app")
mqtt_log = logging.getLogger("mqtt")

# =========================
# ⚙️ CONFIG
# =========================

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

MQTT_HOST     = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT     = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER     = os.getenv("MQTT_USER", "")
MQTT_PASS     = os.getenv("MQTT_PASS", "")
SELF_URL      = os.getenv("SELF_URL", "http://localhost:8000")

HOLD_AMOUNT   = 20_000   # сум, холд при старте аренды
PRICE_1H      = 7        # сом за первый час
PRICE_DAY     = 14       # сом за сутки
DEBT_LIMIT    = 150      # максимальный долг
STATION_TIMEOUT = 120    # секунд до offline

# =========================
# 🗄 DATABASE
# =========================

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"sslmode": "require"}
)
SessionLocal = sessionmaker(bind=engine)
Base         = declarative_base()

# =========================
# 📦 MODELS
# =========================

class User(Base):
    __tablename__ = "users"

    id          = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True)
    phone       = Column(String, nullable=True)
    name        = Column(String, nullable=True)
    balance     = Column(Float, default=0)
    is_blocked  = Column(Integer, default=0)


class LoginToken(Base):
    __tablename__ = "login_tokens"

    token   = Column(String, primary_key=True)
    user_id = Column(Integer, nullable=True)


class Card(Base):
    __tablename__ = "cards"

    id        = Column(Integer, primary_key=True)
    user_id   = Column(Integer)
    brand     = Column(String)
    last4     = Column(String)
    is_active = Column(Integer, default=0)
    position  = Column(Integer, default=0)


class Station(Base):
    __tablename__ = "stations"

    id         = Column(Integer, primary_key=True)
    name       = Column(String)
    serial     = Column(String, unique=True)
    mqtt_topic = Column(String)
    powerbanks = Column(Integer, default=0)
    slots      = Column(Integer, default=8)
    online     = Column(Integer, default=0)
    last_seen  = Column(DateTime)
    lat        = Column(Float)
    lng        = Column(Float)
    address    = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class StationSlot(Base):
    __tablename__ = "station_slots"

    id               = Column(Integer, primary_key=True)
    station_id       = Column(Integer)
    slot_number      = Column(Integer)
    powerbank_serial = Column(String)
    status           = Column(String)   # occupied | empty | reserved
    created_at       = Column(DateTime, default=datetime.utcnow)


class StationCommand(Base):
    __tablename__ = "station_commands"

    id         = Column(Integer, primary_key=True)
    station_id = Column(Integer)
    command_id = Column(String, unique=True)
    command    = Column(String)
    status     = Column(String)   # pending | sent | executed | failed
    payload    = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class StationLog(Base):
    __tablename__ = "station_logs"

    id         = Column(Integer, primary_key=True)
    station_id = Column(Integer)
    event      = Column(String)
    payload    = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class Rental(Base):
    __tablename__ = "rentals"

    id              = Column(Integer, primary_key=True)
    user_id         = Column(Integer)
    station_id      = Column(Integer)
    slot_number     = Column(Integer)
    status          = Column(String)          # pending | active | completed | failed
    cost            = Column(Integer, default=0)
    charged_amount  = Column(Float, default=0)
    payment_status  = Column(String, default="none")  # none | hold | paid | failed
    unlock_deadline = Column(DateTime)
    hold_id         = Column(String)
    hold_amount     = Column(Integer, default=0)
    start_time      = Column(DateTime, default=datetime.utcnow)
    end_time        = Column(DateTime)


class Payment(Base):
    __tablename__ = "payments"

    id             = Column(Integer, primary_key=True)
    rental_id      = Column(Integer)
    amount         = Column(Integer)
    provider       = Column(String)
    transaction_id = Column(String)
    status         = Column(String)   # hold | paid | failed | pending
    created_at     = Column(DateTime, default=datetime.utcnow)


# =========================
# 🛠 ADMIN VIEWS
# =========================

class UserAdmin(ModelView, model=User):
    column_list = [User.id, User.telegram_id, User.phone, User.name, User.is_blocked]

class CardAdmin(ModelView, model=Card):
    column_list = [Card.id, Card.user_id, Card.brand, Card.last4, Card.is_active]

class StationAdmin(ModelView, model=Station):
    column_list = [Station.id, Station.name, Station.serial, Station.powerbanks, Station.online]

class StationSlotAdmin(ModelView, model=StationSlot):
    column_list = [StationSlot.id, StationSlot.station_id, StationSlot.slot_number, StationSlot.status, StationSlot.powerbank_serial]

class StationCommandAdmin(ModelView, model=StationCommand):
    column_list = [StationCommand.id, StationCommand.station_id, StationCommand.command, StationCommand.status, StationCommand.created_at]

class StationLogAdmin(ModelView, model=StationLog):
    column_list = [StationLog.id, StationLog.station_id, StationLog.event, StationLog.created_at]

class RentalAdmin(ModelView, model=Rental):
    column_list = [Rental.id, Rental.user_id, Rental.status, Rental.cost, Rental.payment_status]

class PaymentAdmin(ModelView, model=Payment):
    column_list = [Payment.id, Payment.rental_id, Payment.amount, Payment.provider, Payment.transaction_id, Payment.status, Payment.created_at]

# =========================
# 📨 REQUEST SCHEMAS
# =========================

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

class SelectCardRequest(BaseModel):
    user_id: int
    card_id: int

class PaymentRequest(BaseModel):
    rental_id: int

class ConfirmPaymentRequest(BaseModel):
    payment_id: int

class StationEventRequest(BaseModel):
    serial: str
    event: str
    slot: int | None = None
    command_id: str | None = None
    powerbank_serial: str | None = None

# =========================
# 🚀 APP
# =========================

app = FastAPI(title="PowerBank Rental API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

admin = Admin(app=app, engine=engine)
admin.add_view(UserAdmin)
admin.add_view(CardAdmin)
admin.add_view(StationAdmin)
admin.add_view(StationSlotAdmin)
admin.add_view(StationCommandAdmin)
admin.add_view(StationLogAdmin)
admin.add_view(RentalAdmin)
admin.add_view(PaymentAdmin)

# =========================
# 💾 DB INIT
# =========================

Base.metadata.create_all(bind=engine)

# Миграции — добавляем колонки если нет (безопасно)
_migrations = [
    "ALTER TABLE users ADD COLUMN balance FLOAT DEFAULT 0",
    "ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0",
    "ALTER TABLE rentals ADD COLUMN hold_amount FLOAT DEFAULT 0",
    "ALTER TABLE rentals ADD COLUMN hold_id VARCHAR",
    "ALTER TABLE rentals ADD COLUMN charged_amount FLOAT DEFAULT 0",
]
with engine.begin() as conn:
    for q in _migrations:
        try:
            conn.execute(text(q))
        except Exception:
            pass

# =========================
# 🌐 GLOBAL STATE
# =========================

connections: dict[int, list[WebSocket]] = {}  # user_id -> [WebSocket]
otp_codes:   dict[str, str]             = {}  # phone -> code
_seq_counter = 0
_seq_lock    = threading.Lock()

def next_seq() -> int:
    global _seq_counter
    with _seq_lock:
        _seq_counter += 1
        return _seq_counter

# =========================
# 🛠 HELPERS
# =========================

def get_db():
    return SessionLocal()

def get_user_by_token(token: str | None) -> User | None:
    if not token:
        return None
    db = get_db()
    try:
        lt = db.query(LoginToken).filter(LoginToken.token == token).first()
        if not lt:
            return None
        return db.query(User).filter(User.id == lt.user_id).first()
    finally:
        db.close()

def require_user(token: str | None) -> User:
    user = get_user_by_token(token)
    if not user:
        raise HTTPException(401, "Unauthorized")
    if user.is_blocked:
        raise HTTPException(403, "Аккаунт временно заблокирован")
    return user

def calc_cost(start_time: datetime, end_time: datetime) -> int:
    hours = (end_time - start_time).total_seconds() / 3600
    if hours <= 1:
        return PRICE_1H
    if hours <= 24:
        return PRICE_DAY
    extra_days = int((hours - 24) / 24) + 1
    return PRICE_DAY + extra_days * PRICE_DAY

def station_is_online(station: Station) -> bool:
    return station.online == 1

def add_station_log(db, station_id: int, event: str, payload: dict):
    db.add(StationLog(
        station_id=station_id,
        event=event,
        payload=json.dumps(payload, ensure_ascii=False),
    ))

# =========================
# 📡 MQTT CLIENT
# =========================

_mqtt_client: mqtt.Client | None = None


def _mqtt_on_connect(client, userdata, flags, rc):
    if rc == 0:
        mqtt_log.info("Connected to broker")
        client.subscribe("cabinet/+/reply/#", qos=1)
        client.subscribe("cabinet/+/report/#", qos=1)
    else:
        mqtt_log.error(f"Connection failed, rc={rc}")


def _mqtt_on_disconnect(client, userdata, rc):
    mqtt_log.warning(f"Disconnected rc={rc}")


def _mqtt_on_message(client, userdata, msg):
    topic   = msg.topic
    payload = msg.payload

    mqtt_log.info(f"<< {topic}: {payload[:200]}")

    parts = topic.split("/")
    if len(parts) != 4:
        return

    _, serial, direction, cmd_type = parts

    try:
        data = json.loads(payload)
    except Exception:
        data = {}

    if direction == "reply":
        _handle_reply(serial, cmd_type, data)
    elif direction == "report":
        _handle_report(serial, cmd_type, data)


def _handle_reply(serial: str, cmd_type: str, data: dict):
    """Обработка ответов от станции."""

    # cmd/15 — Push Power Bank (выдача)
    if cmd_type == "15":
        result     = data.get("rl_result", 0)
        slot       = data.get("rl_slot")
        command_id = data.get("command_id")
        pb_serial  = data.get("rl_pbid")

        event = "dispensed" if result == 1 else "dispense_failed"
        _notify_self(serial, event, slot=slot, command_id=command_id, powerbank_serial=str(pb_serial) if pb_serial else None)

    # cmd/25 — Reset
    elif cmd_type == "25":
        result = data.get("rl_result", 0)
        mqtt_log.info(f"Station {serial} reset {'ok' if result else 'failed'}")

    # cmd/13 — Inventory query reply
    elif cmd_type == "13":
        mqtt_log.info(f"Station {serial} inventory: {data}")


def _handle_report(serial: str, cmd_type: str, data: dict):
    """Обработка репортов от станции (события без запроса)."""

    # report/10 — Login (станция появилась в сети)
    if cmd_type == "10":
        mqtt_log.info(f"Station {serial} logged in")
        _notify_self(serial, "heartbeat")

    # report/22 — Power Bank Returned
    elif cmd_type == "22":
        slot      = data.get("slot")
        pb_serial = data.get("rl_pdid")
        mqtt_log.info(f"Station {serial} powerbank returned to slot {slot}")
        _notify_self(serial, "returned", slot=slot, powerbank_serial=str(pb_serial) if pb_serial else None)

        # Подтверждаем станции что возврат принят
        _mqtt_publish(
            f"cabinet/{serial}/cmd/22",
            {"rl_slot": slot, "rl_result": 1, "rl_seq": next_seq()}
        )


def _notify_self(serial: str, event: str, slot: int | None = None,
                 command_id: str | None = None, powerbank_serial: str | None = None):
    """Дёргает /station/event внутри самого сервиса."""
    payload = {"serial": serial, "event": event}
    if slot is not None:
        payload["slot"] = slot
    if command_id:
        payload["command_id"] = command_id
    if powerbank_serial:
        payload["powerbank_serial"] = powerbank_serial

    try:
        http_requests.post(f"{SELF_URL}/station/event", json=payload, timeout=5)
    except Exception as e:
        mqtt_log.error(f"notify_self error: {e}")


def _mqtt_publish(topic: str, data: dict):
    if not _mqtt_client or not _mqtt_client.is_connected():
        mqtt_log.warning(f"MQTT not connected, cannot publish to {topic}")
        return False
    payload = json.dumps(data)
    result  = _mqtt_client.publish(topic, payload, qos=1)
    mqtt_log.info(f">> {topic}: {payload}")
    return result.rc == mqtt.MQTT_ERR_SUCCESS


def mqtt_unlock_slot(serial: str, slot: int, command_id: str) -> bool:
    """Отправляет команду выдачи павербанка (cmd/15)."""
    return _mqtt_publish(
        f"cabinet/{serial}/cmd/15",
        {"rl_slot": slot, "rl_seq": next_seq(), "command_id": command_id}
    )


def mqtt_query_inventory(serial: str):
    """Запрашивает состояние слотов (cmd/13)."""
    _mqtt_publish(f"cabinet/{serial}/cmd/13", {"rl_seq": next_seq()})


def mqtt_reset_station(serial: str):
    """Перезагружает станцию (cmd/25)."""
    _mqtt_publish(f"cabinet/{serial}/cmd/25", {"rl_seq": next_seq()})


def _start_mqtt():
    global _mqtt_client

    _mqtt_client = mqtt.Client()
    if MQTT_USER:
        _mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)

    _mqtt_client.on_connect    = _mqtt_on_connect
    _mqtt_client.on_disconnect = _mqtt_on_disconnect
    _mqtt_client.on_message    = _mqtt_on_message

    def _run():
        while True:
            try:
                mqtt_log.info(f"Connecting to {MQTT_HOST}:{MQTT_PORT}...")
                _mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
                _mqtt_client.loop_forever()
            except Exception as e:
                mqtt_log.error(f"MQTT error: {e}, retry in 5s")
                time.sleep(5)

    threading.Thread(target=_run, daemon=True, name="mqtt").start()

# =========================
# ⏱ BACKGROUND TASKS
# =========================

def _station_monitor():
    """Помечает станции offline если нет heartbeat дольше STATION_TIMEOUT секунд."""
    while True:
        db = get_db()
        try:
            stations = db.query(Station).all()
            for s in stations:
                if not s.last_seen:
                    continue
                if (datetime.utcnow() - s.last_seen).total_seconds() > STATION_TIMEOUT:
                    if s.online:
                        s.online = 0
                        log.warning(f"Station {s.serial} went offline")
            db.commit()
        finally:
            db.close()
        time.sleep(30)


def _unlock_timeout_monitor():
    """Если павербанк не забрали за 2 минуты — отменяем аренду."""
    while True:
        db = get_db()
        try:
            expired = db.query(Rental).filter(
                Rental.status == "pending",
                Rental.unlock_deadline < datetime.utcnow(),
            ).all()

            for rental in expired:
                log.warning(f"Rental {rental.id} expired without pickup")
                rental.status = "failed"

                # Освобождаем слот
                slot = db.query(StationSlot).filter(
                    StationSlot.station_id == rental.station_id,
                    StationSlot.slot_number == rental.slot_number,
                ).first()
                if slot:
                    slot.status = "occupied"

                # Возвращаем счётчик
                station = db.query(Station).filter(Station.id == rental.station_id).first()
                if station:
                    station.powerbanks += 1

            db.commit()
        finally:
            db.close()
        time.sleep(15)

# =========================
# 🚀 STARTUP
# =========================

@app.on_event("startup")
async def on_startup():
    _start_mqtt()
    threading.Thread(target=_station_monitor,       daemon=True, name="station_monitor").start()
    threading.Thread(target=_unlock_timeout_monitor, daemon=True, name="unlock_monitor").start()
    log.info("App started")

# =========================
# 🔐 AUTH
# =========================

@app.post("/auth/send-code")
def send_code(data: SendCodeRequest):
    # TODO: заменить на реальную SMS-отправку (например, Eskiz, SMS.ru и т.д.)
    code = "1111"
    otp_codes[data.phone] = code
    log.info(f"OTP for {data.phone}: {code}")
    return {"success": True}


@app.post("/auth/verify-code")
def verify_code(data: VerifyCodeRequest):
    saved = otp_codes.get(data.phone)
    if not saved:
        raise HTTPException(400, "Код не найден или истёк")
    if saved != data.code:
        raise HTTPException(400, "Неверный код")

    db = get_db()
    try:
        user = db.query(User).filter(User.phone == data.phone).first()
        if not user:
            user = User(
                telegram_id=str(uuid.uuid4()),
                phone=data.phone,
                name="User",
            )
            db.add(user)
            db.commit()
            db.refresh(user)

        token = str(uuid.uuid4())
        db.add(LoginToken(token=token, user_id=user.id))
        db.commit()

        otp_codes.pop(data.phone, None)

        return {"token": token, "user": {"id": user.id, "phone": user.phone}}
    finally:
        db.close()

# =========================
# 📍 STATIONS
# =========================

@app.get("/stations")
def get_stations():
    db = get_db()
    try:
        stations = db.query(Station).all()
        return [
            {
                "id": s.id,
                "name": s.name,
                "powerbanks": s.powerbanks,
                "empty_slots": s.slots - s.powerbanks,
                "online": s.online,
                "address": s.address,
                "lat": s.lat,
                "lng": s.lng,
            }
            for s in stations
        ]
    finally:
        db.close()


@app.get("/stations/{station_id}")
def get_station(station_id: int):
    db = get_db()
    try:
        s = db.query(Station).filter(Station.id == station_id).first()
        if not s:
            raise HTTPException(404, "Station not found")
        slots = db.query(StationSlot).filter(StationSlot.station_id == station_id).all()
        return {
            "id": s.id,
            "name": s.name,
            "serial": s.serial,
            "powerbanks": s.powerbanks,
            "slots": s.slots,
            "online": s.online,
            "address": s.address,
            "lat": s.lat,
            "lng": s.lng,
            "slot_details": [
                {"slot": sl.slot_number, "status": sl.status}
                for sl in slots
            ],
        }
    finally:
        db.close()


@app.post("/station/event")
async def station_event(data: StationEventRequest):
    """
    Внутренний эндпоинт — вызывается MQTT-обработчиком.
    Также может быть вызван напрямую для тестирования.
    """
    db = get_db()
    try:
        station = db.query(Station).filter(Station.serial == data.serial).first()
        if not station:
            raise HTTPException(404, "Station not found")

        add_station_log(db, station.id, data.event, data.dict())

        # --- heartbeat ---
        if data.event == "heartbeat":
            station.online    = 1
            station.last_seen = datetime.utcnow()

        # --- павербанк выдан ---
        elif data.event == "dispensed":
            slot = db.query(StationSlot).filter(
                StationSlot.station_id == station.id,
                StationSlot.slot_number == data.slot,
            ).first()
            if slot:
                slot.status = "empty"
                if data.powerbank_serial:
                    slot.powerbank_serial = data.powerbank_serial

            # Ищем pending-аренду именно по этому слоту
            rental = db.query(Rental).filter(
                Rental.station_id == station.id,
                Rental.slot_number == data.slot,
                Rental.status == "pending",
            ).order_by(Rental.id.desc()).first()

            if rental:
                rental.status = "active"

            # Помечаем команду как выполненную
            if data.command_id:
                cmd = db.query(StationCommand).filter(
                    StationCommand.command_id == data.command_id
                ).first()
                if cmd:
                    cmd.status = "executed"

        # --- выдача не удалась ---
        elif data.event == "dispense_failed":
            # Откатываем аренду
            rental = db.query(Rental).filter(
                Rental.station_id == station.id,
                Rental.slot_number == data.slot,
                Rental.status == "pending",
            ).order_by(Rental.id.desc()).first()

            if rental:
                rental.status = "failed"
                station.powerbanks += 1

                # Освобождаем слот
                slot = db.query(StationSlot).filter(
                    StationSlot.station_id == station.id,
                    StationSlot.slot_number == data.slot,
                ).first()
                if slot:
                    slot.status = "occupied"

            if data.command_id:
                cmd = db.query(StationCommand).filter(
                    StationCommand.command_id == data.command_id
                ).first()
                if cmd:
                    cmd.status = "failed"

        # --- павербанк возвращён ---
        elif data.event == "returned":
            slot = db.query(StationSlot).filter(
                StationSlot.station_id == station.id,
                StationSlot.slot_number == data.slot,
            ).first()
            if slot:
                slot.status = "occupied"
                slot.powerbank_serial = data.powerbank_serial or None

            # Ищем активную аренду по слоту
            rental = db.query(Rental).filter(
                Rental.station_id == station.id,
                Rental.slot_number == data.slot,
                Rental.status == "active",
            ).order_by(Rental.id.desc()).first()

            if rental:
                end_time   = datetime.utcnow()
                cost       = calc_cost(rental.start_time, end_time)
                rental.cost     = cost
                rental.end_time = end_time
                rental.status   = "completed"

                # Capture hold через Alif
                success = capture_hold(rental.hold_id, cost)
                rental.payment_status = "paid" if success else "failed"
                rental.charged_amount = cost if success else 0

                station.powerbanks += 1

                # Обновляем запись Payment
                payment = db.query(Payment).filter(
                    Payment.transaction_id == rental.hold_id
                ).first()
                if payment:
                    payment.status = rental.payment_status
                    payment.amount = cost

                # Уведомляем пользователя через WebSocket
                for ws in connections.get(rental.user_id, []):
                    try:
                        await ws.send_json({
                            "type": "rental_finished",
                            "cost": cost,
                            "payment_status": rental.payment_status,
                        })
                    except Exception:
                        pass

        db.commit()
        return {"success": True}

    finally:
        db.close()


@app.post("/station/reset/{serial}")
def reset_station_endpoint(serial: str, authorization: str = Header(None)):
    """Перезагрузка станции (для операторов)."""
    require_user(authorization)
    mqtt_reset_station(serial)
    return {"success": True}


@app.post("/station/inventory/{serial}")
def query_station_inventory(serial: str, authorization: str = Header(None)):
    """Запросить инвентарь станции."""
    require_user(authorization)
    mqtt_query_inventory(serial)
    return {"success": True}

# =========================
# 🔋 RENTAL
# =========================

@app.post("/rent")
def rent(data: RentRequest, authorization: str = Header(None)):
    user = require_user(authorization)
    db   = get_db()

    try:
        station = db.query(Station).filter(Station.id == data.station_id).first()
        if not station:
            raise HTTPException(404, "Station not found")

        if not station_is_online(station):
            raise HTTPException(400, "Станция оффлайн")

        if station.powerbanks <= 0:
            raise HTTPException(400, "Нет доступных павербанков")

        # Проверяем активную аренду
        active = db.query(Rental).filter(
            Rental.user_id == user.id,
            Rental.status.in_(["active", "pending"]),
        ).first()
        if active:
            raise HTTPException(400, "У вас уже есть активная аренда")

        # Проверяем долг
        unpaid    = db.query(Rental).filter(
            Rental.user_id == user.id,
            Rental.payment_status != "paid",
            Rental.status == "completed",
        ).all()
        debt_sum  = sum(r.cost or 0 for r in unpaid)
        if debt_sum > DEBT_LIMIT:
            raise HTTPException(400, f"Превышен лимит долга ({debt_sum} сом)")

        # Выбираем свободный слот
        slot = db.query(StationSlot).filter(
            StationSlot.station_id == station.id,
            StationSlot.status == "occupied",
        ).first()
        if not slot:
            raise HTTPException(400, "Нет доступных слотов")

        # Hold через Alif
        hold = create_hold(user.id, HOLD_AMOUNT)
        if not hold.get("success"):
            raise HTTPException(400, "Ошибка холда оплаты")

        command_id = str(uuid.uuid4())

        # Создаём команду в БД
        cmd = StationCommand(
            station_id=station.id,
            command_id=command_id,
            command="unlock",
            status="pending",
            payload=json.dumps({"slot": slot.slot_number, "serial": station.serial}),
        )
        db.add(cmd)

        # Резервируем слот
        slot.status = "reserved"

        # Создаём аренду
        rental = Rental(
            user_id=user.id,
            station_id=data.station_id,
            slot_number=slot.slot_number,
            status="pending",
            start_time=datetime.utcnow(),
            unlock_deadline=datetime.utcnow() + timedelta(minutes=2),
            hold_id=hold["hold_id"],
            hold_amount=HOLD_AMOUNT,
        )
        station.powerbanks -= 1
        db.add(rental)
        db.commit()
        db.refresh(rental)

        # Сохраняем платёж
        payment = Payment(
            rental_id=rental.id,
            amount=HOLD_AMOUNT,
            provider="alif",
            transaction_id=hold["hold_id"],
            status="hold",
        )
        db.add(payment)
        db.commit()

        # Отправляем MQTT команду
        sent = mqtt_unlock_slot(station.serial, slot.slot_number, command_id)
        cmd.status = "sent" if sent else "failed"
        if not sent:
            log.warning(f"MQTT not connected, unlock command queued for station {station.serial}")
        db.commit()

        return {"id": rental.id, "slot": slot.slot_number}

    finally:
        db.close()


@app.get("/rentals/{user_id}")
def get_rentals(user_id: int, authorization: str = Header(None)):
    require_user(authorization)
    db = get_db()
    try:
        rentals = db.query(Rental).filter(
            Rental.user_id == user_id
        ).order_by(Rental.id.desc()).all()

        return [
            {
                "id": r.id,
                "status": r.status,
                "cost": r.cost,
                "payment_status": r.payment_status,
                "station_id": r.station_id,
                "slot_number": r.slot_number,
                "start_time": r.start_time,
                "end_time": r.end_time,
            }
            for r in rentals
        ]
    finally:
        db.close()

# =========================
# 💳 CARDS
# =========================

@app.get("/cards/{user_id}")
def get_cards(user_id: int):
    db = get_db()
    try:
        cards = db.query(Card).filter(
            Card.user_id == user_id
        ).order_by(Card.position, Card.id).all()
        return [
            {"id": c.id, "brand": c.brand, "last4": c.last4, "is_active": c.is_active}
            for c in cards
        ]
    finally:
        db.close()


@app.post("/cards/add")
def add_card(data: AddCardRequest, authorization: str = Header(None)):
    user = require_user(authorization)
    db   = get_db()
    try:
        db.query(Card).filter(Card.user_id == user.id).update({"is_active": 0})

        last = db.query(Card).filter(
            Card.user_id == user.id
        ).order_by(Card.position.desc()).first()

        card = Card(
            user_id=user.id,
            brand="VISA",
            last4=data.number[-4:],
            is_active=1,
            position=(last.position or 0) + 1 if last else 1,
        )
        db.add(card)
        db.commit()
        db.refresh(card)

        return {"id": card.id, "brand": card.brand, "last4": card.last4, "is_active": 1}
    finally:
        db.close()


@app.post("/cards/select")
def select_card(data: SelectCardRequest):
    db = get_db()
    try:
        selected = db.query(Card).filter(
            Card.id == data.card_id,
            Card.user_id == data.user_id,
        ).first()
        if not selected:
            raise HTTPException(404, "Card not found")

        db.query(Card).filter(Card.user_id == data.user_id).update({"is_active": 0})
        selected.is_active = 1
        db.commit()
        return {"success": True}
    finally:
        db.close()


@app.delete("/cards/{card_id}")
def delete_card(card_id: int):
    db = get_db()
    try:
        card = db.query(Card).filter(Card.id == card_id).first()
        if not card:
            raise HTTPException(404, "Card not found")

        total = db.query(Card).filter(Card.user_id == card.user_id).count()
        if total <= 1:
            raise HTTPException(400, "Нельзя удалить последнюю карту")

        was_active = card.is_active == 1
        user_id    = card.user_id
        db.delete(card)
        db.commit()

        if was_active:
            new = db.query(Card).filter(
                Card.user_id == user_id
            ).order_by(Card.position, Card.id).first()
            if new:
                new.is_active = 1
                db.commit()

        return {"success": True}
    finally:
        db.close()

# =========================
# 💰 PAYMENTS
# =========================

@app.post("/payment/create")
def create_payment(data: PaymentRequest):
    db = get_db()
    try:
        rental = db.query(Rental).filter(Rental.id == data.rental_id).first()
        if not rental:
            raise HTTPException(404, "Rental not found")
        if rental.payment_status == "paid":
            raise HTTPException(400, "Already paid")

        payment = Payment(
            rental_id=rental.id,
            amount=rental.cost,
            status="pending",
        )
        db.add(payment)
        db.commit()
        db.refresh(payment)

        return {"payment_id": payment.id, "amount": payment.amount}
    finally:
        db.close()


@app.get("/payment/status/{rental_id}")
def payment_status(rental_id: int):
    db = get_db()
    try:
        rental = db.query(Rental).filter(Rental.id == rental_id).first()
        if not rental:
            raise HTTPException(404, "Not found")
        return {"status": rental.payment_status, "cost": rental.cost}
    finally:
        db.close()


@app.post("/payment/confirm")
def confirm_payment(data: ConfirmPaymentRequest):
    db = get_db()
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


@app.post("/payment/webhook")
async def payment_webhook(request: Request):
    """Webhook от Alif при изменении статуса оплаты."""
    data     = await request.json()
    log.info(f"Payment webhook: {data}")

    order_id = data.get("order_id")
    status   = data.get("status")

    if not order_id:
        return {"ok": False}

    db = get_db()
    try:
        payment = db.query(Payment).filter(Payment.id == int(order_id)).first()
        if not payment:
            return {"ok": False}

        if status == "paid":
            payment.status = "paid"
            rental = db.query(Rental).filter(Rental.id == payment.rental_id).first()
            if rental:
                rental.payment_status = "paid"
                for ws in connections.get(rental.user_id, []):
                    try:
                        await ws.send_json({"type": "payment_success"})
                    except Exception:
                        pass

        db.commit()
        return {"ok": True}
    finally:
        db.close()


# Алиас — на случай если Alif шлёт на /payments/webhook (с 's')
@app.post("/payments/webhook")
async def payments_webhook_alias(request: Request):
    return await payment_webhook(request)

# =========================
# 🔌 WEBSOCKET
# =========================

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(ws: WebSocket, user_id: int):
    await ws.accept()
    connections.setdefault(user_id, []).append(ws)
    log.info(f"WS connected: user {user_id}")
    try:
        while True:
            await ws.receive_text()
    except Exception:
        pass
    finally:
        try:
            connections[user_id].remove(ws)
        except (KeyError, ValueError):
            pass
        log.info(f"WS disconnected: user {user_id}")

# =========================
# ❤️ HEALTH CHECK
# =========================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "mqtt_connected": _mqtt_client is not None and _mqtt_client.is_connected(),
        "time": datetime.utcnow().isoformat(),
    }
