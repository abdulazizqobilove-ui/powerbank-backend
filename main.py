"""
PowerBank Rental Backend
FastAPI + SQLAlchemy + MQTT (Relink CS-S08 protocol v1.2)

MQTT Topics (Relink protocol):
  Device publishes: cabinet/{serial}/reply/{type}
  Device reports:   cabinet/{serial}/report/{type}
  Server sends:     cabinet/{serial}/cmd/{type}

Commands:
  cmd/11  — Force push (принудительная выдача, для тестов)
  cmd/13  — Query inventory (запрос инвентаря)
  cmd/15  — Push power bank (выдача)
  cmd/16  — Set APN
  cmd/17  — Query APN
  cmd/18  — Query server info
  cmd/20  — Query SIM ICCID
  cmd/21  — Set voice
  cmd/22  — Reply to return (подтверждение возврата)
  cmd/24  — Query network info
  cmd/25  — Reset cabinet

Reports:
  report/10 — Cabinet login (станция онлайн)
  report/22 — Power bank returned (возврат)
"""

import os
import json
import time
import uuid
import threading
import logging
from datetime import datetime, timedelta
from typing import Optional

import paho.mqtt.client as mqtt
import requests as http_requests

from fastapi import FastAPI, HTTPException, WebSocket, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqladmin import Admin, ModelView
from twilio.rest import Client

# =========================
# 🪵 LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log      = logging.getLogger("app")
mqtt_log = logging.getLogger("mqtt")

# =========================
# ⚙️ CONFIG
# =========================

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./powerbank.db")

# Render/Heroku иногда дают postgres:// — фиксим
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# SSL для PostgreSQL
if DATABASE_URL.startswith("postgresql://") and "sslmode=" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

MQTT_HOST       = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT       = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER       = os.getenv("MQTT_USER", "")
MQTT_PASS       = os.getenv("MQTT_PASS", "")
SELF_URL        = os.getenv("SELF_URL", "http://localhost:8000")

HOLD_AMOUNT     = int(os.getenv("HOLD_AMOUNT", 20000))   # сумонӣ, холд при старте
PRICE_1H        = int(os.getenv("PRICE_1H", 7))           # за первый час
PRICE_DAY       = int(os.getenv("PRICE_DAY", 14))         # за сутки
DEBT_LIMIT      = int(os.getenv("DEBT_LIMIT", 150))       # лимит долга
STATION_TIMEOUT = int(os.getenv("STATION_TIMEOUT", 120))  # секунд до offline

# =========================
# 🗄 DATABASE
# =========================

engine       = create_engine(DATABASE_URL, pool_pre_ping=True)
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
    serial     = Column(String, unique=True)   # device_id станции (из доков Relink)
    mqtt_topic = Column(String)
    powerbanks = Column(Integer, default=0)    # сколько павербанков сейчас в станции
    slots      = Column(Integer, default=8)    # CS-S08 = 8 слотов
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
    status           = Column(String)    # occupied | empty | reserved
    charge_level     = Column(Integer, default=0)   # 0-5 (из rl_qoe)
    created_at       = Column(DateTime, default=datetime.utcnow)


class StationCommand(Base):
    __tablename__ = "station_commands"

    id         = Column(Integer, primary_key=True)
    station_id = Column(Integer)
    command_id = Column(String, unique=True)
    command    = Column(String)
    status     = Column(String)    # pending | sent | executed | failed
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
    status          = Column(String)           # pending | active | completed | failed
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
    status         = Column(String)    # hold | paid | failed | pending
    created_at     = Column(DateTime, default=datetime.utcnow)


# =========================
# 🛠 ADMIN VIEWS
# =========================

class UserAdmin(ModelView, model=User):
    column_list = [User.id, User.telegram_id, User.phone, User.name, User.balance, User.is_blocked]

class CardAdmin(ModelView, model=Card):
    column_list = [Card.id, Card.user_id, Card.brand, Card.last4, Card.is_active]

class StationAdmin(ModelView, model=Station):
    column_list = [Station.id, Station.name, Station.serial, Station.powerbanks, Station.slots, Station.online, Station.address]

class StationSlotAdmin(ModelView, model=StationSlot):
    column_list = [StationSlot.id, StationSlot.station_id, StationSlot.slot_number, StationSlot.status, StationSlot.powerbank_serial, StationSlot.charge_level]

class StationCommandAdmin(ModelView, model=StationCommand):
    column_list = [StationCommand.id, StationCommand.station_id, StationCommand.command, StationCommand.status, StationCommand.created_at]

class StationLogAdmin(ModelView, model=StationLog):
    column_list = [StationLog.id, StationLog.station_id, StationLog.event, StationLog.created_at]

class RentalAdmin(ModelView, model=Rental):
    column_list = [Rental.id, Rental.user_id, Rental.status, Rental.cost, Rental.payment_status, Rental.start_time, Rental.end_time]

class PaymentAdmin(ModelView, model=Payment):
    column_list = [Payment.id, Payment.rental_id, Payment.amount, Payment.provider, Payment.status, Payment.created_at]

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
    slot: Optional[int] = None
    command_id: Optional[str] = None
    powerbank_serial: Optional[str] = None

# =========================
# 🚀 APP
# =========================

app = FastAPI(title="PowerBank Rental API — Relink CS-S08")

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
# 💾 DB INIT + MIGRATIONS
# =========================

Base.metadata.create_all(bind=engine)

_migrations = [
    "ALTER TABLE users ADD COLUMN balance FLOAT DEFAULT 0",
    "ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0",
    "ALTER TABLE rentals ADD COLUMN hold_amount FLOAT DEFAULT 0",
    "ALTER TABLE rentals ADD COLUMN hold_id VARCHAR",
    "ALTER TABLE rentals ADD COLUMN charged_amount FLOAT DEFAULT 0",
    "ALTER TABLE rentals ADD COLUMN slot_number INTEGER",
    "ALTER TABLE station_slots ADD COLUMN charge_level INTEGER DEFAULT 0",
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

connections: dict  = {}   # user_id -> [WebSocket]
otp_codes:   dict  = {}   # phone -> code
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

def get_user_by_token(token: Optional[str]) -> Optional[User]:
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

def require_user(token: Optional[str]) -> User:
    user = get_user_by_token(token)
    if not user:
        raise HTTPException(401, "Unauthorized")
    if user.is_blocked:
        raise HTTPException(403, "Аккаунт заблокирован")
    return user

def calc_cost(start_time: datetime, end_time: datetime) -> int:
    hours = (end_time - start_time).total_seconds() / 3600
    if hours <= 1:
        return PRICE_1H
    if hours <= 24:
        return PRICE_DAY
    extra_days = int((hours - 24) / 24) + 1
    return PRICE_DAY + extra_days * PRICE_DAY

def add_station_log(db, station_id: int, event: str, payload: dict):
    db.add(StationLog(
        station_id=station_id,
        event=event,
        payload=json.dumps(payload, ensure_ascii=False),
    ))

def qoe_to_percent(qoe: int) -> str:
    """Конвертирует rl_qoe (0-5) в читаемый процент."""
    return {0: "0-20%", 1: "20-40%", 2: "40-60%", 3: "60-80%", 4: "80-100%", 5: "100%"}.get(qoe, "?")

# =========================
# 📡 MQTT CLIENT
# =========================
# Протокол Relink v1.2:
#   Станция → Сервер:  cabinet/{serial}/reply/{type}   (ответы на команды)
#                      cabinet/{serial}/report/{type}  (автоматические репорты)
#   Сервер → Станция:  cabinet/{serial}/cmd/{type}     (команды)
#   QoS = 1, Retain = False, Heartbeat = 60 сек
# =========================

_mqtt_client: Optional[mqtt.Client] = None


def _mqtt_on_connect(client, userdata, flags, rc):
    if rc == 0:
        mqtt_log.info("✅ Connected to MQTT broker")
        # Подписываемся на ВСЕ станции сразу (wildcard +)
        client.subscribe("cabinet/+/reply/#", qos=1)
        client.subscribe("cabinet/+/report/#", qos=1)
        mqtt_log.info("📡 Subscribed to cabinet/+/reply/# and cabinet/+/report/#")
    else:
        errors = {
            1: "Wrong protocol version",
            2: "Client ID rejected",
            3: "Broker unavailable",
            4: "Bad username/password",
            5: "Not authorized",
        }
        mqtt_log.error(f"❌ MQTT connect failed: {errors.get(rc, f'rc={rc}')}")


def _mqtt_on_disconnect(client, userdata, rc):
    if rc == 0:
        mqtt_log.info("MQTT disconnected cleanly")
    else:
        mqtt_log.warning(f"MQTT unexpected disconnect rc={rc}, will reconnect...")


def _mqtt_on_message(client, userdata, msg):
    topic   = msg.topic
    raw     = msg.payload

    mqtt_log.info(f"<< {topic}: {raw[:300]}")

    # Ожидаем формат: cabinet/{serial}/{direction}/{cmd_type}
    parts = topic.split("/")
    if len(parts) != 4:
        mqtt_log.warning(f"Unexpected topic format: {topic}")
        return

    _, serial, direction, cmd_type = parts

    # Парсим JSON payload
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        mqtt_log.warning(f"Non-JSON payload on {topic}: {raw[:100]}")
        data = {}

    if direction == "reply":
        _handle_reply(serial, cmd_type, data)
    elif direction == "report":
        _handle_report(serial, cmd_type, data)
    else:
        mqtt_log.warning(f"Unknown direction '{direction}' in topic {topic}")


# ---------- REPLY HANDLERS ----------

def _handle_reply(serial: str, cmd_type: str, data: dict):
    """Обрабатывает ответы станции на наши команды (reply/*)."""

    # reply/15 — ответ на Push Power Bank (выдача)
    if cmd_type == "15":
        result     = data.get("rl_result", 0)
        slot       = data.get("rl_slot")
        pb_serial  = data.get("rl_pbid")
        error_code = data.get("rl_code", 0)
        qoe        = data.get("rl_qoe", 0)
        command_id = data.get("command_id")  # наш UUID, который мы отправили

        error_map = {
            0: "OK",
            1: "Не удалось прочитать ID павербанка",
            2: "Таймаут выдачи",
            3: "Уже выдаётся",
        }

        if result == 1:
            mqtt_log.info(
                f"Station {serial} dispensed slot={slot} "
                f"pb={pb_serial} charge={qoe_to_percent(qoe)}"
            )
            _notify_self(serial, "dispensed",
                         slot=slot,
                         command_id=command_id,
                         powerbank_serial=str(pb_serial) if pb_serial else None)
        else:
            mqtt_log.warning(
                f"Station {serial} dispense FAILED slot={slot} "
                f"error={error_map.get(error_code, error_code)}"
            )
            _notify_self(serial, "dispense_failed",
                         slot=slot,
                         command_id=command_id)

    # reply/11 — ответ на Force Push (принудительная выдача)
    elif cmd_type == "11":
        result = data.get("rl_result", 0)
        slot   = data.get("rl_slot")
        mqtt_log.info(f"Station {serial} force push slot={slot} result={'OK' if result else 'FAIL'}")

    # reply/13 — ответ на Query Inventory
    elif cmd_type == "13":
        num = data.get("rl_num", 0)
        mqtt_log.info(f"Station {serial} inventory: {num} powerbanks")

        # Обновляем БД по каждому слоту
        db = get_db()
        try:
            station = db.query(Station).filter(Station.serial == serial).first()
            if not station:
                return

            # Relink отдаёт список слотов
            slots_data = data.get("slots", [])
            occupied = 0
            for s in slots_data:
                slot_num  = s.get("rl_slot")
                qoe       = s.get("rl_qoe", 0)
                lock      = s.get("rl_lock", 0)   # 1 = занят
                id_ok     = s.get("rl_idok", 0)
                pb_serial = s.get("rl_pbid")

                # lock=1 и idok=1 → слот занят павербанком
                if lock == 1 and id_ok == 1:
                    status = "occupied"
                    occupied += 1
                else:
                    status = "empty"

                slot_row = db.query(StationSlot).filter(
                    StationSlot.station_id == station.id,
                    StationSlot.slot_number == slot_num,
                ).first()

                if slot_row:
                    slot_row.status           = status
                    slot_row.charge_level     = qoe
                    slot_row.powerbank_serial = str(pb_serial) if pb_serial else None
                else:
                    db.add(StationSlot(
                        station_id=station.id,
                        slot_number=slot_num,
                        status=status,
                        charge_level=qoe,
                        powerbank_serial=str(pb_serial) if pb_serial else None,
                    ))

            station.powerbanks = occupied
            add_station_log(db, station.id, "inventory_updated", data)
            db.commit()
            mqtt_log.info(f"Station {serial} inventory synced: {occupied} occupied")
        finally:
            db.close()

    # reply/25 — ответ на Reset
    elif cmd_type == "25":
        result = data.get("rl_result", 0)
        mqtt_log.info(f"Station {serial} reset {'OK' if result else 'FAIL'}")

    # reply/20 — SIM ICCID
    elif cmd_type == "20":
        iccid = data.get("rl_iccid", "")
        imei  = data.get("rl_imei", "")
        mqtt_log.info(f"Station {serial} SIM ICCID={iccid} IMEI={imei}")

    # reply/24 — Network info
    elif cmd_type == "24":
        conn_map = {0: "WiFi", 1: "2G", 2: "3G", 4: "4G"}
        conn     = conn_map.get(data.get("rl_conn", -1), "?")
        csq      = data.get("rl_csq", 0)
        mqtt_log.info(f"Station {serial} network: {conn} CSQ={csq}")

    else:
        mqtt_log.info(f"Station {serial} reply/{cmd_type}: {data}")


# ---------- REPORT HANDLERS ----------

def _handle_report(serial: str, cmd_type: str, data: dict):
    """Обрабатывает автоматические репорты от станции (report/*)."""

    # report/10 — Cabinet Login (станция вышла онлайн)
    if cmd_type == "10":
        count    = data.get("rl_count", 8)
        conn_map = {0: "WiFi", 1: "2G", 2: "3G", 4: "4G"}
        conn     = conn_map.get(data.get("rl_conn", -1), "?")
        iccid    = data.get("rl_iccid", "")
        sw_ver   = data.get("rl_commsoftver", "")
        mqtt_log.info(
            f"Station {serial} ONLINE — slots={count} "
            f"net={conn} ICCID={iccid} SW={sw_ver}"
        )

        # Обновляем онлайн-статус и создаём слоты если их нет
        db = get_db()
        try:
            station = db.query(Station).filter(Station.serial == serial).first()
            if station:
                station.online    = 1
                station.last_seen = datetime.utcnow()
                if count and station.slots != count:
                    station.slots = count

                # Создаём слоты если ещё не созданы
                existing = db.query(StationSlot).filter(
                    StationSlot.station_id == station.id
                ).count()
                if existing == 0:
                    for i in range(1, count + 1):
                        db.add(StationSlot(
                            station_id=station.id,
                            slot_number=i,
                            status="empty",
                        ))
                    mqtt_log.info(f"Created {count} slots for station {serial}")

                add_station_log(db, station.id, "login", data)
                db.commit()

                # Сразу запрашиваем инвентарь чтобы знать реальное состояние слотов
                mqtt_query_inventory(serial)
        finally:
            db.close()

        _notify_self(serial, "heartbeat")

    # report/22 — Power Bank Returned (пользователь вернул павербанк)
    elif cmd_type == "22":
        slot      = data.get("slot")
        pb_serial = data.get("rl_pdid")
        qoe       = data.get("rl_qoe", 0)
        temp      = data.get("rl_tmp", 0)
        mqtt_log.info(
            f"Station {serial} RETURN slot={slot} "
            f"pb={pb_serial} charge={qoe_to_percent(qoe)} temp={temp}°C"
        )

        # Подтверждаем станции (обязательно по протоколу!)
        _mqtt_publish(
            f"cabinet/{serial}/cmd/22",
            {"rl_slot": slot, "rl_result": 1, "rl_seq": next_seq()}
        )

        # Уведомляем /station/event
        _notify_self(
            serial, "returned",
            slot=slot,
            powerbank_serial=str(pb_serial) if pb_serial else None,
        )
    else:
        mqtt_log.info(f"Station {serial} report/{cmd_type}: {data}")


# ---------- MQTT PUBLISH ----------

def _mqtt_publish(topic: str, data: dict) -> bool:
    if not _mqtt_client or not _mqtt_client.is_connected():
        mqtt_log.warning(f"MQTT not connected — cannot publish to {topic}")
        return False
    payload = json.dumps(data)
    result  = _mqtt_client.publish(topic, payload, qos=1, retain=False)
    mqtt_log.info(f">> {topic}: {payload}")
    return result.rc == mqtt.MQTT_ERR_SUCCESS


def mqtt_unlock_slot(serial: str, slot: int, command_id: str) -> bool:
    """cmd/15 — Выдать павербанк из слота."""
    return _mqtt_publish(
        f"cabinet/{serial}/cmd/15",
        {"rl_slot": slot, "rl_seq": next_seq(), "command_id": command_id},
    )


def mqtt_force_push(serial: str, slot: int) -> bool:
    """cmd/11 — Принудительная выдача (для тестов/обслуживания)."""
    return _mqtt_publish(
        f"cabinet/{serial}/cmd/11",
        {"rl_slot": slot, "rl_seq": next_seq()},
    )


def mqtt_query_inventory(serial: str) -> bool:
    """cmd/13 — Запросить состояние всех слотов."""
    return _mqtt_publish(
        f"cabinet/{serial}/cmd/13",
        {"rl_seq": next_seq()},
    )


def mqtt_reset_station(serial: str) -> bool:
    """cmd/25 — Перезагрузить станцию."""
    return _mqtt_publish(
        f"cabinet/{serial}/cmd/25",
        {"rl_seq": next_seq()},
    )


def mqtt_query_sim(serial: str) -> bool:
    """cmd/20 — Запросить ICCID SIM карты."""
    return _mqtt_publish(
        f"cabinet/{serial}/cmd/20",
        {"rl_seq": next_seq()},
    )


def mqtt_query_network(serial: str) -> bool:
    """cmd/24 — Запросить информацию о сети."""
    return _mqtt_publish(
        f"cabinet/{serial}/cmd/24",
        {"rl_seq": next_seq()},
    )


# ---------- INTERNAL EVENT ----------

def _notify_self(serial: str, event: str,
                 slot: Optional[int] = None,
                 command_id: Optional[str] = None,
                 powerbank_serial: Optional[str] = None):
    """Вызывает /station/event внутри самого сервиса (из MQTT треда)."""
    payload: dict = {"serial": serial, "event": event}
    if slot is not None:
        payload["slot"] = slot
    if command_id:
        payload["command_id"] = command_id
    if powerbank_serial:
        payload["powerbank_serial"] = powerbank_serial
    try:
        http_requests.post(f"{SELF_URL}/station/event", json=payload, timeout=5)
    except Exception as e:
        mqtt_log.error(f"_notify_self error: {e}")


# ---------- MQTT START ----------

def _start_mqtt():
    global _mqtt_client

    _mqtt_client = mqtt.Client(client_id=f"powerbank_server_{uuid.uuid4().hex[:8]}")
    if MQTT_USER:
        _mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)

    _mqtt_client.on_connect    = _mqtt_on_connect
    _mqtt_client.on_disconnect = _mqtt_on_disconnect
    _mqtt_client.on_message    = _mqtt_on_message

    def _run():
        while True:
            try:
                mqtt_log.info(f"Connecting to MQTT {MQTT_HOST}:{MQTT_PORT}...")
                _mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
                _mqtt_client.loop_forever()
            except Exception as e:
                mqtt_log.error(f"MQTT error: {e} — retry in 5s")
                time.sleep(5)

    threading.Thread(target=_run, daemon=True, name="mqtt").start()
    mqtt_log.info("MQTT thread started")


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
                delta = (datetime.utcnow() - s.last_seen).total_seconds()
                if delta > STATION_TIMEOUT and s.online == 1:
                    s.online = 0
                    log.warning(f"Station {s.serial} went OFFLINE (no heartbeat for {int(delta)}s)")
            db.commit()
        except Exception as e:
            log.error(f"station_monitor error: {e}")
        finally:
            db.close()
        time.sleep(30)


def _unlock_timeout_monitor():
    """Если павербанк не забрали за 2 минуты — отменяем аренду и освобождаем слот."""
    while True:
        db = get_db()
        try:
            expired = db.query(Rental).filter(
                Rental.status == "pending",
                Rental.unlock_deadline < datetime.utcnow(),
            ).all()

            for rental in expired:
                log.warning(f"Rental {rental.id} expired without pickup, cancelling")
                rental.status = "failed"

                slot = db.query(StationSlot).filter(
                    StationSlot.station_id == rental.station_id,
                    StationSlot.slot_number == rental.slot_number,
                ).first()
                if slot:
                    slot.status = "occupied"

                station = db.query(Station).filter(Station.id == rental.station_id).first()
                if station:
                    station.powerbanks += 1

            if expired:
                db.commit()
        except Exception as e:
            log.error(f"unlock_timeout_monitor error: {e}")
        finally:
            db.close()
        time.sleep(15)


# =========================
# 🚀 STARTUP
# =========================

@app.on_event("startup")
async def on_startup():
    _start_mqtt()
    threading.Thread(target=_station_monitor,        daemon=True, name="station_monitor").start()
    threading.Thread(target=_unlock_timeout_monitor, daemon=True, name="unlock_monitor").start()
    log.info("✅ App started")


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


# =========================
# 🔐 AUTH
# =========================

@app.post("/admin/seed-station/{station_id}")
def seed_station(station_id: int):
    """Создаёт тестовые слоты для станции."""
    db = get_db()
    try:
        station = db.query(Station).filter(Station.id == station_id).first()
        if not station:
            raise HTTPException(404, "Station not found")
        db.query(StationSlot).filter(StationSlot.station_id == station_id).delete()
        for i in range(1, station.slots + 1):
            db.add(StationSlot(
                station_id=station_id,
                slot_number=i,
                status="occupied",
                charge_level=4,
                powerbank_serial=f"PB{i:03d}",
            ))
        station.powerbanks = station.slots
        db.commit()
        return {"success": True, "slots": station.slots}
    finally:
        db.close()


@app.post("/auth/dev-login")
def dev_login():
    """Быстрый вход для тестов — без SMS."""
    db = get_db()
    try:
        user = db.query(User).filter(User.phone == "998000000000").first()
        if not user:
            user = User(
                telegram_id="dev_test_user",
                phone="998000000000",
                name="Dev User",
                balance=1000,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
        token = str(uuid.uuid4())
        db.add(LoginToken(token=token, user_id=user.id))
        db.commit()
        return {"token": token, "user": {"id": user.id, "phone": user.phone}}
    finally:
        db.close()


@app.post("/auth/send-code")
def send_code(data: SendCodeRequest):

    import random

    code = str(random.randint(1000, 9999))

    otp_codes[data.phone] = code

    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    twilio_phone = os.getenv("TWILIO_PHONE")

    client = Client(account_sid, auth_token)

    try:

        client.messages.create(
            body=f"Azapower code: {code}",
            from_=twilio_phone,
            to=f"+{data.phone}",
        )

        log.info(f"OTP sent to {data.phone}")

        return {
            "success": True
        }

    except Exception as e:

        log.error(f"SMS error: {e}")

        raise HTTPException(
            500,
            "Ошибка отправки SMS",
        )


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
# 👤 ME
# =========================

@app.get("/me")
def get_me(authorization: str = Header(None)):
    user = require_user(authorization)
    db   = get_db()
    try:
        cards   = db.query(Card).filter(Card.user_id == user.id).all()
        rentals = db.query(Rental).filter(Rental.user_id == user.id).order_by(Rental.id.desc()).all()

        active_rental = next(
            (r for r in rentals if r.status in ["active", "pending"]), None
        )

        return {
            "id":      user.id,
            "phone":   user.phone,
            "name":    user.name,
            "balance": user.balance,
            "cards": [
                {"id": c.id, "brand": c.brand, "last4": c.last4, "is_active": c.is_active}
                for c in cards
            ],
            "active_rental": {
                "id":         active_rental.id,
                "station_id": active_rental.station_id,
                "status":     active_rental.status,
                "start_time": active_rental.start_time,
            } if active_rental else None,
            "history": [
                {
                    "id":         r.id,
                    "status":     r.status,
                    "cost":       r.cost,
                    "station_id": r.station_id,
                    "start_time": r.start_time,
                    "end_time":   r.end_time,
                }
                for r in rentals
            ],
        }
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
                "id":          s.id,
                "name":        s.name,
                "serial":      s.serial,
                "powerbanks":  s.powerbanks,
                "empty_slots": s.slots - s.powerbanks,
                "slots":       s.slots,
                "online":      s.online,
                "address":     s.address,
                "lat":         s.lat,
                "lng":         s.lng,
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

        slots = db.query(StationSlot).filter(
            StationSlot.station_id == station_id
        ).order_by(StationSlot.slot_number).all()

        return {
            "id":          s.id,
            "name":        s.name,
            "serial":      s.serial,
            "powerbanks":  s.powerbanks,
            "slots":       s.slots,
            "online":      s.online,
            "address":     s.address,
            "lat":         s.lat,
            "lng":         s.lng,
            "last_seen":   s.last_seen,
            "slot_details": [
                {
                    "slot":             sl.slot_number,
                    "status":           sl.status,
                    "powerbank_serial": sl.powerbank_serial,
                    "charge_level":     sl.charge_level,
                    "charge_display":   qoe_to_percent(sl.charge_level or 0),
                }
                for sl in slots
            ],
        }
    finally:
        db.close()


# =========================
# 📡 STATION EVENTS (внутренний эндпоинт от MQTT треда)
# =========================

@app.post("/station/event")
async def station_event(data: StationEventRequest):
    """
    Вызывается MQTT-обработчиком через _notify_self().
    Обновляет БД и уведомляет пользователей через WebSocket.
    """
    db = get_db()
    try:
        station = db.query(Station).filter(Station.serial == data.serial).first()
        if not station:
            log.warning(f"/station/event: unknown serial={data.serial}")
            raise HTTPException(404, "Station not found")

        add_station_log(db, station.id, data.event, data.dict())

        # --- станция онлайн ---
        if data.event == "heartbeat":
            station.online    = 1
            station.last_seen = datetime.utcnow()

        # --- павербанк успешно выдан ---
        elif data.event == "dispensed":
            # Помечаем слот пустым
            slot_row = db.query(StationSlot).filter(
                StationSlot.station_id == station.id,
                StationSlot.slot_number == data.slot,
            ).first()
            if slot_row:
                slot_row.status = "empty"
                if data.powerbank_serial:
                    slot_row.powerbank_serial = data.powerbank_serial

            # Активируем аренду
            rental = db.query(Rental).filter(
                Rental.station_id == station.id,
                Rental.slot_number == data.slot,
                Rental.status == "pending",
            ).order_by(Rental.id.desc()).first()

            if rental:
                rental.status = "active"
                log.info(f"Rental {rental.id} activated (slot {data.slot})")

                # Уведомляем пользователя
                for ws in connections.get(rental.user_id, []):
                    try:
                        await ws.send_json({
                            "type":   "powerbank_dispensed",
                            "rental": rental.id,
                            "slot":   data.slot,
                        })
                    except Exception:
                        pass

            # Помечаем команду выполненной
            if data.command_id:
                cmd = db.query(StationCommand).filter(
                    StationCommand.command_id == data.command_id
                ).first()
                if cmd:
                    cmd.status = "executed"

        # --- выдача провалилась ---
        elif data.event == "dispense_failed":
            rental = db.query(Rental).filter(
                Rental.station_id == station.id,
                Rental.slot_number == data.slot,
                Rental.status == "pending",
            ).order_by(Rental.id.desc()).first()

            if rental:
                rental.status = "failed"
                station.powerbanks += 1

                slot_row = db.query(StationSlot).filter(
                    StationSlot.station_id == station.id,
                    StationSlot.slot_number == data.slot,
                ).first()
                if slot_row:
                    slot_row.status = "occupied"

                log.warning(f"Rental {rental.id} failed (dispense error)")

                for ws in connections.get(rental.user_id, []):
                    try:
                        await ws.send_json({
                            "type":  "dispense_failed",
                            "rental": rental.id,
                        })
                    except Exception:
                        pass

            if data.command_id:
                cmd = db.query(StationCommand).filter(
                    StationCommand.command_id == data.command_id
                ).first()
                if cmd:
                    cmd.status = "failed"

        # --- павербанк возвращён ---
        elif data.event == "returned":
            slot_row = db.query(StationSlot).filter(
                StationSlot.station_id == station.id,
                StationSlot.slot_number == data.slot,
            ).first()
            if slot_row:
                slot_row.status           = "occupied"
                slot_row.powerbank_serial = data.powerbank_serial or None

            rental = db.query(Rental).filter(
                Rental.station_id == station.id,
                Rental.slot_number == data.slot,
                Rental.status == "active",
            ).order_by(Rental.id.desc()).first()

            if rental:
                end_time       = datetime.utcnow()
                cost           = calc_cost(rental.start_time, end_time)
                rental.cost    = cost
                rental.end_time = end_time
                rental.status  = "completed"
                # Оплата: заглушка — TODO подключить Alif
                rental.payment_status = "pending"

                station.powerbanks += 1
                log.info(f"Rental {rental.id} completed, cost={cost}")

                for ws in connections.get(rental.user_id, []):
                    try:
                        await ws.send_json({
                            "type":           "rental_finished",
                            "rental":         rental.id,
                            "cost":           cost,
                            "payment_status": rental.payment_status,
                        })
                    except Exception:
                        pass

        db.commit()
        return {"success": True}
    finally:
        db.close()


# =========================
# 🔧 STATION CONTROL (операторские команды)
# =========================

@app.post("/station/reset/{serial}")
def reset_station(serial: str, authorization: str = Header(None)):
    """Перезагрузить станцию."""
    require_user(authorization)
    ok = mqtt_reset_station(serial)
    return {"success": ok}


@app.post("/station/inventory/{serial}")
def query_inventory(serial: str, authorization: str = Header(None)):
    """Запросить и обновить инвентарь станции."""
    require_user(authorization)
    ok = mqtt_query_inventory(serial)
    return {"success": ok}


@app.post("/station/force-push/{serial}")
def force_push(serial: str, slot: int, authorization: str = Header(None)):
    """Принудительная выдача — только для тестов/обслуживания."""
    require_user(authorization)
    ok = mqtt_force_push(serial, slot)
    return {"success": ok}


@app.post("/station/sim/{serial}")
def query_sim(serial: str, authorization: str = Header(None)):
    """Запросить ICCID SIM карты станции."""
    require_user(authorization)
    ok = mqtt_query_sim(serial)
    return {"success": ok}


@app.post("/station/network/{serial}")
def query_network(serial: str, authorization: str = Header(None)):
    """Запросить информацию о сети станции."""
    require_user(authorization)
    ok = mqtt_query_network(serial)
    return {"success": ok}


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
        if station.online == 0:
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
        unpaid   = db.query(Rental).filter(
            Rental.user_id == user.id,
            Rental.payment_status != "paid",
            Rental.status == "completed",
        ).all()
        debt_sum = sum(r.cost or 0 for r in unpaid)
        if debt_sum > DEBT_LIMIT:
            raise HTTPException(400, f"Превышен лимит долга: {debt_sum}")

        # Выбираем свободный слот с максимальным зарядом
        slot = db.query(StationSlot).filter(
            StationSlot.station_id == station.id,
            StationSlot.status == "occupied",
        ).order_by(StationSlot.charge_level.desc()).first()

        if not slot:
            raise HTTPException(400, "Нет доступных слотов")

        command_id = str(uuid.uuid4())

        # Сохраняем команду
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

        # Создаём аренду (без реального холда — оплата TODO)
        rental = Rental(
            user_id=user.id,
            station_id=data.station_id,
            slot_number=slot.slot_number,
            status="pending",
            start_time=datetime.utcnow(),
            unlock_deadline=datetime.utcnow() + timedelta(minutes=2),
            hold_id="stub",
            hold_amount=0,
            payment_status="none",
        )
        station.powerbanks -= 1
        db.add(rental)
        db.commit()
        db.refresh(rental)

        # Отправляем MQTT команду станции
        sent = mqtt_unlock_slot(station.serial, slot.slot_number, command_id)
        cmd.status = "sent" if sent else "failed"
        if not sent:
            log.warning(f"MQTT offline — command queued for station {station.serial}")
        db.commit()

        return {
            "id":   rental.id,
            "slot": slot.slot_number,
            "sent": sent,
        }
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
                "id":             r.id,
                "status":         r.status,
                "cost":           r.cost,
                "payment_status": r.payment_status,
                "station_id":     r.station_id,
                "slot_number":    r.slot_number,
                "start_time":     r.start_time,
                "end_time":       r.end_time,
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
# 💰 PAYMENTS (заглушка — TODO Alif)
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
            provider="stub",
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
