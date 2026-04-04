from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime
from database import Base
from datetime import datetime

class Station(Base):
    __tablename__ = "stations"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    powerbanks = Column(Integer)

class Rental(Base):
    __tablename__ = "rentals"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer)
    station_id = Column(Integer)
    status = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
cost = Column(Integer)

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True)
    password = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    role = Column(String, default="user")

class RentRequest(BaseModel):
    user_id: int
    station_id: int


class ReturnRequest(BaseModel):
    rental_id: int