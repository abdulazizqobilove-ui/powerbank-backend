from database import SessionLocal
from models import Station

db = SessionLocal()

station = Station(
    id=1,
    name="Street Game Club",
    address="ул. Табиби",
    lat=41.3111,
    lng=69.2797
)

db.add(station)
db.commit()

print("✅ Station added")