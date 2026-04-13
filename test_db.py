from database import engine

try:
    connection = engine.connect()
    print("✅ DB CONNECTED")
    connection.close()
except Exception as e:
    print("❌ ERROR:", e)