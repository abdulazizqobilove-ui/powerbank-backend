import os
import requests

from dotenv import load_dotenv

load_dotenv()

ALIF_API_URL = os.getenv("ALIF_API_URL")
ALIF_MERCHANT_ID = os.getenv("ALIF_MERCHANT_ID")
ALIF_SECRET_KEY = os.getenv("ALIF_SECRET_KEY")


def create_hold(user_id, amount):

    print("CREATE HOLD")

    return {
        "success": True,
        "hold_id": f"hold_{user_id}_{amount}",
    }


def capture_hold(hold_id, amount):

    print("CAPTURE HOLD")

    return True