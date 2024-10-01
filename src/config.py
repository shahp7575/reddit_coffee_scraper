import os
from dotenv import load_dotenv

load_dotenv()

def get_credentials():

    return {
        "client_id": os.getenv("CLIENT_ID"),
        "client_secret": os.getenv("CLIENT_SECRET"),
        "user_agent": os.getenv("USER_AGENT")
    }

