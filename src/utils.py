import pandas as pd
import os
from datetime import datetime

def save_to_csv(data, filename):
    """Save data to a csv file."""
    if not data:
        return
    os.makedirs("data", exist_ok=True)
    filepath = f"data/{filename}.csv"
    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    