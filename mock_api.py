# mock_api.py

from fastapi import FastAPI

app = FastAPI()

# Simulated database
MOCK_DB = []

# Generate 50 records
for i in range(50):
    MOCK_DB.append({
        "date": "2025-06-15",
        "declaration_number": f"DEC{i+1}",
        "hs_code": "85",
        "country": "India",
        "type": "export"
    })


@app.get("/{api_key}/{country}/{data_type}/{from_date}/{to_date}/{range_value}/{operator}/{hs_code}")
def fetch_data(api_key: str,
               country: str,
               data_type: str,
               from_date: str,
               to_date: str,
               range_value: str,
               operator: str,
               hs_code: str):

    # API key validation
    if api_key != "API123":
        return []

    start, end = map(int, range_value.split("-"))

    filtered = [
        record for record in MOCK_DB
        if record["country"] == country
        and record["type"] == data_type
        and record["hs_code"] == hs_code.split("-")[1]
        and from_date <= record["date"] <= to_date
    ]

    return filtered[start:end]