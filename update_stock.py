import json
import os
import gspread
from google.oauth2.service_account import Credentials
from vnstock import Quote

service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])

scopes = [
    "https://www.googleapis.com/auth/spreadsheets"
]

creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=scopes
)

gc = gspread.authorize(creds)

SPREADSHEET_ID = "1g3TUiH4ReFruGBbkhbs2DFofFwZoWMcDzqlloYv3VfQ"
sheet = gc.open_by_key(SPREADSHEET_ID).sheet1

symbols = ["HPG", "VCG", "VPB", "POW", "SZC", "PVD", "PLX", "VIC"]

for symbol in symbols:
    quote = Quote(symbol=symbol, source="KBS")
    df = quote.history(start="2024-01-01", interval="1D")

    if df is None or df.empty:
        print(f"No data for {symbol}")
        continue

    last = df.tail(1).iloc[0]

    row = [
        str(symbol),
        str(last["time"])[:10],
        float(last["open"]),
        float(last["high"]),
        float(last["low"]),
        float(last["close"]),
        int(last["volume"])
    ]

    sheet.append_row(row)
    print("Updated:", symbol, row)
