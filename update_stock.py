import json
import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from vnstock import Quote

# đọc secret từ github
service_account_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)

gc = gspread.authorize(creds)

# mở google sheet
sheet = gc.open("Data Cp").sheet1

symbols = ["HPG","VCG","VPB","POW","SZC","PVD","PLX","VIC"]

for symbol in symbols:

    quote = Quote(symbol=symbol, source="KBS")

    df = quote.history(
        start="2024-01-01",
        interval="1D"
    )

    last = df.tail(1)

    row = [
        symbol,
        str(last.iloc[0]["time"]),
        last.iloc[0]["open"],
        last.iloc[0]["high"],
        last.iloc[0]["low"],
        last.iloc[0]["close"],
        last.iloc[0]["volume"]
    ]

    sheet.append_row(row)

    print("Updated:", symbol)
