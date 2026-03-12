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

symbols = ["HPG", "VCG", "VPB", "POW", "SZC", "PVD", "MBB", "GVR", "GMD", "PNJ", "E1VFVN30", "FUEVFVND"]

# Đọc toàn bộ dữ liệu hiện có để tạo map: (symbol, date) -> row number
all_values = sheet.get_all_values()

existing_map = {}
if len(all_values) > 1:
    for idx, row in enumerate(all_values[1:], start=2):  # start=2 vì dòng 1 là header
        if len(row) >= 2:
            symbol_key = str(row[0]).strip().upper()
            date_key = str(row[1]).strip()[:10]
            existing_map[(symbol_key, date_key)] = idx

for symbol in symbols:
    quote = Quote(symbol=symbol, source="KBS")
    df = quote.history(start="2024-01-01", interval="1D")

    if df is None or df.empty:
        print(f"No data for {symbol}")
        continue

    last = df.tail(1).iloc[0]

    date_value = str(last["time"])[:10]

    row = [
        str(symbol),
        date_value,
        float(last["open"]),
        float(last["high"]),
        float(last["low"]),
        float(last["close"]),
        int(last["volume"])
    ]

    key = (symbol, date_value)

    if key in existing_map:
        row_number = existing_map[key]
        sheet.update(f"A{row_number}:G{row_number}", [row])
        print(f"Updated existing row: {symbol} {date_value}")
    else:
        sheet.append_row(row)
        print(f"Appended new row: {symbol} {date_value}")
