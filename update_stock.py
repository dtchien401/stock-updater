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

spreadsheet = gc.open_by_key(SPREADSHEET_ID)
data_sheet = spreadsheet.worksheet("Sheet1")
symbols_sheet = spreadsheet.worksheet("Symbols")

# =========================
# 1) Đọc danh sách mã từ sheet Symbols
# =========================
symbols_values = symbols_sheet.get_all_values()
symbols = []

if len(symbols_values) > 1:
    for row in symbols_values[1:]:
        if len(row) > 0 and str(row[0]).strip():
            symbols.append(str(row[0]).strip().upper())

# =========================
# 2) Đọc toàn bộ dữ liệu hiện có trong Sheet1
#    tạo map:
#    (symbol, date) -> row number
#    đồng thời biết mã nào đã có dữ liệu
# =========================
all_values = data_sheet.get_all_values()

existing_map = {}
existing_symbols = set()

if len(all_values) > 1:
    for idx, row in enumerate(all_values[1:], start=2):
        if len(row) >= 2:
            symbol_key = str(row[0]).strip().upper()
            date_key = str(row[1]).strip()[:10]
            existing_map[(symbol_key, date_key)] = idx
            existing_symbols.add(symbol_key)

# =========================
# 3) Hàm chuẩn hóa row để ghi vào Google Sheet
# =========================
def make_row(symbol, rec):
    return [
        str(symbol),
        str(rec["time"])[:10],
        float(rec["open"]),
        float(rec["high"]),
        float(rec["low"]),
        float(rec["close"]),
        int(rec["volume"])
    ]

# =========================
# 4) Chạy theo từng mã
#    - mã mới => backfill 250 phiên
#    - mã cũ => update 1 phiên mới nhất
# =========================
for symbol in symbols:
    print(f"Processing {symbol}...")

    quote = Quote(symbol=symbol, source="KBS")
    df = quote.history(start="2024-01-01", interval="1D")

    if df is None or df.empty:
        print(f"No data for {symbol}")
        continue

    # ===== CASE A: mã mới => backfill 250 phiên =====
    if symbol not in existing_symbols:
        print(f"New symbol detected: {symbol} -> backfill 250 sessions")

        backfill_df = df.tail(250)
        rows_to_append = []

        for _, rec in backfill_df.iterrows():
            row = make_row(symbol, rec)
            key = (symbol, row[1])

            if key not in existing_map:
                rows_to_append.append(row)
                existing_map[key] = True

        if rows_to_append:
            data_sheet.append_rows(rows_to_append)
            print(f"Backfilled {len(rows_to_append)} rows for {symbol}")
        else:
            print(f"No backfill rows needed for {symbol}")

        continue

    # ===== CASE B: mã cũ => chỉ lấy 1 dòng mới nhất =====
    last = df.tail(1).iloc[0]
    row = make_row(symbol, last)
    key = (symbol, row[1])

    if key in existing_map and isinstance(existing_map[key], int):
        row_number = existing_map[key]
        data_sheet.update(f"A{row_number}:G{row_number}", [row])
        print(f"Updated existing row: {symbol} {row[1]}")
    else:
        data_sheet.append_row(row)
        print(f"Appended new row: {symbol} {row[1]}")
