from vnstock import Quote
import pandas as pd

symbols = ["HPG","VCG","VPB","POW","SZC","PVD","PLX","VIC", "GVR", "GMD"]

for symbol in symbols:

    quote = Quote(symbol=symbol, source="KBS")

    df = quote.history(
        start="2024-01-01",
        interval="1D"
    )

    print(symbol)
    print(df.tail(1))
