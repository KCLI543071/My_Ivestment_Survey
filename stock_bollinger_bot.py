import requests
import pandas as pd
import time
import random
import os
import json
import gspread
import google.auth
from google.oauth2.service_account import Credentials
import yfinance as yf

# ==========================================
# 1. 取得家人Sheet的股票編號資料
# ==========================================
csv_url = "https://docs.google.com/spreadsheets/d/1GGtdEkqKvG6PJnxl0qpi4eaOB9hbUNB_XhnFy0H6TEc/export?format=csv&gid=830651350"
df = pd.read_csv(csv_url)
column_a_list = df["個股"].dropna().tolist()  
N = 100 # 要抓幾支股票
Ini = 20
column_a_list_100 = column_a_list[Ini:Ini+N]

# ==========================================
# 2. 抓取單支股票最近 6 個月的日收盤價 (已優化：改用 Yahoo Finance API)
# ==========================================
def get_stock_data(stock_no):
    """使用 yfinance 快速抓取半年資料，免除 3 秒延遲"""
    stock_no = str(stock_no).strip()
    # 台灣股票代碼格式：上市為 .TW，上櫃為 .TWO
    ticker_tw = f"{stock_no}.TW"
    ticker_two = f"{stock_no}.TWO"
    # 隱藏 yfinance 預設的輸出訊息，並加上 auto_adjust=True 消除 FutureWarning 並取得還原權息股價
    df = yf.download(ticker_tw, period="6mo", progress=False, auto_adjust=True)
    if df.empty: # 如果上市找不到，改找上櫃
        df = yf.download(ticker_two, period="6mo", progress=False, auto_adjust=True)
    return df

# ==========================================
# 3. 取得所有股票的布林通道數據
# ==========================================
def calculate_bollinger_bands(stock_no):
    """計算單支股票的布林通道"""
    df_raw = get_stock_data(stock_no)
    
    if df_raw.empty or len(df_raw) < 100:
        print(f"{stock_no} 資料不足 100 天或找不到標的。")
        return None

    # 處理 yfinance 回傳的資料結構，取出收盤價
    if isinstance(df_raw.columns, pd.MultiIndex):
        close_price = df_raw['Close'].squeeze()
    else:
        close_price = df_raw['Close']

    # 建立乾淨的 DataFrame 進行計算
    calc_df = pd.DataFrame({'收盤價': close_price})
    calc_df['SMA'] = calc_df['收盤價'].rolling(window=100).mean()
    calc_df['STD'] = calc_df['收盤價'].rolling(window=100).std()
    calc_df['Upper'] = calc_df['SMA'] + 2 * calc_df['STD']
    calc_df['Lower'] = calc_df['SMA'] - 2 * calc_df['STD']
    calc_df['跌破下軌?'] = calc_df['收盤價'] < calc_df['Lower']

    latest = calc_df.iloc[-1]
    
    if latest['跌破下軌?']:
        return {
            "股票代號": stock_no,
            "股票名稱": stock_no,  # Yahoo Finance 較難抓中文，暫以代號取代
            "日期": calc_df.index[-1], # 保持時間格式，交由主程式統一轉字串
            "收盤價": round(float(latest['收盤價']), 2),
            "SMA": round(float(latest['SMA']), 2),
            "Upper": round(float(latest['Upper']), 2),
            "Lower": round(float(latest['Lower']), 2)
        }
    return None

# ==========================================
# 4. 主程式執行
# ==========================================
print("開始執行布林通道篩選...")
result_list = []

for stock_no in column_a_list_100:
    print(f"正在處理 {stock_no}...")
    result = calculate_bollinger_bands(stock_no)
    if result is not None:
        result_list.append(result)

if not result_list:
    print("\n本週沒有找到跌破下軌的股票。")
    df_result = pd.DataFrame(columns=["股票代號", "股票名稱", "日期", "收盤價", "SMA", "Upper", "Lower"])
else:
    df_result = pd.DataFrame(result_list)
    # 將日期格式轉回字串，避免寫入 Google Sheet 時格式跑掉
    df_result["日期"] = df_result["日期"].dt.strftime("%Y-%m-%d") 
    required_columns = ["股票代號", "股票名稱", "日期", "收盤價", "SMA", "Upper", "Lower"]
    df_result = df_result[required_columns]
    print(df_result.to_csv(index=False))
# ==========================================
# 5. 自動化輸出到 Google Sheet (免手動認證)
# ==========================================
try:
    # 這裡我們改用 gspread 與環境變數中的金鑰來自動登入
    # GitHub Actions 會將我們設定的密鑰注入到環境變數 GOOGLE_CREDENTIALS 中
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # 爸爸幫你升級了！支援兩種認證方式：
    # 1. JSON 金鑰字串 (個人帳號適用)
    # 2. Workload Identity Federation 免金鑰認證 (公司/學校帳號適用)
    if 'GOOGLE_CREDENTIALS' in os.environ and os.environ['GOOGLE_CREDENTIALS'].strip():
        print("使用環境變數中的 JSON 金鑰進行認證...")
        creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    else:
        print("使用無金鑰 (Workload Identity) 或預設憑證認證...")
        creds, _ = google.auth.default(scopes=scope)

    client = gspread.authorize(creds)

    # 開啟目標 Google Sheet (請替換為你要寫入的目標試算表 ID)
    TARGET_SPREADSHEET_ID = "1GGtdEkqKvG6PJnxl0qpi4eaOB9hbUNB_XhnFy0H6TEc"
    spreadsheet = client.open_by_key(TARGET_SPREADSHEET_ID)
    
    # 假設你要寫入的分頁名稱叫做 "跌破下軌清單"
    # 如果沒有這個分頁，程式會報錯，請先在 Google Sheet 建好這個分頁
    worksheet = spreadsheet.worksheet("跌破下軌清單")
    
    # 清空原本的資料，寫入新資料
    worksheet.clear()
    worksheet.update([df_result.columns.values.tolist()] + df_result.values.tolist())
    
    print("\n太棒了！結果已自動輸出到 Google Sheet。")
except Exception as e:
    print(f"\n無法輸出到 Google Sheet，錯誤原因: {e}")
