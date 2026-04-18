import requests
import pandas as pd
import time
import random
import os
import json
import gspread
import google.auth
from google.oauth2.service_account import Credentials

# ==========================================
# 1. 取得家人Sheet的股票編號資料
# ==========================================
csv_url = "https://docs.google.com/spreadsheets/d/1GGtdEkqKvG6PJnxl0qpi4eaOB9hbUNB_XhnFy0H6TEc/export?format=csv&gid=830651350"
df = pd.read_csv(csv_url)
column_a_list = df["個股"].dropna().tolist()  
N = 10  # 要抓幾支股票
Ini = 0
column_a_list_100 = column_a_list[Ini:Ini+N]

# ==========================================
# 2. 抓取單支股票最近 6 個月的日收盤價
# ==========================================
def fetch_stock_price(stock_no, months=6):
    """抓取單支股票最近 n 個月的日收盤價"""
    all_data = []
    today = pd.Timestamp.today()

    for i in range(months):
        query_date = (today - pd.DateOffset(months=i)).strftime("%Y%m01")
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={query_date}&stockNo={stock_no}"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                try:
                    data = response.json()
                except requests.exceptions.JSONDecodeError:
                    print(f"警告: {stock_no} 於 {query_date} 解析 JSON 失敗，跳過。")
                    time.sleep(3) # 遇到錯誤時稍微等久一點
                    continue

                stock_name = stock_no

                if data and 'stat' in data and data['stat'] == 'OK':
                    try:
                        title_parts = data['title'].split(' ')
                        if len(title_parts) >= 3:
                            stock_name = title_parts[2]
                    except (KeyError, IndexError):
                        pass 

                    if "data" in data:
                        for row in data["data"]:
                            # 轉換日期（民國年 → 西元年）
                            year, month, day = row[0].split("/")
                            date = f"{int(year) + 1911}-{month}-{day}"

                            if row[6] == "--":
                                continue  # 跳過無收盤價的日期

                            close_price = float(row[6].replace(",", ""))
                            all_data.append([stock_no, stock_name, date, close_price])
            else:
                print(f"API 請求失敗 {stock_no} 狀態碼: {response.status_code}")
        except Exception as e:
            print(f"請求發生錯誤 {stock_no}: {e}")

        time.sleep(3)  # 優化: 證交所API抓很嚴，建議休息 3 秒避免被封鎖 IP
    return all_data

# ==========================================
# 3. 取得所有股票的布林通道數據
# ==========================================
def calculate_bollinger_bands(stock_no):
    """計算單支股票的布林通道"""
    data = fetch_stock_price(stock_no)
    if not data:
        return None

    df = pd.DataFrame(data, columns=["股票代號", "股票名稱", "日期", "收盤價"])
    df["日期"] = pd.to_datetime(df["日期"])
    df = df.sort_values(by="日期")

    # 確保資料量大於 100 天再計算，否則會報錯或失真
    if len(df) < 100:
        print(f"{stock_no} 資料不足 100 天，無法計算 20 週布林通道。")
        return None

    df["SMA"] = df["收盤價"].rolling(window=100).mean()
    df["STD"] = df["收盤價"].rolling(window=100).std()
    df["Upper"] = df["SMA"] + 2 * df["STD"]
    df["Lower"] = df["SMA"] - 2 * df["STD"]
    df["跌破下軌?"] = df["收盤價"] < df["Lower"]

    latest = df.iloc[-1]
    return latest if latest["跌破下軌?"] else None

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
