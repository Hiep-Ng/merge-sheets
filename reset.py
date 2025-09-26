from google.oauth2.service_account import Credentials
import gspread

SERVICE_ACCOUNT_FILE = "service-account.json"
TARGET_SHEET_ID = "1WgIL9FVP2iLXe1-zXICoBzcl2dQhuyyEUZvDofRltrQ"
SCOPES = ["https://www.googleapis.com/auth/drive",
          "https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)
ws = gc.open_by_key(TARGET_SHEET_ID).sheet1

# Thực hiện clear
ws.clear()
print("✅ Sheet đã được xóa toàn bộ nội dung!")
