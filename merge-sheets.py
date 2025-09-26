from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
import gspread
import pandas as pd
import requests
import os
import json

SERVICE_ACCOUNT_FILE = "service-account.json"
SOURCE_FOLDER_ID = "1dJ6Ilx7Sf25ZehS_ZS3Hvkms89mclgfT"
TARGET_SHEET_ID = "1WgIL9FVP2iLXe1-zXICoBzcl2dQhuyyEUZvDofRltrQ"
MANAGER_SHEET_ID = "1LDyAilDBDuM9ND0ncv-SOF12_JhCZlNrZPU4iVkgRnY"

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

PROCESSED_JSON = "processed_files.json"


def load_processed():
    """Đọc danh sách file đã xử lý từ JSON (nếu chưa có → tạo mới)."""
    if not os.path.exists(PROCESSED_JSON):
        with open(PROCESSED_JSON, "w") as f:
            json.dump([], f)
        return set()
    with open(PROCESSED_JSON, "r") as f:
        return set(json.load(f))


def save_processed(processed):
    """Lưu danh sách file đã xử lý vào JSON."""
    with open(PROCESSED_JSON, "w") as f:
        json.dump(sorted(list(processed)), f)


def sync_manager_to_target(gc, ws, manager_ws):
    """Đồng bộ dữ liệu từ Manager Sheet sang Target Sheet."""
    target_df = pd.DataFrame(ws.get_all_records())
    if target_df.empty:
        print("⚠️ Target Sheet đang rỗng, bỏ qua sync.")
        return

    manager_df = pd.DataFrame(manager_ws.get_all_records())
    if manager_df.empty:
        print("⚠️ Manager Sheet rỗng, bỏ qua sync.")
        return

    if "Input file" not in manager_df.columns:
        print("⚠️ Manager Sheet không có cột 'Input file', bỏ qua sync.")
        return

    if "Source_File" not in target_df.columns:
        print("⚠️ Target Sheet không có cột 'Source_File', bỏ qua sync.")
        return

    # Gộp dữ liệu: match Source_File vs Input file
    merged_df = target_df.merge(
        manager_df, left_on="Source_File", right_on="Input file", how="left"
    )

    # Bỏ cột Input file trùng lặp sau khi merge
    merged_df = merged_df.drop(columns=["Input file"], errors="ignore")

    # Ghi đè toàn bộ sheet (giữ header + giá trị)
    ws.clear()
    ws.update([merged_df.columns.values.tolist()] + merged_df.values.tolist())
    print("🔄 Đã đồng bộ dữ liệu từ Manager Sheet vào Target Sheet.")


def main():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    creds.refresh(Request())
    gc = gspread.authorize(creds)

    ws = gc.open_by_key(TARGET_SHEET_ID).sheet1
    manager_ws = gc.open_by_key(MANAGER_SHEET_ID).sheet1

    processed_files = load_processed()
    print(
        f"📂 Đã ghi nhận {len(processed_files)} file đã xử lý trước đây (JSON).")

    token = creds.token
    query = (
        f"'{SOURCE_FOLDER_ID}' in parents and "
        "(mimeType='application/vnd.google-apps.spreadsheet' or "
        " mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')"
    )
    url = (
        "https://www.googleapis.com/drive/v3/files?"
        f"q={query}&fields=files(id,name,mimeType)"
    )
    headers = {"Authorization": f"Bearer {token}"}
    files = requests.get(url, headers=headers).json().get("files", [])
    print(f"🔍 Tìm thấy {len(files)} file trong folder nguồn.")

    all_data = []
    updated_processed = set(processed_files)

    for f in files:
        if f["name"] in processed_files:
            print(f"⏭️ Bỏ qua (đã xử lý JSON): {f['name']}")
            continue

        print(f"➡️ Đang xử lý: {f['name']} ({f['mimeType']})")
        try:
            if f["mimeType"] == "application/vnd.google-apps.spreadsheet":
                sh = gc.open_by_key(f["id"])
                df = pd.DataFrame(sh.sheet1.get_all_records())
            else:
                download_url = f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media"
                res = requests.get(download_url, headers=headers)
                temp_file = f"temp_{f['id']}.xlsx"
                with open(temp_file, "wb") as tmp:
                    tmp.write(res.content)
                df = pd.read_excel(temp_file)
                os.remove(temp_file)

            if df.empty:
                print("⚠️ File trống, bỏ qua.")
                continue

            first_col = df.columns[0]
            df = df[~df[first_col].astype(
                str).str.strip().str.fullmatch("Tổng", case=False)]
            df = df.drop(columns=[c for c in df.columns if c.lower() in ["nguon_file", "stt"]],
                         errors="ignore")
            df["Source_File"] = f["name"]

            if not df.empty:
                all_data.append(df)
                updated_processed.add(f["name"])
                print(f"✅ Đã thêm: {f['name']}")
            else:
                print("⚠️ Toàn bộ hàng bị loại sau khi lọc.")
        except Exception as e:
            print(f"❌ Lỗi đọc {f['name']}: {e}")

    if not all_data:
        print("⚠️ Không có dữ liệu mới để gộp.")
    else:
        final_df = pd.concat(all_data, ignore_index=True).fillna("")
        if "Source_File" not in final_df.columns:
            final_df["Source_File"] = ""

        COLUMNS_ORDER = list(final_df.columns)
        final_df = final_df.reindex(columns=COLUMNS_ORDER, fill_value="")

        existing_values = ws.get_all_values()
        if not existing_values or all(not any(cell.strip() for cell in row) for row in existing_values):
            ws.clear()
            ws.update([final_df.columns.values.tolist()] +
                      final_df.values.tolist())
            print("✅ Google Sheet đã được tạo mới với toàn bộ dữ liệu!")
        else:
            ws.append_rows(final_df.values.tolist(), value_input_option="RAW")
            print("✅ Đã thêm dữ liệu mới vào cuối Google Sheet!")

    # 🔒 Lưu lại danh sách file đã xử lý
    save_processed(updated_processed)
    print(f"💾 Đã cập nhật JSON: {len(updated_processed)} file đã xử lý.")

    # 🔄 Đồng bộ dữ liệu từ Manager Sheet sang Target Sheet
    sync_manager_to_target(gc, ws, manager_ws)


if __name__ == "__main__":
    main()
