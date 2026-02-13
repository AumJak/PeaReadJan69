import csv
import requests
import requests.adapters
import requests.exceptions
import concurrent.futures
import time
import json
import os
import datetime

# --- ⚙️ ตั้งค่า (USER CONFIG) ---
API_URL = "http://127.0.0.1:6000/predict"
INPUT_FILE = "C3.csv"
OUTPUT_FILE = "Complete.csv"
CACHE_FILE = "temp_progress.json"
MAX_WORKERS = 5
SAVE_INTERVAL = 100
RETRY_DELAY = 10

# --- 🔒 Constants (แก้ Issue Line 114) ---
STATUS_NO_IMAGE = "No Image"
STATUS_API_ERROR = "API Error"
STATUS_SUCCESS = "Success"
STATUS_FAILED = "Failed"

# --- 🚀 ส่วนจูนความเร็ว (Connection Pooling) ---
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=MAX_WORKERS,
    pool_maxsize=MAX_WORKERS,
    max_retries=1
)
session.mount('http://', adapter)
session.mount('https://', adapter)

# --- 🛠️ Utils Functions ---
def load_cache():
    """โหลดไฟล์ Cache"""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                return {int(k): v for k, v in loaded.items()}
        except (OSError, json.JSONDecodeError):
            return {}
    return {}

def save_cache(data):
    """บันทึกไฟล์ Cache"""
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
    except OSError:
        pass

def format_time(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))

def _prepare_csv_row(row, url_columns, idx, results_map):
    """เตรียมข้อมูล 1 แถวเพื่อเขียนลง CSV"""
    new_row = row.copy()
    for col in url_columns:
        res = results_map.get(idx, {}).get(col, {"pea_no": "", "status": "", "method": ""})
        new_row[f"{col}_PEA"] = res["pea_no"]
        
        status_text = res["status"]
        if status_text == STATUS_SUCCESS:
            status_text = res["method"]
        
        new_row[f"{col}_Status"] = status_text
    return new_row

def save_output_csv(filename, headers, url_columns, rows_data, results_map):
    """บันทึกไฟล์ CSV"""
    new_headers = headers.copy()
    for col in url_columns:
        new_headers.append(f"{col}_PEA")
        new_headers.append(f"{col}_Status")

    try:
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=new_headers)
            writer.writeheader()
            for idx, row in enumerate(rows_data):
                new_row = _prepare_csv_row(row, url_columns, idx, results_map)
                writer.writerow(new_row)
    except OSError as e:
        print(f"⚠️ บันทึกไฟล์ CSV ไม่สำเร็จ: {e}")

# --- 🧠 Logic Functions ---
def _parse_api_response(response):
    """แกะผลลัพธ์จาก API"""
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "success":
            result_data = data.get("data", {})
            pea_no = result_data.get("serial_number", "")
            read_method = result_data.get("method", "")
            
            if read_method == "barcode":
                method_display = "Barcode"
            elif read_method == "ocr":
                method_display = "OCR"
            else:
                method_display = read_method
                
            return pea_no, STATUS_SUCCESS, method_display
        else:
            msg = data.get("message", "Unknown")
            is_img_err = "download" in msg.lower() or "image" in msg.lower()
            # ใช้ Constant แทน String Literal
            status = STATUS_NO_IMAGE if is_img_err else STATUS_FAILED
            return "", status, msg
            
    elif response.status_code in [400, 404, 422]:
        return "", STATUS_NO_IMAGE, f"API {response.status_code}"
    
    return "", STATUS_API_ERROR, f"HTTP {response.status_code}"

def process_url_task(row_index, col_name, url):
    """ฟังก์ชันหลัก: ยิง API และ Retry"""
    if not url or not str(url).strip():
        return row_index, col_name, "", STATUS_NO_IMAGE, ""
    
    clean_url = str(url).strip()
    if not clean_url.lower().startswith("http"):
         return row_index, col_name, "", STATUS_NO_IMAGE, "Invalid URL"

    payload = {"url": clean_url}
    
    while True:
        try:
            response = session.post(API_URL, json=payload, timeout=100)
            pea_no, status, method = _parse_api_response(response)
            return row_index, col_name, pea_no, status, method 
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"⚠️ [Row {row_index}] Connection Lost. Retrying in {RETRY_DELAY}s... ({e})")
            time.sleep(RETRY_DELAY)
            continue 
        except Exception as e:
            return row_index, col_name, "", STATUS_API_ERROR, str(e)

# --- 🧩 Sub-Functions for Main (แก้ Issue Line 167) ---

def _load_input_csv(filename):
    """โหลดไฟล์ CSV และคืนค่า headers กับ rows"""
    print(f"📂 กำลังโหลดไฟล์: {filename} ...")
    try:
        with open(filename, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            return reader.fieldnames, list(reader)
    except FileNotFoundError:
        print(f"❌ ไม่พบไฟล์ {filename}")
        return None, None

def _detect_url_columns(headers, rows_data):
    """หาคอลัมน์ที่มี http/https"""
    print("🔎 กำลังสแกนหาคอลัมน์ที่มี Link...")
    url_columns = []
    check_limit = min(len(rows_data), 200)

    for col in headers:
        is_url_col = False
        for i in range(check_limit):
            val = str(rows_data[i].get(col, "")).strip().lower()
            if val.startswith("http://") or val.startswith("https://"):
                is_url_col = True
                break
        if is_url_col:
            url_columns.append(col)
    return url_columns

def _prepare_execution_tasks(rows_data, url_columns, results_map):
    """เตรียม List ของงานที่ต้องทำ"""
    tasks = []
    # เตรียม dict ล่วงหน้าเพื่อกัน key error
    for idx in range(len(rows_data)):
        if idx not in results_map:
            results_map[idx] = {}

    for idx, row in enumerate(rows_data):
        for col in url_columns:
            # ข้ามงานที่เสร็จแล้ว
            if col in results_map.get(idx, {}):
                continue
            url = row.get(col, "")
            tasks.append((idx, col, url))
    return tasks

def _execute_and_track(tasks, results_map, headers, url_columns, rows_data):
    """รัน ThreadPool และแสดงผล (แยกออกมาเพื่อลด Complexity ของ Main)"""
    total_tasks = len(tasks)
    completed_in_session = 0
    start_time = time.time()

    print("🚀 กำลังประมวลผล...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(process_url_task, r_idx, col, u): (r_idx, col) 
            for r_idx, col, u in tasks
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_task), 1):
            row_idx, col_name, pea_no, status, method = future.result()
            
            if row_idx not in results_map:
                results_map[row_idx] = {}
                
            results_map[row_idx][col_name] = {
                "pea_no": pea_no,
                "status": status,
                "method": method
            }
            
            completed_in_session += 1
            # คำนวณความเร็ว
            elapsed = time.time() - start_time
            avg_time = elapsed / completed_in_session if completed_in_session > 0 else 0.1
            eta = avg_time * (total_tasks - completed_in_session)
            
            # Print Progress
            if i % 10 == 0 or i == total_tasks:
                speed_txt = f"{1/avg_time:.1f}" if avg_time > 0 else "N/A"
                print(f"⏳ [{i}/{total_tasks}] Speed: {speed_txt} img/s | ETA: {format_time(eta)} | Last: {status}")

            # Auto Save
            if i % SAVE_INTERVAL == 0:
                save_cache(results_map)
                save_output_csv(OUTPUT_FILE, headers, url_columns, rows_data, results_map)
                print(f"✅ Auto-Saved ({completed_in_session} done)")

    return start_time # คืนค่าเวลาเริ่มเพื่อใช้คำนวณสรุป

# --- 🏁 Main Entry Point ---
def main():
    print("="*60)
    print("🚀 เริ่มต้นโปรแกรม (Auto-Detect Link Columns)")
    print("="*60)
    
    # 1. Load Data
    headers, rows_data = _load_input_csv(INPUT_FILE)
    if not headers:
        return

    # 2. Detect Columns
    url_columns = _detect_url_columns(headers, rows_data)
    if not url_columns:
        print("❌ ไม่พบคอลัมน์ที่มี Link (http/https) เลย")
        return
    print(f"✅ พบคอลัมน์ Link ทั้งหมด {len(url_columns)} คอลัมน์: {url_columns}")
    
    # 3. Load Cache & Prepare Tasks
    results_map = load_cache()
    tasks = _prepare_execution_tasks(rows_data, url_columns, results_map)
    
    total_tasks = len(tasks)
    print(f"📌 จำนวนงานที่ต้องทำ: {total_tasks}")
    
    if total_tasks == 0:
        print("🎉 ไม่มีงานเหลือให้ทำ (เสร็จหมดแล้ว)")
        return

    # 4. Execute Tasks (Logic ส่วนวนลูปถูกแยกออกไปแล้ว)
    start_time = _execute_and_track(tasks, results_map, headers, url_columns, rows_data)

    # 5. Final Save
    print("\n💾 บันทึกรอบสุดท้าย...")
    save_output_csv(OUTPUT_FILE, headers, url_columns, rows_data, results_map)
    
    total_time = time.time() - start_time
    print("="*60)
    print("🎉 เสร็จสมบูรณ์!")
    print(f"⏱️ ใช้เวลาทั้งหมด: {format_time(total_time)}")
    print("="*60)

if __name__ == "__main__":
    main()