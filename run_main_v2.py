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
API_URL = "http://127.0.0.1:5000/predict"
INPUT_FILE = "C3.csv"
OUTPUT_FILE = "Sheet_C3_Complete.csv"
CACHE_FILE = "temp_progress.json"
MAX_WORKERS = 5           
SAVE_INTERVAL = 100
RETRY_DELAY = 10  # (วินาที) เวลาหน่วงเมื่อเน็ตหลุดก่อนลองใหม่

# --- 🚀 ส่วนจูนความเร็ว ---
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=MAX_WORKERS,
    pool_maxsize=MAX_WORKERS,
    max_retries=1
)
session.mount('http://', adapter)
session.mount('https://', adapter)

def process_url_task(row_index, col_name, url):
    """ฟังก์ชันยิง API พร้อม Logic Status + กันเน็ตหลุด"""
    
    if not url or not str(url).strip():
        return row_index, col_name, "", "No Image", ""
    
    clean_url = str(url).strip()
    if not clean_url.lower().startswith("http"):
         return row_index, col_name, "", "No Image", "Invalid URL"

    payload = {"url": clean_url}
    
    # 🔥 [จุดแก้ไข] วนลูปยิงจนกว่าจะได้ (กันเน็ตหลุด/Server ดับ)
    while True:
        try:
            # ยิง Request
            response = session.post(API_URL, json=payload, timeout=100)
            
            # ถ้าเชื่อมต่อได้สำเร็จ (ไม่ Error ระดับ Network) ให้หลุด Loop ไปเช็ค Status Code
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    result_data = data.get("data", {})
                    pea_no = result_data.get("serial_number", "")
                    read_method = result_data.get("method", "")
                    
                    method_display = "Barcode" if read_method == "barcode" else ("OCR" if read_method == "ocr" else read_method)
                    return row_index, col_name, pea_no, "Success", method_display
                else:
                    msg = data.get("message", "Unknown")
                    if "download" in msg.lower() or "image" in msg.lower():
                        return row_index, col_name, "", "No Image", msg
                    return row_index, col_name, "", "Failed", msg

            elif response.status_code in [400, 404, 422]:
                return row_index, col_name, "", "No Image", f"API {response.status_code}"
            elif response.status_code >= 500:
                # 500 = Server Error อาจจะลองใหม่ได้ หรือจะให้ผ่านเลยก็ได้ 
                # ในที่นี้ให้ผ่านเลยเพื่อไม่ให้ค้างถาวรถ้าโค้ด Server พัง
                return row_index, col_name, "", "API Error", f"HTTP {response.status_code}"
            else:
                return row_index, col_name, "", "API Error", f"HTTP {response.status_code}"
                
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # 🔥 จับ Error เน็ตหลุด หรือ ต่อ Server ไม่ติด
            print(f"⚠️ [Row {row_index}] Connection Lost/Timeout. Retrying in {RETRY_DELAY}s... ({e})")
            time.sleep(RETRY_DELAY) # หยุดรอก่อนลองใหม่
            continue # กลับไปเริ่ม while loop ใหม่
            
        except Exception as e:
            # Error อื่นๆ ที่ไม่ใช่ Network (เช่น Code พัง) ให้ข้าม
            return row_index, col_name, "", "API Error", str(e)

# --- Utils ---
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                return {int(k): v for k, v in loaded.items()}
        except: return {}
    return {}

def save_cache(data):
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
    except: pass

def format_time(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))

def save_output_csv(filename, headers, url_columns, rows_data, results_map):
    new_headers = headers.copy()
    
    for col in url_columns:
        new_headers.append(f"{col}_PEA")
        new_headers.append(f"{col}_Status")

    try:
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=new_headers)
            writer.writeheader()
            
            for idx, row in enumerate(rows_data):
                new_row = row.copy()
                for col in url_columns:
                    res = results_map.get(idx, {}).get(col, {"pea_no": "", "status": "", "method": ""})
                    new_row[f"{col}_PEA"] = res["pea_no"]
                    
                    status_text = res["status"]
                    if status_text == "Success":
                        status_text = res["method"]
                    
                    new_row[f"{col}_Status"] = status_text
                writer.writerow(new_row)
    except Exception as e:
        print(f"⚠️ บันทึกไฟล์ CSV ไม่สำเร็จ: {e}")

# --- Main ---
def main():
    print(f"📂 กำลังอ่านไฟล์: {INPUT_FILE} ...")
    
    rows_data = []
    headers = []
    
    try:
        with open(INPUT_FILE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows_data = list(reader)
    except FileNotFoundError:
        print(f"❌ ไม่พบไฟล์ {INPUT_FILE}")
        return

    found_urls_set = set()
    for row in rows_data[:20]:
        for col in headers:
            if row[col] and str(row[col]).lower().startswith("http"):
                found_urls_set.add(col)
    
    url_columns = sorted(list(found_urls_set), key=lambda x: (x.split('_')[-1], x))
    
    print(f"🔍 พบคอลัมน์รูปภาพ (เรียงตามกลุ่มเดือน): {url_columns}")

    results_map = load_cache()
    if results_map:
        print(f"♻️ พบข้อมูลเดิม {len(results_map)} แถว! ทำต่อเลย...")

    tasks = []
    skipped_count = 0
    
    for idx, row in enumerate(rows_data):
        for col in url_columns:
            if idx in results_map and col in results_map[idx]:
                skipped_count += 1
                continue
            url = row.get(col, "")
            tasks.append((idx, col, url))

    total_tasks = len(tasks)
    print(f"⚡ งานเหลือ: {total_tasks} รูป (เสร็จแล้ว {skipped_count}) | Workers: {MAX_WORKERS}")
    print("-" * 60)

    start_time = time.time()
    completed_in_session = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {executor.submit(process_url_task, r, c, u): (r, c) for (r, c, u) in tasks}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_task), 1):
            row_idx, col_name, pea_no, status, method = future.result()
            
            if row_idx not in results_map: results_map[row_idx] = {}
            results_map[row_idx][col_name] = {
                "pea_no": pea_no,
                "status": status,
                "method": method
            }
            
            completed_in_session += 1
            
            elapsed = time.time() - start_time
            # ป้องกันการหารด้วย 0 กรณีเสร็จเร็วมาก
            if completed_in_session > 0:
                avg_time = elapsed / completed_in_session
            else:
                avg_time = 0.1
                
            eta = avg_time * (total_tasks - completed_in_session)
            
            if i % 10 == 0 or i == total_tasks:
                speed_txt = f"{1/avg_time:.1f}" if avg_time > 0 else "N/A"
                print(f"⏳ [{i}/{total_tasks}] Speed: {speed_txt} img/s | ETA: {format_time(eta)} | Last: {status}")

            if i % SAVE_INTERVAL == 0:
                save_cache(results_map)
                save_output_csv(OUTPUT_FILE, headers, url_columns, rows_data, results_map)
                print(f"✅ Auto-Saved ({completed_in_session} done)")

    print("\n💾 บันทึกรอบสุดท้าย...")
    save_output_csv(OUTPUT_FILE, headers, url_columns, rows_data, results_map)

    if os.path.exists(CACHE_FILE): os.remove(CACHE_FILE)
    
    total_time = time.time() - start_time
    print("="*60)
    print(f"🎉 เสร็จสมบูรณ์!")
    print(f"⏱️ ใช้เวลา: {format_time(total_time)}")
    print("="*60)

if __name__ == "__main__":
    main()