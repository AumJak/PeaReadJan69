import time
import requests
import base64

# API URL ของคุณ (Docker ต้องเปิดอยู่นะ)
API_URL = "http://127.0.0.1:5000/predict"

# ใช้รูปอะไรก็ได้ในเครื่องมาเทส (แก้ชื่อไฟล์ด้วย)
IMAGE_PATH = "test_meter.jpg" # <--- ใส่ชื่อไฟล์รูปที่มีอยู่จริงตรงนี้

def benchmark_api():
    # 1. เตรียมข้อมูล (โหลดรูปเป็น Base64 หรือจะส่ง URL ก็ได้แล้วแต่ API รับท่าไหน)
    # สมมติว่า API รับ URL ก็ส่ง dummy url หรือถ้า API รับ base64 ก็แปลงตรงนี้
    # แต่จากโค้ดเก่าคุณรับ URL งั้นเรายิง URL มั่วๆ หรือ URL จริงที่เปิดได้ไวๆ
    payload = {"url": "https://pecom.sgp1.digitaloceanspaces.com/img/product/QAW6310-0040/IPGSM235100/IPGSM235100-1.jpg"} # รูปตัวอย่างบนเน็ตโหลดไวๆ

    print(f"🚀 เริ่มทดสอบยิง API ไปที่ {API_URL} ...")
    
    # Warm up 3 รอบ
    for _ in range(3):
        try: requests.post(API_URL, json=payload, timeout=5)
        except: pass

    # Test จริง 20 รอบ
    start_time = time.time()
    success_count = 0
    rounds = 20
    
    for i in range(rounds):
        try:
            res = requests.post(API_URL, json=payload, timeout=10)
            if res.status_code == 200:
                success_count += 1
        except Exception as e:
            print(f"Request Error: {e}")

    end_time = time.time()
    
    total_time = end_time - start_time
    avg_time = total_time / rounds
    fps = 1 / avg_time

    print("\n" + "="*40)
    print(f"📊 ผลทดสอบความเร็ว API (End-to-End Latency)")
    print("="*40)
    print(f"⏱️  เฉลี่ยต่อ 1 Request: {avg_time:.4f} วินาที")
    print(f"⚡  รองรับได้ประมาณ:     {fps:.2f} รูป/วินาที (RPS)")
    print("="*40)

if __name__ == "__main__":
    benchmark_api()