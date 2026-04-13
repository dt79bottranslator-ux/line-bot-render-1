import os
import json
import html
import hmac
import hashlib
import base64
import time
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional, List

import requests
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, request, jsonify

app = Flask(__name__)

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# =========================================================
# BASIC HELPER NEEDED EARLY
# =========================================================
def safe_str(v) -> str:
    return str(v).strip() if v else ""

# =========================================================
# ENV
# =========================================================
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()

# Giữ env này cho phase sau, KHÔNG đọc Sheet trong hot path /callback
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
PHASE1_SPREADSHEET_NAME = os.getenv("PHASE1_SPREADSHEET_NAME", "DT79_PHASE1_WORKER_CASES_V1").strip()
USER_STATE_SHEET_NAME = "user_state"

ADMIN_IDS = os.getenv("ADMIN_IDS", "").strip()
ADMIN_LIST = [x.strip() for x in ADMIN_IDS.split(",") if x.strip()]

ALL_COOLDOWN_SECONDS = int(os.getenv("ALL_COOLDOWN_SECONDS", "15").strip() or "15")
MAX_ALL_CHARS = int(os.getenv("MAX_ALL_CHARS", "500").strip() or "500")

RUNTIME_STATE_TTL_SECONDS = int(os.getenv("RUNTIME_STATE_TTL_SECONDS", "1800").strip() or "1800")
RUNTIME_STATE_MAX_KEYS = int(os.getenv("RUNTIME_STATE_MAX_KEYS", "5000").strip() or "5000")

DEFAULT_LANGUAGE_GROUP = os.getenv("DEFAULT_LANGUAGE_GROUP", "vi").strip().lower() or "vi"
USER_LANGUAGE_MAP_JSON = os.getenv("USER_LANGUAGE_MAP_JSON", "").strip()

# =========================================================
# CONSTANTS
# =========================================================
APP_VERSION = "PHASE1_RUNTIME_STATE_SAFE__WORKER_ADS_DETAIL_ROUTING_FIXED_V14"
TW_TZ = timezone(timedelta(hours=8))
LOCKED_TARGET_LANG = "zh-TW"

CONNECT_TIMEOUT_SECONDS = int(os.getenv("CONNECT_TIMEOUT_SECONDS", "3").strip() or "3")
READ_TIMEOUT_SECONDS = int(os.getenv("READ_TIMEOUT_SECONDS", "8").strip() or "8")
OUTBOUND_TIMEOUT = (CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS)

FALLBACK_REPLY_TEXT = "Hệ thống bận, thử lại sau."
LINE_TEXT_HARD_LIMIT = 5000
RATE_LIMIT_STORE_MAX_KEYS = 5000
ERROR_BODY_LOG_LIMIT = 800

LINE_REPLY_API_URL = "https://api.line.me/v2/bot/message/reply"
GOOGLE_TRANSLATE_API_URL = "https://translation.googleapis.com/language/translate/v2"

WORKER_ENTRY_COMMAND = "/worker"
ADS_ENTRY_COMMAND = "/ads"
SUPPORTED_LANGUAGE_GROUPS = {"vi", "id", "th"}

ADS_CATALOG_SHEET_NAME = "ads_catalog"
ADS_LIST_LIMIT = int(os.getenv("ADS_LIST_LIMIT", "6").strip() or "6")
ADS_CACHE_TTL_SECONDS = int(os.getenv("ADS_CACHE_TTL_SECONDS", "30").strip() or "30")
ADS_VIEW_TTL_SECONDS = int(os.getenv("ADS_VIEW_TTL_SECONDS", "300").strip() or "300")

ADS_TYPE_JOB_OPENING = "job_opening"
ADS_TYPE_SERVICE_OFFER = "service_offer"

VISIBILITY_SAME_LANGUAGE_ONLY = "same_language_only"
VISIBILITY_CROSS_LANGUAGE_ALLOWED = "cross_language_allowed"
VISIBILITY_VIEWER_LOCALIZED = "viewer_localized"

ACTIVE_AD_STATUSES = {"active"}
SUPPORTED_AD_TYPES = {ADS_TYPE_JOB_OPENING, ADS_TYPE_SERVICE_OFFER}
SUPPORTED_VISIBILITY_POLICIES = {
    VISIBILITY_SAME_LANGUAGE_ONLY,
    VISIBILITY_CROSS_LANGUAGE_ALLOWED,
    VISIBILITY_VIEWER_LOCALIZED,
}

# =========================================================
# PHASE 1 STATE MACHINE
# =========================================================
STATE_IDLE = "idle"
STATE_AWAITING_NEED_TYPE = "awaiting_need_type"
STATE_AWAITING_URGENCY = "awaiting_urgency"
STATE_AWAITING_JOB_TARGET = "awaiting_job_target"
STATE_AWAITING_CV_FORM = "awaiting_cv_form"
STATE_AWAITING_RESIDENCE_CARD = "awaiting_residence_card"
STATE_AWAITING_PHONE_NUMBER = "awaiting_phone_number"
STATE_CASE_COMPLETED = "case_completed"

ALLOWED_STATES = {
    STATE_IDLE,
    STATE_AWAITING_NEED_TYPE,
    STATE_AWAITING_URGENCY,
    STATE_AWAITING_JOB_TARGET,
    STATE_AWAITING_CV_FORM,
    STATE_AWAITING_RESIDENCE_CARD,
    STATE_AWAITING_PHONE_NUMBER,
    STATE_CASE_COMPLETED,
}

NEED_TYPE_V1 = {
    "transfer_job": "transfer_job",
    "part_time": "part_time",
    "taiwan_job": "taiwan_job",
    "overseas_referral": "overseas_referral",
    "passport": "passport",
    "driver_license": "driver_license",
    "airport_taxi": "airport_taxi",
    "motorcycle": "motorcycle",
    "sim_card": "sim_card",
    "device_trade": "device_trade",
    "other_service": "other_service",
    "chinese_course": "chinese_course",
    "driver_license_course": "driver_license_course",
    "forklift_course": "forklift_course",
    "weekend_tour": "weekend_tour",
    "room_rental": "room_rental",
    # legacy hidden compatibility
    "arc": "arc",
}

NEED_TYPE_MENU_ITEMS = [
    "transfer_job",
    "part_time",
    "taiwan_job",
    "overseas_referral",
    "passport",
    "driver_license",
    "airport_taxi",
    "motorcycle",
    "sim_card",
    "device_trade",
    "other_service",
    "chinese_course",
    "driver_license_course",
    "forklift_course",
    "weekend_tour",
    "room_rental",
]

NEED_TYPE_ALIAS_MAP = {
    "1": "transfer_job",
    "2": "part_time",
    "3": "taiwan_job",
    "4": "overseas_referral",
    "5": "passport",
    "6": "driver_license",
    "7": "airport_taxi",
    "8": "motorcycle",
    "9": "sim_card",
    "10": "device_trade",
    "11": "other_service",
    "12": "chinese_course",
    "13": "driver_license_course",
    "14": "forklift_course",
    "15": "weekend_tour",
    "16": "room_rental",
    "transfer_job": "transfer_job",
    "part_time": "part_time",
    "taiwan_job": "taiwan_job",
    "overseas_referral": "overseas_referral",
    "passport": "passport",
    "driver_license": "driver_license",
    "airport_taxi": "airport_taxi",
    "motorcycle": "motorcycle",
    "sim_card": "sim_card",
    "device_trade": "device_trade",
    "other_service": "other_service",
    "chinese_course": "chinese_course",
    "driver_license_course": "driver_license_course",
    "forklift_course": "forklift_course",
    "weekend_tour": "weekend_tour",
    "room_rental": "room_rental",
    # legacy hidden compatibility
    "arc": "arc",
}

URGENCY_LEVEL_V1 = {
    "urgent": "urgent",
    "soon": "soon",
    "normal": "normal",
}

URGENCY_ALIAS_MAP = {
    "1": "urgent",
    "2": "soon",
    "3": "normal",
    "urgent": "urgent",
    "soon": "soon",
    "normal": "normal",
}

JOB_TARGET_V1 = {
    "job_for_overseas": "job_for_overseas",
    "job_in_taiwan": "job_in_taiwan",
}

DIRECT_ARC_NEED_TYPES = {
    "transfer_job",
    "part_time",
}

# =========================================================
# I18N DICTIONARY
# =========================================================
I18N = {
    "vi": {
        "worker_need_title": "📋 YÊU CẦU HỖ TRỢ",
        "worker_need_intro": "Vui lòng chọn 1 nhu cầu bằng cách gửi đúng mã hoặc số dưới đây:",
        "need.transfer_job": "Chuyển chủ / đổi việc",
        "need.part_time": "Việc làm thêm",
        "need.taiwan_job": "Đơn hàng tại Đài Loan",
        "need.overseas_referral": "Có bạn/người nhà ở nước ngoài muốn sang Đài làm việc",
        "need.passport": "Đổi Hộ chiếu",
        "need.arc": "Thẻ cư trú / ARC",
        "need.driver_license": "Đổi Bằng lái xe",
        "need.airport_taxi": "Dịch vụ Taxi / sân bay",
        "need.motorcycle": "Bán Xe máy",
        "need.other_service": "Dịch vụ khác",
        "need.sim_card": "Sim thẻ lên mạng",
        "need.device_trade": "Mua bán điện thoại + laptop",
        "need.chinese_course": "Học tiếng Trung giao tiếp",
        "need.driver_license_course": "Học thi bằng lái xe",
        "need.forklift_course": "Học thi lái xe nâng",
        "need.weekend_tour": "Tour du lịch cuối tuần",
        "need.room_rental": "Thuê phòng trọ",
        "example_send": "Ví dụ gửi:",
        "urgency_title": "⏱️ MỨC ĐỘ GẤP",
        "urgency_intro": "Vui lòng chọn 1 mức độ bằng cách gửi đúng mã hoặc số dưới đây:",
        "urgency.urgent": "Cần gấp hôm nay",
        "urgency.soon": "Trong vài ngày",
        "urgency.normal": "Chưa gấp",
        "job_target_title": "🧭 LOẠI NHU CẦU TÌM VIỆC",
        "job_target_intro": "Vui lòng chọn đúng 1 mã dưới đây:",
        "job_target.job_for_overseas": "Tìm đơn cho người ở nhà sang Đài Loan làm việc",
        "job_target.job_in_taiwan": "Tìm việc cho người hiện đang ở Đài Loan",
        "request_arc_title": "🪪 VUI LÒNG GỬI ẢNH THẺ CƯ TRÚ / ARC",
        "request_arc_body": "Hãy gửi ảnh rõ mặt trước của thẻ cư trú.",
        "request_arc_note_1": "- Ảnh rõ nét",
        "request_arc_note_2": "- Không che thông tin",
        "request_arc_note_3": "- Không chụp quá mờ",
        "request_cv_title": "📄 VUI LÒNG GỬI FORM / CV SƠ YẾU LÝ LỊCH",
        "request_cv_intro": "Chỉ cần các thông tin chính sau:",
        "cv.full_name": "- Họ tên",
        "cv.height": "- Chiều cao",
        "cv.weight": "- Cân nặng",
        "cv.hometown": "- Quê quán",
        "cv.education": "- Trình độ học vấn",
        "cv.marital_status": "- Tình trạng kết hôn",
        "cv.siblings_count": "- Số anh chị em",
        "cv.birth_order": "- Xếp thứ mấy trong gia đình",
        "cv.phone_overseas": "- Số điện thoại của người ứng tuyển",
        "cv.job_preference": "- Nguyện vọng đơn hàng (vị trí nhà máy, ngành nghề, tăng ca nhiều hay ít)",
        "cv.work_exp_vn": "- Kinh nghiệm làm việc tại Việt Nam",
        "cv.send_hint": "Có thể gửi ảnh form hoặc nội dung text.",
        "invalid_need_type": "❌ Mã nhu cầu không hợp lệ.\nVui lòng gửi đúng 1 mã trong danh sách dưới đây:",
        "invalid_urgency": "❌ Mã mức độ gấp không hợp lệ.\nVui lòng gửi đúng 1 mã hoặc số:",
        "invalid_job_target": "❌ Mã loại nhu cầu tìm việc không hợp lệ.\nVui lòng gửi đúng 1 mã:",
        "awaiting_cv_form_text": "Hệ thống đang chờ form/CV. Anh/chị có thể gửi ảnh form hoặc nội dung text theo mẫu đã hướng dẫn.",
        "awaiting_arc_text": "Hệ thống đang chờ ảnh thẻ cư trú / ARC. Vui lòng gửi ảnh rõ mặt trước.",
        "cv_form_image_received": "✅ Đã nhận tín hiệu form/CV.\nBước lưu form vào hệ thống sẽ được bật ở pha kế tiếp.\nHiện tại anh/chị chờ hướng dẫn tiếp theo.",
        "arc_image_received": "✅ Đã nhận tín hiệu ảnh thẻ cư trú.\nBước lưu ảnh vào hệ thống sẽ được bật ở pha kế tiếp.\nHiện tại anh/chị chờ hướng dẫn tiếp theo.",
        "generic_image_placeholder": "Bước này chưa bật nhận ảnh hoàn chỉnh. Hiện tại hãy dùng /worker rồi làm theo từng bước.",
        "resume_flow_prompt": "⚠️ Hệ thống chưa ở đúng bước hiện tại.\nVui lòng bắt đầu lại bằng /worker rồi làm theo từng bước.",
        "not_authorized": "❌ Không có quyền",
        "all_need_content": "⚠️ !all cần nội dung",
        "all_too_long": "⚠️ !all tối đa {max_chars} ký tự",
        "all_cooldown": "⏳ Vui lòng chờ {remaining} giây rồi dùng !all lại",
        "all_broadcast_prefix": "📢 THÔNG BÁO:\n",
    },
    "id": {
        "worker_need_title": "📋 PERMINTAAN BANTUAN",
        "worker_need_intro": "Silakan pilih 1 kebutuhan dengan mengirim kode atau nomor yang benar di bawah ini:",
        "need.transfer_job": "Pindah majikan / ganti pekerjaan",
        "need.part_time": "Kerja paruh waktu",
        "need.taiwan_job": "Lowongan kerja di Taiwan",
        "need.overseas_referral": "Ada teman / keluarga yang mencari lowongan",
        "need.passport": "Ganti paspor",
        "need.arc": "Kartu ARC / izin tinggal",
        "need.driver_license": "Ganti SIM",
        "need.airport_taxi": "Layanan taksi / bandara",
        "need.motorcycle": "Jual sepeda motor",
        "need.other_service": "Layanan lainnya",
        "need.sim_card": "Kartu SIM internet",
        "need.device_trade": "Jual beli HP + laptop",
        "need.chinese_course": "Belajar Mandarin komunikasi",
        "need.driver_license_course": "Belajar ujian SIM",
        "need.forklift_course": "Belajar ujian forklift",
        "need.weekend_tour": "Tur akhir pekan",
        "need.room_rental": "Sewa kamar",
        "example_send": "Contoh kirim:",
        "urgency_title": "⏱️ TINGKAT KEPENTINGAN",
        "urgency_intro": "Silakan pilih 1 tingkat dengan mengirim kode atau angka yang benar di bawah ini:",
        "urgency.urgent": "Perlu segera hari ini",
        "urgency.soon": "Dalam beberapa hari",
        "urgency.normal": "Belum mendesak",
        "job_target_title": "🧭 JENIS KEBUTUHAN CARI KERJA",
        "job_target_intro": "Silakan pilih 1 kode yang benar di bawah ini:",
        "job_target.job_for_overseas": "Cari lowongan untuk keluarga/teman di negara asal agar bisa bekerja di Taiwan",
        "job_target.job_in_taiwan": "Cari pekerjaan untuk orang yang saat ini sudah berada di Taiwan",
        "request_arc_title": "🪪 SILAKAN KIRIM FOTO KARTU ARC / IZIN TINGGAL",
        "request_arc_body": "Silakan kirim foto sisi depan kartu ARC yang jelas.",
        "request_arc_note_1": "- Foto harus jelas",
        "request_arc_note_2": "- Jangan menutupi informasi",
        "request_arc_note_3": "- Jangan terlalu buram",
        "request_cv_title": "📄 SILAKAN KIRIM FORM / CV RIWAYAT SINGKAT",
        "request_cv_intro": "Cukup isi informasi utama berikut:",
        "cv.full_name": "- Nama lengkap",
        "cv.height": "- Tinggi badan",
        "cv.weight": "- Berat badan",
        "cv.hometown": "- Asal daerah",
        "cv.education": "- Pendidikan",
        "cv.marital_status": "- Status pernikahan",
        "cv.siblings_count": "- Jumlah saudara",
        "cv.birth_order": "- Anak ke berapa",
        "cv.phone_overseas": "- Nomor telepon pelamar",
        "cv.job_preference": "- Preferensi lowongan (posisi pabrik, bidang kerja, lembur banyak atau sedikit)",
        "cv.work_exp_vn": "- Pengalaman kerja di Vietnam / negara asal",
        "cv.send_hint": "Bisa kirim foto form atau isi dalam bentuk teks.",
        "invalid_need_type": "❌ Kode kebutuhan tidak valid.\nSilakan kirim 1 kode yang benar dari daftar berikut:",
        "invalid_urgency": "❌ Kode tingkat kepentingan tidak valid.\nSilakan kirim 1 kode atau angka yang benar:",
        "invalid_job_target": "❌ Kode jenis kebutuhan kerja tidak valid.\nSilakan kirim 1 kode yang benar:",
        "awaiting_cv_form_text": "Sistem sedang menunggu form/CV. Anda bisa mengirim foto form atau isi teks sesuai panduan.",
        "awaiting_arc_text": "Sistem sedang menunggu foto kartu ARC / izin tinggal. Silakan kirim foto sisi depan yang jelas.",
        "cv_form_image_received": "✅ Sinyal form/CV sudah diterima.\nLangkah penyimpanan form akan diaktifkan di fase berikutnya.\nSaat ini silakan tunggu instruksi berikutnya.",
        "arc_image_received": "✅ Sinyal foto kartu ARC sudah diterima.\nLangkah penyimpanan gambar akan diaktifkan di fase berikutnya.\nSaat ini silakan tunggu instruksi berikutnya.",
        "generic_image_placeholder": "Fitur penerimaan gambar penuh belum diaktifkan. Saat ini silakan gunakan /worker lalu ikuti langkah demi langkah.",
        "resume_flow_prompt": "⚠️ Sistem belum berada di langkah yang benar.\nSilakan mulai lagi dengan /worker lalu ikuti langkah demi langkah.",
        "not_authorized": "❌ Tidak punya izin",
        "all_need_content": "⚠️ !all harus ada isi",
        "all_too_long": "⚠️ !all maksimal {max_chars} karakter",
        "all_cooldown": "⏳ Tunggu {remaining} detik lalu gunakan !all lagi",
        "all_broadcast_prefix": "📢 PENGUMUMAN:\n",
    },
    "th": {
        "worker_need_title": "📋 คำขอความช่วยเหลือ",
        "worker_need_intro": "กรุณาเลือก 1 ความต้องการโดยส่งรหัสหรือหมายเลขที่ถูกต้องด้านล่าง:",
        "need.transfer_job": "ย้ายนายจ้าง / เปลี่ยนงาน",
        "need.part_time": "งานพาร์ตไทม์",
        "need.taiwan_job": "งานในไต้หวัน",
        "need.overseas_referral": "มีเพื่อนหรือคนในครอบครัวหางาน",
        "need.passport": "เปลี่ยนหนังสือเดินทาง",
        "need.arc": "บัตร ARC / บัตรพำนัก",
        "need.driver_license": "เปลี่ยนใบขับขี่",
        "need.airport_taxi": "บริการแท็กซี่ / สนามบิน",
        "need.motorcycle": "ขายมอเตอร์ไซค์",
        "need.other_service": "บริการอื่น ๆ",
        "need.sim_card": "ซิมการ์ดอินเทอร์เน็ต",
        "need.device_trade": "ซื้อขายโทรศัพท์ + แล็ปท็อป",
        "need.chinese_course": "เรียนภาษาจีนเพื่อสื่อสาร",
        "need.driver_license_course": "เรียนสอบใบขับขี่",
        "need.forklift_course": "เรียนสอบรถยก",
        "need.weekend_tour": "ทัวร์สุดสัปดาห์",
        "need.room_rental": "เช่าห้องพัก",
        "example_send": "ตัวอย่างส่ง:",
        "urgency_title": "⏱️ ระดับความเร่งด่วน",
        "urgency_intro": "กรุณาเลือก 1 ระดับโดยส่งรหัสหรือหมายเลขที่ถูกต้องด้านล่าง:",
        "urgency.urgent": "ต้องการด่วนวันนี้",
        "urgency.soon": "ภายในไม่กี่วัน",
        "urgency.normal": "ยังไม่ด่วน",
        "job_target_title": "🧭 ประเภทความต้องการหางาน",
        "job_target_intro": "กรุณาเลือก 1 รหัสที่ถูกต้องด้านล่าง:",
        "job_target.job_for_overseas": "หางานให้คนที่อยู่ประเทศต้นทางเพื่อมาทำงานที่ไต้หวัน",
        "job_target.job_in_taiwan": "หางานให้คนที่อยู่ไต้หวันอยู่แล้ว",
        "request_arc_title": "🪪 กรุณาส่งรูปบัตร ARC / บัตรพำนัก",
        "request_arc_body": "กรุณาส่งรูปด้านหน้าของบัตรพำนักที่ชัดเจน",
        "request_arc_note_1": "- รูปต้องชัด",
        "request_arc_note_2": "- ห้ามปิดบังข้อมูล",
        "request_arc_note_3": "- ห้ามเบลอเกินไป",
        "request_cv_title": "📄 กรุณาส่งแบบฟอร์ม / CV ประวัติย่อ",
        "request_cv_intro": "ใช้ข้อมูลหลักดังต่อไปนี้:",
        "cv.full_name": "- ชื่อ-นามสกุล",
        "cv.height": "- ส่วนสูง",
        "cv.weight": "- น้ำหนัก",
        "cv.hometown": "- ภูมิลำเนา",
        "cv.education": "- ระดับการศึกษา",
        "cv.marital_status": "- สถานภาพสมรส",
        "cv.siblings_count": "- จำนวนพี่น้อง",
        "cv.birth_order": "- เป็นลูกคนที่เท่าไร",
        "cv.phone_overseas": "- หมายเลขโทรศัพท์ของผู้สมัคร",
        "cv.job_preference": "- ความต้องการงาน (ตำแหน่งโรงงาน, สายงาน, ล่วงเวลามากหรือน้อย)",
        "cv.work_exp_vn": "- ประสบการณ์ทำงานในเวียดนาม / ประเทศต้นทาง",
        "cv.send_hint": "สามารถส่งเป็นรูปแบบฟอร์มหรือข้อความก็ได้",
        "invalid_need_type": "❌ รหัสความต้องการไม่ถูกต้อง\nกรุณาส่ง 1 รหัสที่ถูกต้องจากรายการด้านล่าง:",
        "invalid_urgency": "❌ รหัสระดับความเร่งด่วนไม่ถูกต้อง\nกรุณาส่ง 1 รหัสหรือหมายเลขที่ถูกต้อง:",
        "invalid_job_target": "❌ รหัสประเภทความต้องการหางานไม่ถูกต้อง\nกรุณาส่ง 1 รหัสที่ถูกต้อง:",
        "awaiting_cv_form_text": "ระบบกำลังรอ form/CV คุณสามารถส่งรูปแบบฟอร์มหรือพิมพ์ข้อความตามตัวอย่างได้",
        "awaiting_arc_text": "ระบบกำลังรอรูปบัตร ARC / บัตรพำนัก กรุณาส่งรูปด้านหน้าที่ชัดเจน",
        "cv_form_image_received": "✅ ได้รับสัญญาณ form/CV แล้ว\nขั้นตอนบันทึกฟอร์มจะเปิดในเฟสถัดไป\nขณะนี้กรุณารอคำแนะนำต่อไป",
        "arc_image_received": "✅ ได้รับสัญญาณรูปบัตรพำนักแล้ว\nขั้นตอนบันทึกรูปจะเปิดในเฟสถัดไป\nขณะนี้กรุณารอคำแนะนำต่อไป",
        "generic_image_placeholder": "ขั้นตอนนี้ยังไม่เปิดรับรูปแบบสมบูรณ์ ขณะนี้กรุณาใช้ /worker แล้วทำตามทีละขั้นตอน",
        "resume_flow_prompt": "⚠️ ระบบยังไม่อยู่ในขั้นตอนที่ถูกต้อง\nกรุณาเริ่มใหม่ด้วย /worker แล้วทำตามทีละขั้นตอน",
        "not_authorized": "❌ ไม่มีสิทธิ์",
        "all_need_content": "⚠️ !all ต้องมีเนื้อหา",
        "all_too_long": "⚠️ !all ได้สูงสุด {max_chars} ตัวอักษร",
        "all_cooldown": "⏳ กรุณารอ {remaining} วินาทีแล้วใช้ !all อีกครั้ง",
        "all_broadcast_prefix": "📢 ประกาศ:\n",
    },
}

# =========================================================
# IN-MEMORY STORES
# =========================================================
LAST_ALL_USED_AT: Dict[str, int] = {}
RUNTIME_USER_STATE: Dict[str, Dict[str, str]] = {}
USER_LANGUAGE_MAP: Dict[str, str] = {}

# =========================================================
# STARTUP / VALIDATION
# =========================================================
def load_user_language_map() -> Dict[str, str]:
    if not USER_LANGUAGE_MAP_JSON:
        return {}

    try:
        raw = json.loads(USER_LANGUAGE_MAP_JSON)
        if not isinstance(raw, dict):
            logger.warning("[STARTUP] USER_LANGUAGE_MAP_JSON is not dict")
            return {}

        cleaned = {}
        for k, v in raw.items():
            user_id = safe_str(k)
            language_group = safe_str(v).lower()
            if not user_id:
                continue
            cleaned[user_id] = language_group
        return cleaned
    except Exception as e:
        logger.warning(f"[STARTUP] Failed to parse USER_LANGUAGE_MAP_JSON: {type(e).__name__}:{e}")
        return {}


def normalize_language_group(value: str) -> str:
    lang = safe_str(value).lower()
    if lang in SUPPORTED_LANGUAGE_GROUPS:
        return lang
    fallback = DEFAULT_LANGUAGE_GROUP if DEFAULT_LANGUAGE_GROUP in SUPPORTED_LANGUAGE_GROUPS else "vi"
    return fallback


def is_runtime_ready() -> bool:
    return all([
        bool(LINE_CHANNEL_ACCESS_TOKEN),
        bool(LINE_CHANNEL_SECRET),
        bool(GOOGLE_API_KEY),
    ])


def validate_startup_config() -> None:
    missing_required = []
    missing_optional = []

    if not LINE_CHANNEL_ACCESS_TOKEN:
        missing_required.append("LINE_CHANNEL_ACCESS_TOKEN")
    if not LINE_CHANNEL_SECRET:
        missing_required.append("LINE_CHANNEL_SECRET")
    if not GOOGLE_API_KEY:
        missing_required.append("GOOGLE_API_KEY")

    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        missing_optional.append("GOOGLE_SERVICE_ACCOUNT_JSON")

    if DEFAULT_LANGUAGE_GROUP not in SUPPORTED_LANGUAGE_GROUPS:
        logger.warning(f"[STARTUP] Invalid DEFAULT_LANGUAGE_GROUP={DEFAULT_LANGUAGE_GROUP}, fallback to vi")

    if missing_required:
        logger.warning(f"[STARTUP] Missing required env: {', '.join(missing_required)}")
    else:
        logger.info("[STARTUP] Required env loaded")

    if missing_optional:
        logger.warning(f"[STARTUP] Missing optional env: {', '.join(missing_optional)}")
    else:
        logger.info("[STARTUP] Optional sheet env loaded")

    logger.info(
        f"[STARTUP] app_version={APP_VERSION} "
        f"admin_count={len(ADMIN_LIST)} "
        f"all_cooldown_seconds={ALL_COOLDOWN_SECONDS} "
        f"max_all_chars={MAX_ALL_CHARS} "
        f"connect_timeout_s={CONNECT_TIMEOUT_SECONDS} "
        f"read_timeout_s={READ_TIMEOUT_SECONDS} "
        f"phase1_spreadsheet_name={PHASE1_SPREADSHEET_NAME} "
        f"runtime_state_ttl_s={RUNTIME_STATE_TTL_SECONDS} "
        f"runtime_state_max_keys={RUNTIME_STATE_MAX_KEYS} "
        f"sheet_env_ready={bool(GOOGLE_SERVICE_ACCOUNT_JSON)} "
        f"default_language_group={normalize_language_group(DEFAULT_LANGUAGE_GROUP)} "
        f"user_language_map_size={len(USER_LANGUAGE_MAP)}"
    )


USER_LANGUAGE_MAP = load_user_language_map()
validate_startup_config()

# =========================================================
# BASIC HELPERS
# =========================================================
def now_tw_iso() -> str:
    return datetime.now(TW_TZ).isoformat()


def now_tw_dt() -> datetime:
    return datetime.now(TW_TZ)


def make_trace_id() -> str:
    return f"trc_{uuid.uuid4().hex[:12]}"


def crop_text(text: str, max_len: int = LINE_TEXT_HARD_LIMIT) -> str:
    text = safe_str(text)
    if len(text) <= max_len:
        return text
    return text[:max_len]


def truncate_log_text(text: str, max_len: int = ERROR_BODY_LOG_LIMIT) -> str:
    text = safe_str(text)
    if len(text) <= max_len:
        return text
    return text[:max_len] + "...<truncated>"


def is_admin(user_id: str) -> bool:
    return user_id in ADMIN_LIST


def extract_source_ids(event: dict) -> Tuple[str, str, str, str]:
    s = event.get("source", {})
    return (
        safe_str(s.get("type")),
        safe_str(s.get("userId")),
        safe_str(s.get("groupId")),
        safe_str(s.get("roomId")),
    )


def get_now_ts() -> int:
    return int(time.time())


def ms_since(start_perf: float) -> int:
    return int((time.perf_counter() - start_perf) * 1000)


def get_scope_key(source_type: str, user_id: str, group_id: str, room_id: str) -> str:
    if source_type == "group" and group_id:
        return group_id
    if source_type == "room" and room_id:
        return room_id
    return user_id or "unknown"


def get_all_rate_limit_key(user_id: str, scope_key: str) -> str:
    return f"{user_id}:{scope_key}"


def get_runtime_state_key(user_id: str, scope_key: str) -> str:
    return f"{safe_str(user_id)}:{safe_str(scope_key)}"


def normalize_state_value(value: str) -> str:
    state = safe_str(value)
    if state in ALLOWED_STATES:
        return state
    return STATE_IDLE


def resolve_user_language_group(user_id: str) -> str:
    return normalize_language_group(USER_LANGUAGE_MAP.get(user_id, DEFAULT_LANGUAGE_GROUP))


def i18n_text(language_group: str, key: str, **kwargs) -> str:
    lang = normalize_language_group(language_group)
    value = I18N.get(lang, I18N["vi"]).get(key, I18N["vi"].get(key, key))
    if kwargs:
        try:
            return value.format(**kwargs)
        except Exception:
            return value
    return value


def normalize_urgency_input(input_text: str) -> str:
    raw = safe_str(input_text).lower()
    return URGENCY_ALIAS_MAP.get(raw, raw)

# =========================================================
# RATE LIMIT HELPERS
# =========================================================
def cleanup_rate_limit_store(now_ts: int) -> None:
    if len(LAST_ALL_USED_AT) < RATE_LIMIT_STORE_MAX_KEYS:
        return

    expired_before = now_ts - max(ALL_COOLDOWN_SECONDS * 3, 60)
    stale_keys = [k for k, v in LAST_ALL_USED_AT.items() if v < expired_before]

    for k in stale_keys:
        LAST_ALL_USED_AT.pop(k, None)

    logger.info(
        f"[RATE_LIMIT] cleanup removed={len(stale_keys)} "
        f"remaining={len(LAST_ALL_USED_AT)}"
    )


def allow_all_command(user_id: str, scope_key: str) -> Tuple[bool, int]:
    now_ts = get_now_ts()
    cleanup_rate_limit_store(now_ts)

    rate_key = get_all_rate_limit_key(user_id, scope_key)
    last_ts = LAST_ALL_USED_AT.get(rate_key, 0)
    diff = now_ts - last_ts

    if diff < ALL_COOLDOWN_SECONDS:
        return False, ALL_COOLDOWN_SECONDS - diff

    LAST_ALL_USED_AT[rate_key] = now_ts
    return True, 0

# =========================================================
# RUNTIME STATE HELPERS - HOT PATH SAFE
# =========================================================
def make_runtime_state(user_id: str, scope_key: str) -> dict:
    return {
        "user_id": safe_str(user_id),
        "scope_key": safe_str(scope_key),
        "current_state": STATE_IDLE,
        "temp_need_type": "",
        "temp_urgency_level": "",
        "temp_job_target": "",
        "temp_residence_card_image_url": "",
        "updated_at": now_tw_iso(),
        "last_seen_ts": get_now_ts(),
    }


def cleanup_runtime_state_store(now_ts: int) -> None:
    expired_before = now_ts - max(RUNTIME_STATE_TTL_SECONDS, 300)

    stale_keys = [
        k for k, v in RUNTIME_USER_STATE.items()
        if int(v.get("last_seen_ts", 0) or 0) < expired_before
    ]
    for k in stale_keys:
        RUNTIME_USER_STATE.pop(k, None)

    if len(RUNTIME_USER_STATE) > RUNTIME_STATE_MAX_KEYS:
        sorted_items = sorted(
            RUNTIME_USER_STATE.items(),
            key=lambda item: int(item[1].get("last_seen_ts", 0) or 0)
        )
        overflow = len(RUNTIME_USER_STATE) - RUNTIME_STATE_MAX_KEYS
        for k, _v in sorted_items[:overflow]:
            RUNTIME_USER_STATE.pop(k, None)

    if stale_keys:
        logger.info(
            f"[RUNTIME_STATE] cleanup removed={len(stale_keys)} "
            f"remaining={len(RUNTIME_USER_STATE)}"
        )


def get_runtime_state(user_id: str, scope_key: str, trace_id: str) -> dict:
    now_ts = get_now_ts()
    cleanup_runtime_state_store(now_ts)

    state_key = get_runtime_state_key(user_id, scope_key)
    state = RUNTIME_USER_STATE.get(state_key)

    if not state:
        state = make_runtime_state(user_id, scope_key)
        RUNTIME_USER_STATE[state_key] = state
        logger.info(
            f"[{trace_id}] RUNTIME_STATE_GET status=NEW "
            f"user_id={user_id} scope_key={scope_key} "
            f"current_state={state['current_state']}"
        )
        return state

    state["last_seen_ts"] = now_ts
    state["updated_at"] = now_tw_iso()
    logger.info(
        f"[{trace_id}] RUNTIME_STATE_GET status=FOUND "
        f"user_id={user_id} scope_key={scope_key} "
        f"current_state={state['current_state']}"
    )
    return state


def set_runtime_state(
    user_id: str,
    scope_key: str,
    current_state: str,
    temp_need_type: str = "",
    temp_urgency_level: str = "",
    temp_job_target: str = "",
    temp_residence_card_image_url: str = "",
    trace_id: str = "",
) -> dict:
    now_ts = get_now_ts()
    cleanup_runtime_state_store(now_ts)

    state_key = get_runtime_state_key(user_id, scope_key)
    state = {
        "user_id": safe_str(user_id),
        "scope_key": safe_str(scope_key),
        "current_state": normalize_state_value(current_state),
        "temp_need_type": safe_str(temp_need_type),
        "temp_urgency_level": safe_str(temp_urgency_level),
        "temp_job_target": safe_str(temp_job_target),
        "temp_residence_card_image_url": safe_str(temp_residence_card_image_url),
        "updated_at": now_tw_iso(),
        "last_seen_ts": now_ts,
    }
    RUNTIME_USER_STATE[state_key] = state

    if trace_id:
        logger.info(
            f"[{trace_id}] RUNTIME_STATE_SET "
            f"user_id={user_id} scope_key={scope_key} "
            f"current_state={state['current_state']} "
            f"temp_need_type={state['temp_need_type']} "
            f"temp_urgency_level={state['temp_urgency_level']} "
            f"temp_job_target={state['temp_job_target']}"
        )
    return state

# =========================================================
# OUTBOUND HTTP HELPERS
# =========================================================
def post_json(url: str, headers: dict, payload: dict, trace_id: str, op_name: str):
    started = time.perf_counter()

    try:
        response = requests.post(
            url=url,
            headers=headers,
            json=payload,
            timeout=OUTBOUND_TIMEOUT
        )
        latency_ms = ms_since(started)
        logger.info(f"[{trace_id}] {op_name} status={response.status_code} latency_ms={latency_ms}")

        if response.status_code != 200:
            logger.error(
                f"[{trace_id}] {op_name} body={truncate_log_text(response.text)} url={url}"
            )
        return response, latency_ms

    except requests.Timeout as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} timeout_exception={type(e).__name__} latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

    except requests.RequestException as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} request_exception={type(e).__name__} latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

    except Exception as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} exception={type(e).__name__}:{e} latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms


def post_form(url: str, params: dict, data: dict, trace_id: str, op_name: str):
    started = time.perf_counter()

    try:
        response = requests.post(
            url=url,
            params=params,
            data=data,
            timeout=OUTBOUND_TIMEOUT
        )
        latency_ms = ms_since(started)
        logger.info(f"[{trace_id}] {op_name} status={response.status_code} latency_ms={latency_ms}")

        if response.status_code != 200:
            logger.error(
                f"[{trace_id}] {op_name} body={truncate_log_text(response.text)} url={url}"
            )
        return response, latency_ms

    except requests.Timeout as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} timeout_exception={type(e).__name__} latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

    except requests.RequestException as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} request_exception={type(e).__name__} latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

    except Exception as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} exception={type(e).__name__}:{e} latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

# =========================================================
# LINE HELPERS
# =========================================================
def verify_line_signature(secret: str, body: bytes, signature: str) -> bool:
    if not secret or not signature:
        return False

    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(computed, signature)


def line_reply(reply_token: str, text: str, trace_id: str) -> Tuple[bool, int]:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error(f"[{trace_id}] LINE_REPLY skipped reason=missing_channel_access_token")
        return False, 0

    if not reply_token:
        logger.error(f"[{trace_id}] LINE_REPLY skipped reason=missing_reply_token")
        return False, 0

    final_text = crop_text(text or FALLBACK_REPLY_TEXT)
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": final_text
            }
        ]
    }

    response, latency_ms = post_json(
        url=LINE_REPLY_API_URL,
        headers=headers,
        payload=payload,
        trace_id=trace_id,
        op_name="LINE_REPLY"
    )

    if not response:
        return False, latency_ms

    return response.status_code == 200, latency_ms

# =========================================================
# GOOGLE TRANSLATE HELPERS
# =========================================================
def translate_auto_source(text: str, target: str, trace_id: str) -> Tuple[Optional[str], int, Optional[str]]:
    if not GOOGLE_API_KEY:
        logger.error(f"[{trace_id}] GOOGLE_TRANSLATE skipped reason=missing_google_api_key")
        return None, 0, None

    data = {
        "q": text,
        "target": target,
        "format": "text"
    }

    response, latency_ms = post_form(
        url=GOOGLE_TRANSLATE_API_URL,
        params={"key": GOOGLE_API_KEY},
        data=data,
        trace_id=trace_id,
        op_name="GOOGLE_TRANSLATE"
    )

    if not response:
        return None, latency_ms, None

    if response.status_code != 200:
        return None, latency_ms, None

    try:
        payload = response.json()
    except Exception as e:
        logger.exception(f"[{trace_id}] GOOGLE_TRANSLATE json_parse_exception={type(e).__name__}:{e}")
        return None, latency_ms, None

    translations = payload.get("data", {}).get("translations", [])
    if not translations:
        logger.error(f"[{trace_id}] GOOGLE_TRANSLATE empty_translations")
        return None, latency_ms, None

    first = translations[0]
    translated = html.unescape(first.get("translatedText", ""))
    detected_source_language = first.get("detectedSourceLanguage")
    return translated, latency_ms, detected_source_language

# =========================================================
# AUDIT LOG HELPERS
# =========================================================
def log_all_audit(
    trace_id: str,
    user_id: str,
    source_type: str,
    group_id: str,
    room_id: str,
    raw_input: str,
    content: str,
    status: str,
    note: str = ""
) -> None:
    logger.info(
        f"[ALL_AUDIT] trace_id={trace_id} "
        f"user_id={user_id} "
        f"source_type={source_type} "
        f"group_id={group_id} "
        f"room_id={room_id} "
        f"status={status} "
        f"raw_input={json.dumps(raw_input, ensure_ascii=False)} "
        f"content={json.dumps(content, ensure_ascii=False)} "
        f"note={json.dumps(note, ensure_ascii=False)}"
    )


def log_total_latency(trace_id: str, route_name: str, total_ms: int, source_type: str, group_id: str, room_id: str) -> None:
    logger.info(
        f"[LATENCY] trace_id={trace_id} "
        f"route={route_name} "
        f"total_ms={total_ms} "
        f"source_type={source_type} "
        f"group_id={group_id} "
        f"room_id={room_id}"
    )

# =========================================================
# PHASE 1 WORKER FLOW HELPERS
# =========================================================
def is_worker_entry_command(input_text: str) -> bool:
    return safe_str(input_text).lower() == WORKER_ENTRY_COMMAND


def normalize_need_type_input(input_text: str) -> str:
    raw = safe_str(input_text).lower()
    return NEED_TYPE_ALIAS_MAP.get(raw, raw)


def is_valid_need_type(input_text: str) -> bool:
    return normalize_need_type_input(input_text) in NEED_TYPE_V1


def is_valid_urgency_level(input_text: str) -> bool:
    return normalize_urgency_input(input_text) in URGENCY_LEVEL_V1


def is_valid_job_target(input_text: str) -> bool:
    return safe_str(input_text) in JOB_TARGET_V1


def build_worker_need_menu_text(language_group: str) -> str:
    lines = [
        i18n_text(language_group, "worker_need_title"),
        i18n_text(language_group, "worker_need_intro"),
        ""
    ]
    for idx, code in enumerate(NEED_TYPE_MENU_ITEMS, start=1):
        lines.append(f"{idx}. {code} = {i18n_text(language_group, f'need.{code}')}")
    lines.extend([
        "",
        f"{i18n_text(language_group, 'example_send')} transfer_job hoặc số (1)",
        "Gửi taiwan_job hoặc có thể gửi bằng số (3)",
    ])
    return "\n".join(lines)


def build_urgency_menu_text(language_group: str) -> str:
    items = ["urgent", "soon", "normal"]
    lines = [
        i18n_text(language_group, "urgency_title"),
        i18n_text(language_group, "urgency_intro"),
        ""
    ]
    for idx, code in enumerate(items, start=1):
        lines.append(f"{idx}. {code} = {i18n_text(language_group, f'urgency.{code}')}")
    lines.extend([
        "",
        f"{i18n_text(language_group, 'example_send')} 1"
    ])
    return "\n".join(lines)


def build_job_target_menu_text(language_group: str) -> str:
    items = ["job_for_overseas", "job_in_taiwan"]
    lines = [
        i18n_text(language_group, "job_target_title"),
        i18n_text(language_group, "job_target_intro"),
        ""
    ]
    for idx, code in enumerate(items, start=1):
        lines.append(f"{idx}. {code} = {i18n_text(language_group, f'job_target.{code}')}")
    lines.extend([
        "",
        f"{i18n_text(language_group, 'example_send')} job_for_overseas"
    ])
    return "\n".join(lines)


def build_request_residence_card_text(language_group: str) -> str:
    return "\n".join([
        i18n_text(language_group, "request_arc_title"),
        i18n_text(language_group, "request_arc_body"),
        "",
        i18n_text(language_group, "request_arc_note_1"),
        i18n_text(language_group, "request_arc_note_2"),
        i18n_text(language_group, "request_arc_note_3"),
    ])


def build_request_cv_form_text(language_group: str) -> str:
    return "\n".join([
        i18n_text(language_group, "request_cv_title"),
        i18n_text(language_group, "request_cv_intro"),
        "",
        i18n_text(language_group, "cv.full_name"),
        i18n_text(language_group, "cv.height"),
        i18n_text(language_group, "cv.weight"),
        i18n_text(language_group, "cv.hometown"),
        i18n_text(language_group, "cv.education"),
        i18n_text(language_group, "cv.marital_status"),
        i18n_text(language_group, "cv.siblings_count"),
        i18n_text(language_group, "cv.birth_order"),
        i18n_text(language_group, "cv.phone_overseas"),
        i18n_text(language_group, "cv.job_preference"),
        i18n_text(language_group, "cv.work_exp_vn"),
        "",
        i18n_text(language_group, "cv.send_hint"),
    ])


def build_invalid_need_type_text(language_group: str) -> str:
    lines = [
        i18n_text(language_group, "invalid_need_type"),
        "",
    ]
    for idx, code in enumerate(NEED_TYPE_MENU_ITEMS, start=1):
        lines.append(f"- {code} / {idx}")
    return "\n".join(lines)


def build_invalid_urgency_text(language_group: str) -> str:
    return "\n".join([
        i18n_text(language_group, "invalid_urgency"),
        "",
        "- urgent / 1",
        "- soon / 2",
        "- normal / 3",
    ])


def build_invalid_job_target_text(language_group: str) -> str:
    return "\n".join([
        i18n_text(language_group, "invalid_job_target"),
        "",
        "- job_for_overseas",
        "- job_in_taiwan",
    ])


def is_midflow_code_without_context(input_text: str) -> bool:
    raw = safe_str(input_text)
    if normalize_need_type_input(raw) in NEED_TYPE_V1:
        return True
    if normalize_urgency_input(raw) in URGENCY_LEVEL_V1:
        return True
    if raw in JOB_TARGET_V1:
        return True
    return False


def handle_worker_entry(
    user_id: str,
    scope_key: str,
    reply_token: str,
    trace_id: str,
    language_group: str,
) -> Tuple[bool, int]:
    clear_ads_view_cache(scope_key, trace_id)
    set_runtime_state(
        user_id=user_id,
        scope_key=scope_key,
        current_state=STATE_AWAITING_NEED_TYPE,
        temp_need_type="",
        temp_urgency_level="",
        temp_job_target="",
        temp_residence_card_image_url="",
        trace_id=trace_id,
    )
    logger.info(
        f"[{trace_id}] WORKER_ENTRY_TRIGGER matched command={WORKER_ENTRY_COMMAND} "
        f"next_state={STATE_AWAITING_NEED_TYPE} language_group={language_group}"
    )
    return line_reply(reply_token, build_worker_need_menu_text(language_group), trace_id)


def handle_need_type_selection(
    user_id: str,
    scope_key: str,
    input_text: str,
    reply_token: str,
    trace_id: str,
    language_group: str,
) -> Tuple[bool, int]:
    selected_need_type = normalize_need_type_input(input_text)

    if not is_valid_need_type(selected_need_type):
        logger.info(f"[{trace_id}] NEED_TYPE_INVALID input={json.dumps(input_text, ensure_ascii=False)}")
        return line_reply(reply_token, build_invalid_need_type_text(language_group), trace_id)

    existing_state = get_runtime_state(user_id, scope_key, trace_id)
    set_runtime_state(
        user_id=user_id,
        scope_key=scope_key,
        current_state=STATE_AWAITING_URGENCY,
        temp_need_type=selected_need_type,
        temp_urgency_level="",
        temp_job_target=existing_state.get("temp_job_target", ""),
        temp_residence_card_image_url=existing_state.get("temp_residence_card_image_url", ""),
        trace_id=trace_id,
    )

    logger.info(
        f"[{trace_id}] NEED_TYPE_ACCEPTED selected={selected_need_type} "
        f"next_state={STATE_AWAITING_URGENCY} language_group={language_group}"
    )
    return line_reply(reply_token, build_urgency_menu_text(language_group), trace_id)


def handle_urgency_selection(
    user_id: str,
    scope_key: str,
    input_text: str,
    reply_token: str,
    trace_id: str,
    language_group: str,
) -> Tuple[bool, int]:
    normalized_input = normalize_urgency_input(input_text)

    if not is_valid_urgency_level(normalized_input):
        logger.info(f"[{trace_id}] URGENCY_INVALID input={json.dumps(input_text, ensure_ascii=False)}")
        return line_reply(reply_token, build_invalid_urgency_text(language_group), trace_id)

    selected_urgency = normalized_input
    existing_state = get_runtime_state(user_id, scope_key, trace_id)
    current_need_type = safe_str(existing_state.get("temp_need_type"))

    if current_need_type == "taiwan_job":
        next_state = STATE_AWAITING_JOB_TARGET
        reply_text = build_job_target_menu_text(language_group)
    elif current_need_type in DIRECT_ARC_NEED_TYPES:
        next_state = STATE_AWAITING_RESIDENCE_CARD
        reply_text = build_request_residence_card_text(language_group)
    else:
        next_state = STATE_AWAITING_RESIDENCE_CARD
        reply_text = build_request_residence_card_text(language_group)

    set_runtime_state(
        user_id=user_id,
        scope_key=scope_key,
        current_state=next_state,
        temp_need_type=current_need_type,
        temp_urgency_level=selected_urgency,
        temp_job_target=existing_state.get("temp_job_target", ""),
        temp_residence_card_image_url=existing_state.get("temp_residence_card_image_url", ""),
        trace_id=trace_id,
    )

    logger.info(
        f"[{trace_id}] URGENCY_ACCEPTED selected={selected_urgency} "
        f"need_type={current_need_type} next_state={next_state} language_group={language_group}"
    )
    return line_reply(reply_token, reply_text, trace_id)


def handle_job_target_selection(
    user_id: str,
    scope_key: str,
    input_text: str,
    reply_token: str,
    trace_id: str,
    language_group: str,
) -> Tuple[bool, int]:
    selected_job_target = safe_str(input_text)

    if not is_valid_job_target(selected_job_target):
        logger.info(f"[{trace_id}] JOB_TARGET_INVALID input={json.dumps(selected_job_target, ensure_ascii=False)}")
        return line_reply(reply_token, build_invalid_job_target_text(language_group), trace_id)

    existing_state = get_runtime_state(user_id, scope_key, trace_id)
    current_need_type = safe_str(existing_state.get("temp_need_type"))
    current_urgency = safe_str(existing_state.get("temp_urgency_level"))

    if selected_job_target == "job_for_overseas":
        next_state = STATE_AWAITING_CV_FORM
        reply_text = build_request_cv_form_text(language_group)
    else:
        next_state = STATE_AWAITING_RESIDENCE_CARD
        reply_text = build_request_residence_card_text(language_group)

    set_runtime_state(
        user_id=user_id,
        scope_key=scope_key,
        current_state=next_state,
        temp_need_type=current_need_type,
        temp_urgency_level=current_urgency,
        temp_job_target=selected_job_target,
        temp_residence_card_image_url=existing_state.get("temp_residence_card_image_url", ""),
        trace_id=trace_id,
    )

    logger.info(
        f"[{trace_id}] JOB_TARGET_ACCEPTED selected={selected_job_target} "
        f"need_type={current_need_type} next_state={next_state} language_group={language_group}"
    )
    return line_reply(reply_token, reply_text, trace_id)

# =========================================================
# ADS SHEET HELPERS
# =========================================================
_GSPREAD_CLIENT = None
_ADS_CATALOG_CACHE = {
    "rows": [],
    "loaded_at_ts": 0,
    "last_read_ok": False,
}

_ADS_VIEW_CACHE: Dict[str, dict] = {}


def cleanup_ads_view_cache(now_ts: int) -> None:
    expired_before = now_ts - max(ADS_VIEW_TTL_SECONDS, 60)
    stale_keys = [
        k for k, v in _ADS_VIEW_CACHE.items()
        if int(v.get("loaded_at_ts", 0) or 0) < expired_before
    ]
    for k in stale_keys:
        _ADS_VIEW_CACHE.pop(k, None)

    if stale_keys:
        logger.info(f"[ADS_VIEW_CACHE] cleanup removed={len(stale_keys)} remaining={len(_ADS_VIEW_CACHE)}")


def clear_ads_view_cache(scope_key: str, trace_id: str = "") -> None:
    _ADS_VIEW_CACHE.pop(safe_str(scope_key), None)
    if trace_id:
        logger.info(f"[{trace_id}] ADS_VIEW_CACHE_CLEAR scope_key={scope_key}")


def set_ads_view_cache(scope_key: str, viewer_language_group: str, ads: List[dict], trace_id: str) -> None:
    now_ts = get_now_ts()
    cleanup_ads_view_cache(now_ts)
    _ADS_VIEW_CACHE[safe_str(scope_key)] = {
        "scope_key": safe_str(scope_key),
        "viewer_language_group": normalize_language_group(viewer_language_group),
        "ad_ids": [safe_str(ad.get("ad_id")) for ad in ads if safe_str(ad.get("ad_id"))],
        "loaded_at_ts": now_ts,
    }
    logger.info(
        f"[{trace_id}] ADS_VIEW_CACHE_SET scope_key={scope_key} "
        f"viewer_language_group={viewer_language_group} ad_count={len(_ADS_VIEW_CACHE[safe_str(scope_key)]['ad_ids'])}"
    )


def get_ads_view_cache(scope_key: str, trace_id: str) -> Optional[dict]:
    now_ts = get_now_ts()
    cleanup_ads_view_cache(now_ts)

    cached = _ADS_VIEW_CACHE.get(safe_str(scope_key))
    if not cached:
        logger.info(f"[{trace_id}] ADS_VIEW_CACHE_GET status=MISS scope_key={scope_key}")
        return None

    logger.info(
        f"[{trace_id}] ADS_VIEW_CACHE_GET status=HIT scope_key={scope_key} "
        f"ad_count={len(cached.get('ad_ids', []))}"
    )
    return cached


def is_sheet_env_ready() -> bool:
    return bool(GOOGLE_SERVICE_ACCOUNT_JSON) and bool(PHASE1_SPREADSHEET_NAME)


def parse_bool_flag(value) -> bool:
    return safe_str(value).lower() in {"true", "1", "yes", "y", "on"}


def parse_iso_datetime(value: str) -> Optional[datetime]:
    raw = safe_str(value)
    if not raw:
        return None

    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=TW_TZ)
    return dt.astimezone(TW_TZ)


def is_ad_active_in_time_window(start_at: str, end_at: str) -> bool:
    now_dt = now_tw_dt()
    start_dt = parse_iso_datetime(start_at)
    end_dt = parse_iso_datetime(end_at)

    if start_dt and now_dt < start_dt:
        return False
    if end_dt and now_dt > end_dt:
        return False
    return True


def get_google_credentials(trace_id: str):
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        logger.error(f"[{trace_id}] GSHEET_CREDENTIALS_MISSING")
        return None

    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        return Credentials.from_service_account_info(info, scopes=scopes)
    except Exception as e:
        logger.exception(f"[{trace_id}] GSHEET_CREDENTIALS_INVALID exception={type(e).__name__}:{e}")
        return None


def get_gspread_client(trace_id: str):
    global _GSPREAD_CLIENT

    if _GSPREAD_CLIENT is not None:
        return _GSPREAD_CLIENT

    credentials = get_google_credentials(trace_id)
    if not credentials:
        return None

    try:
        _GSPREAD_CLIENT = gspread.authorize(credentials)
        logger.info(f"[{trace_id}] GSHEET_CLIENT_READY")
        return _GSPREAD_CLIENT
    except Exception as e:
        logger.exception(f"[{trace_id}] GSHEET_CLIENT_INIT_FAILED exception={type(e).__name__}:{e}")
        return None


def open_ads_catalog_worksheet(trace_id: str):
    client = get_gspread_client(trace_id)
    if not client:
        return None

    try:
        spreadsheet = client.open(PHASE1_SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(ADS_CATALOG_SHEET_NAME)
        return worksheet
    except Exception as e:
        logger.exception(f"[{trace_id}] ADS_SHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        return None


def normalize_ad_type(value: str) -> str:
    value = safe_str(value).lower()
    if value in SUPPORTED_AD_TYPES:
        return value
    return ""


def normalize_visibility_policy(value: str) -> str:
    value = safe_str(value).lower()
    if value in SUPPORTED_VISIBILITY_POLICIES:
        return value
    return VISIBILITY_SAME_LANGUAGE_ONLY


def normalize_ad_status(value: str) -> str:
    return safe_str(value).lower()


def parse_priority(value: str) -> int:
    raw = safe_str(value)
    if not raw:
        return 0

    try:
        return int(float(raw))
    except Exception:
        return 0


def parse_sortable_time(value: str) -> str:
    return safe_str(value) or "9999-12-31T23:59:59+08:00"


def is_worker_flow_active(current_state: str) -> bool:
    return normalize_state_value(current_state) in {
        STATE_AWAITING_NEED_TYPE,
        STATE_AWAITING_URGENCY,
        STATE_AWAITING_JOB_TARGET,
        STATE_AWAITING_CV_FORM,
        STATE_AWAITING_RESIDENCE_CARD,
        STATE_AWAITING_PHONE_NUMBER,
    }


def is_ads_entry_command(input_text: str) -> bool:
    return safe_str(input_text).lower() == ADS_ENTRY_COMMAND


def load_ads_catalog_rows(trace_id: str) -> Tuple[List[dict], bool]:
    now_ts = get_now_ts()

    if (
        int(_ADS_CATALOG_CACHE["loaded_at_ts"] or 0) > 0
        and now_ts - int(_ADS_CATALOG_CACHE["loaded_at_ts"] or 0) < ADS_CACHE_TTL_SECONDS
    ):
        logger.info(
            f"[{trace_id}] ADS_CACHE_HIT rows={len(_ADS_CATALOG_CACHE['rows'])} "
            f"last_read_ok={_ADS_CATALOG_CACHE['last_read_ok']}"
        )
        return _ADS_CATALOG_CACHE["rows"], bool(_ADS_CATALOG_CACHE["last_read_ok"])

    ws = open_ads_catalog_worksheet(trace_id)
    if not ws:
        _ADS_CATALOG_CACHE["loaded_at_ts"] = now_ts
        _ADS_CATALOG_CACHE["rows"] = []
        _ADS_CATALOG_CACHE["last_read_ok"] = False
        return [], False

    try:
        records = ws.get_all_records()
    except Exception as e:
        logger.exception(f"[{trace_id}] ADS_SHEET_READ_FAILED exception={type(e).__name__}:{e}")
        _ADS_CATALOG_CACHE["loaded_at_ts"] = now_ts
        _ADS_CATALOG_CACHE["rows"] = []
        _ADS_CATALOG_CACHE["last_read_ok"] = False
        return [], False

    rows = []
    skipped_inactive_time = 0
    for raw in records:
        row = {
            "ad_id": safe_str(raw.get("ad_id")),
            "owner_user_id": safe_str(raw.get("owner_user_id")),
            "owner_contact_name": safe_str(raw.get("owner_contact_name")),
            "owner_line_id": safe_str(raw.get("owner_line_id")),
            "ad_type": normalize_ad_type(raw.get("ad_type")),
            "author_language_group": normalize_language_group(raw.get("author_language_group")),
            "visibility_policy": normalize_visibility_policy(raw.get("visibility_policy")),
            "direct_contact_enabled": parse_bool_flag(raw.get("direct_contact_enabled")),
            "title_source": safe_str(raw.get("title_source")),
            "body_source": safe_str(raw.get("body_source")),
            "status": normalize_ad_status(raw.get("status")),
            "priority": parse_priority(raw.get("priority")),
            "start_at": safe_str(raw.get("start_at")),
            "end_at": safe_str(raw.get("end_at")),
            "created_at": safe_str(raw.get("created_at")),
            "updated_at": safe_str(raw.get("updated_at")),
        }

        if not row["ad_id"]:
            continue
        if row["ad_type"] not in SUPPORTED_AD_TYPES:
            continue
        if row["status"] not in ACTIVE_AD_STATUSES:
            continue
        if not row["title_source"]:
            continue
        if not is_ad_active_in_time_window(row["start_at"], row["end_at"]):
            skipped_inactive_time += 1
            continue

        rows.append(row)

    rows.sort(
        key=lambda item: (
            -item["priority"],
            parse_sortable_time(item["start_at"]),
            parse_sortable_time(item["created_at"]),
            item["ad_id"],
        )
    )

    _ADS_CATALOG_CACHE["rows"] = rows
    _ADS_CATALOG_CACHE["loaded_at_ts"] = now_ts
    _ADS_CATALOG_CACHE["last_read_ok"] = True
    logger.info(
        f"[{trace_id}] ADS_CACHE_REFRESH rows={len(rows)} skipped_inactive_time={skipped_inactive_time}"
    )
    return rows, True


def is_ad_visible_to_viewer(ad: dict, viewer_language_group: str) -> bool:
    ad_type = ad.get("ad_type", "")
    author_language_group = normalize_language_group(ad.get("author_language_group", ""))
    visibility_policy = normalize_visibility_policy(ad.get("visibility_policy", ""))

    if ad_type == ADS_TYPE_JOB_OPENING:
        return viewer_language_group == author_language_group

    if ad_type == ADS_TYPE_SERVICE_OFFER:
        if visibility_policy == VISIBILITY_SAME_LANGUAGE_ONLY:
            return viewer_language_group == author_language_group
        if visibility_policy in {
            VISIBILITY_CROSS_LANGUAGE_ALLOWED,
            VISIBILITY_VIEWER_LOCALIZED,
        }:
            return True

    return False


def get_visible_ads_for_viewer(viewer_language_group: str, trace_id: str) -> Tuple[List[dict], bool]:
    all_ads, read_ok = load_ads_catalog_rows(trace_id)
    visible_ads = [ad for ad in all_ads if is_ad_visible_to_viewer(ad, viewer_language_group)]
    logger.info(
        f"[{trace_id}] ADS_FILTER_DONE viewer_language_group={viewer_language_group} "
        f"visible_count={len(visible_ads)} read_ok={read_ok}"
    )
    return visible_ads[:ADS_LIST_LIMIT], read_ok


def build_ads_entry_text(ads: List[dict], viewer_language_group: str) -> str:
    if not ads:
        return (
            "📢 DANH SÁCH TIN\n"
            f"Ngôn ngữ hiện tại: {viewer_language_group}\n\n"
            "Hiện chưa có tin phù hợp.\n"
            "Vui lòng thử lại sau."
        )

    service_offer_lines = []
    job_opening_lines = []

    for idx, ad in enumerate(ads, start=1):
        line = f"{idx}. [{ad['ad_type']}] {ad['title_source']}"
        if ad["ad_type"] == ADS_TYPE_SERVICE_OFFER:
            service_offer_lines.append(line)
        elif ad["ad_type"] == ADS_TYPE_JOB_OPENING:
            job_opening_lines.append(line)

    lines = [
        "📢 DANH SÁCH TIN",
        f"Ngôn ngữ hiện tại: {viewer_language_group}",
        "",
    ]

    if service_offer_lines:
        lines.append("Dịch vụ / Service Offer:")
        lines.extend(service_offer_lines)
        lines.append("")

    if job_opening_lines:
        lines.append("Việc làm / Job Opening:")
        lines.extend(job_opening_lines)
        lines.append("")

    lines.append("Gõ số tương ứng để xem chi tiết tin.")
    lines.append("Gõ /ads để xem lại danh sách.")
    return "\n".join(lines).strip()


def is_ads_numeric_selection(input_text: str) -> bool:
    raw = safe_str(input_text)
    return raw.isdigit() and int(raw) >= 1


def get_ad_by_id(ad_id: str, trace_id: str) -> Optional[dict]:
    rows, read_ok = load_ads_catalog_rows(trace_id)
    if not read_ok:
        return None
    for row in rows:
        if safe_str(row.get("ad_id")) == safe_str(ad_id):
            return row
    return None


def resolve_selected_ad_from_cache(scope_key: str, input_text: str, trace_id: str) -> Tuple[Optional[dict], str]:
    cached = get_ads_view_cache(scope_key, trace_id)
    if not cached:
        return None, "cache_miss"

    raw = safe_str(input_text)
    if not raw.isdigit():
        return None, "not_digit"

    selection_index = int(raw)
    ad_ids = cached.get("ad_ids", [])
    if selection_index < 1 or selection_index > len(ad_ids):
        return None, "index_out_of_range"

    ad_id = safe_str(ad_ids[selection_index - 1])
    if not ad_id:
        return None, "empty_ad_id"

    ad = get_ad_by_id(ad_id, trace_id)
    if not ad:
        return None, "ad_not_found"

    viewer_language_group = normalize_language_group(cached.get("viewer_language_group", DEFAULT_LANGUAGE_GROUP))
    if not is_ad_visible_to_viewer(ad, viewer_language_group):
        return None, "ad_not_visible"

    return ad, "ok"


def build_ads_detail_text(ad: dict, viewer_language_group: str) -> str:
    ad_type = safe_str(ad.get("ad_type"))
    title_source = safe_str(ad.get("title_source")) or "(không có tiêu đề)"
    body_source = safe_str(ad.get("body_source")) or "(không có mô tả)"
    owner_contact_name = safe_str(ad.get("owner_contact_name")) or "Chưa có tên liên hệ"

    lines = [
        "📄 CHI TIẾT TIN",
        f"Ngôn ngữ hiện tại: {viewer_language_group}",
        f"Loại tin: {ad_type}",
        "",
        f"Tiêu đề: {title_source}",
        f"Mô tả: {body_source}",
        f"Người đăng: {owner_contact_name}",
        "",
        "Gõ /ads để xem lại danh sách.",
    ]
    return "\n".join(lines).strip()
# =========================================================
# COMMAND HELPERS
# =========================================================
def parse_all_command(input_text: str) -> Tuple[bool, str]:
    raw = safe_str(input_text)
    lowered = raw.lower()

    if lowered == "!all":
        return True, ""

    if lowered.startswith("!all "):
        content = raw[5:].strip()
        return True, content

    return False, ""

# =========================================================
# HEALTH
# =========================================================
@app.route("/", methods=["GET"])
def health():
    ready = is_runtime_ready()
    return jsonify({
        "ok": ready,
        "service": "line-bot-render-phase1-rootcause-patched",
        "app_version": APP_VERSION,
        "time": now_tw_iso(),
        "ready": ready,
        "timeouts": {
            "connect_seconds": CONNECT_TIMEOUT_SECONDS,
            "read_seconds": READ_TIMEOUT_SECONDS
        },
        "phase1": {
            "spreadsheet_name": PHASE1_SPREADSHEET_NAME,
            "user_state_sheet": USER_STATE_SHEET_NAME,
            "sheet_env_ready": bool(GOOGLE_SERVICE_ACCOUNT_JSON),
            "state_read_in_callback": False,
            "runtime_state_enabled": True,
            "runtime_state_ttl_seconds": RUNTIME_STATE_TTL_SECONDS,
            "worker_entry_command": WORKER_ENTRY_COMMAND,
            "ads_entry_command": ADS_ENTRY_COMMAND,
            "ads_catalog_sheet": ADS_CATALOG_SHEET_NAME,
            "ads_entry_enabled": True,
            "ads_detail_enabled": True,
            "ads_detail_enabled": True,
            "worker_entry_enabled": True,
            "need_type_selection_enabled": True,
            "urgency_selection_enabled": True,
            "urgency_numeric_alias_enabled": True,
            "job_target_selection_enabled": True,
            "language_personalization_enabled": True,
            "midflow_idle_guard_enabled": True,
            "supported_language_groups": sorted(list(SUPPORTED_LANGUAGE_GROUPS)),
            "default_language_group": normalize_language_group(DEFAULT_LANGUAGE_GROUP),
            "sheet_env_ready_for_ads": is_sheet_env_ready(),
            "ads_cache_ttl_seconds": ADS_CACHE_TTL_SECONDS,
            "ads_view_ttl_seconds": ADS_VIEW_TTL_SECONDS,
            "user_language_map_size": len(USER_LANGUAGE_MAP),
        }
    }), 200 if ready else 503

# =========================================================
# WEBHOOK
# =========================================================
@app.route("/callback", methods=["POST"])
def callback():
    total_started = time.perf_counter()
    trace_id = make_trace_id()
    body = request.get_data()
    sig = request.headers.get("X-Line-Signature", "").strip()

    logger.info(f"[{trace_id}] WEBHOOK_RECEIVED")

    source_type = ""
    group_id = ""
    room_id = ""

    if not is_runtime_ready():
        logger.error(f"[{trace_id}] RUNTIME_NOT_READY")
        log_total_latency(
            trace_id=trace_id,
            route_name="runtime_not_ready",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "Service unavailable", 503

    if not verify_line_signature(LINE_CHANNEL_SECRET, body, sig):
        logger.error(f"[{trace_id}] INVALID_SIGNATURE")
        log_total_latency(
            trace_id=trace_id,
            route_name="invalid_signature",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "Invalid", 403

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as e:
        logger.exception(f"[{trace_id}] JSON_PARSE_ERROR exception={type(e).__name__}:{e}")
        log_total_latency(
            trace_id=trace_id,
            route_name="bad_payload",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "Bad payload", 400

    events = payload.get("events", [])
    if not events:
        logger.warning(f"[{trace_id}] NO_EVENTS")
        log_total_latency(
            trace_id=trace_id,
            route_name="no_events",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    event = events[0]

    if event.get("type") != "message":
        logger.info(f"[{trace_id}] SKIP_NON_MESSAGE type={event.get('type')}")
        log_total_latency(
            trace_id=trace_id,
            route_name="skip_non_message",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    message = event.get("message", {})
    message_type = safe_str(message.get("type"))

    source_type, user_id, group_id, room_id = extract_source_ids(event)
    scope_key = get_scope_key(source_type, user_id, group_id, room_id)
    reply_token = safe_str(event.get("replyToken"))
    language_group = resolve_user_language_group(user_id)

    logger.info(
        f"[{trace_id}] INPUT_META source_type={source_type} "
        f"group_id={group_id} room_id={room_id} message_type={message_type} "
        f"language_group={language_group}"
    )

    runtime_state = get_runtime_state(user_id, scope_key, trace_id)
    current_state = normalize_state_value(runtime_state.get("current_state", STATE_IDLE))
    logger.info(
        f"[{trace_id}] RUNTIME_STATE_CONTEXT "
        f"user_id={user_id} scope_key={scope_key} "
        f"current_state={current_state} language_group={language_group}"
    )

    if message_type == "image":
        logger.info(f"[{trace_id}] IMAGE_INPUT current_state={current_state} language_group={language_group}")

        if current_state == STATE_AWAITING_RESIDENCE_CARD:
            reply_ok, reply_ms = line_reply(
                reply_token,
                i18n_text(language_group, "arc_image_received"),
                trace_id,
            )
            logger.info(f"[{trace_id}] RESIDENCE_CARD_IMAGE_PLACEHOLDER reply_ok={reply_ok} reply_ms={reply_ms}")
            log_total_latency(
                trace_id=trace_id,
                route_name="residence_card_image_placeholder",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            return "OK", 200

        if current_state == STATE_AWAITING_CV_FORM:
            reply_ok, reply_ms = line_reply(
                reply_token,
                i18n_text(language_group, "cv_form_image_received"),
                trace_id,
            )
            logger.info(f"[{trace_id}] CV_FORM_IMAGE_PLACEHOLDER reply_ok={reply_ok} reply_ms={reply_ms}")
            log_total_latency(
                trace_id=trace_id,
                route_name="cv_form_image_placeholder",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            return "OK", 200

        reply_ok, reply_ms = line_reply(
            reply_token,
            i18n_text(language_group, "generic_image_placeholder"),
            trace_id,
        )
        logger.info(f"[{trace_id}] IMAGE_PLACEHOLDER reply_ok={reply_ok} reply_ms={reply_ms}")
        log_total_latency(
            trace_id=trace_id,
            route_name="image_placeholder",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    if message_type != "text":
        logger.info(f"[{trace_id}] SKIP_NON_TEXT message_type={message_type}")
        log_total_latency(
            trace_id=trace_id,
            route_name="skip_non_text",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    input_text = safe_str(message.get("text"))
    logger.info(
        f"[{trace_id}] INPUT source_type={source_type} "
        f"group_id={group_id} room_id={room_id} "
        f"text={json.dumps(input_text, ensure_ascii=False)} language_group={language_group}"
    )

    if not input_text:
        logger.info(f"[{trace_id}] SKIP_EMPTY_TEXT")
        log_total_latency(
            trace_id=trace_id,
            route_name="skip_empty_text",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    # =====================================================
    # /ADS ENTRY V1
    # =====================================================
    if is_ads_entry_command(input_text):
        if is_worker_flow_active(current_state):
            reply_ok, reply_ms = line_reply(
                reply_token,
                "Bạn đang ở giữa luồng /worker. Hãy hoàn tất hoặc quay lại /worker trước khi xem /ads.",
                trace_id,
            )
            logger.info(
                f"[{trace_id}] ADS_ENTRY_BLOCKED_BY_WORKER current_state={current_state} "
                f"reply_ok={reply_ok} reply_ms={reply_ms}"
            )
            log_total_latency(
                trace_id=trace_id,
                route_name="ads_entry_blocked_by_worker",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            return "OK", 200

        if not is_sheet_env_ready():
            reply_ok, reply_ms = line_reply(
                reply_token,
                "Tạm thời chưa bật dữ liệu quảng cáo. Thiếu kết nối Google Sheets.",
                trace_id,
            )
            logger.info(
                f"[{trace_id}] ADS_ENTRY_SHEET_ENV_NOT_READY reply_ok={reply_ok} reply_ms={reply_ms}"
            )
            log_total_latency(
                trace_id=trace_id,
                route_name="ads_entry_sheet_env_not_ready",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            return "OK", 200

        viewer_language_group = language_group
        visible_ads, ads_read_ok = get_visible_ads_for_viewer(viewer_language_group, trace_id)

        if ads_read_ok:
            set_ads_view_cache(scope_key, viewer_language_group, visible_ads, trace_id)
            ads_text = build_ads_entry_text(visible_ads, viewer_language_group)
        else:
            clear_ads_view_cache(scope_key, trace_id)
            ads_text = "Tạm thời chưa đọc được dữ liệu quảng cáo. Vui lòng thử lại sau."

        reply_ok, reply_ms = line_reply(reply_token, ads_text, trace_id)
        logger.info(
            f"[{trace_id}] ADS_ENTRY_OK viewer_language_group={viewer_language_group} "
            f"ads_count={len(visible_ads)} ads_read_ok={ads_read_ok} reply_ok={reply_ok} reply_ms={reply_ms}"
        )
        log_total_latency(
            trace_id=trace_id,
            route_name="ads_entry_ok",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    # =====================================================
    # ADS DETAIL V1
    # =====================================================
    if current_state in {STATE_IDLE, STATE_CASE_COMPLETED} and is_ads_numeric_selection(input_text):
        cached_ads_view = get_ads_view_cache(scope_key, trace_id)
        if cached_ads_view:
            selected_ad, resolve_status = resolve_selected_ad_from_cache(scope_key, input_text, trace_id)
            if selected_ad:
                viewer_language_group = language_group
                detail_text = build_ads_detail_text(selected_ad, viewer_language_group)
                reply_ok, reply_ms = line_reply(reply_token, detail_text, trace_id)
                logger.info(
                    f"[{trace_id}] ADS_DETAIL_OK selection={input_text} ad_id={selected_ad.get('ad_id','')} "
                    f"reply_ok={reply_ok} reply_ms={reply_ms}"
                )
                log_total_latency(
                    trace_id=trace_id,
                    route_name="ads_detail_ok",
                    total_ms=ms_since(total_started),
                    source_type=source_type,
                    group_id=group_id,
                    room_id=room_id
                )
                return "OK", 200

            reply_ok, reply_ms = line_reply(
                reply_token,
                "Không tìm thấy tin tương ứng. Gõ /ads để xem lại danh sách.",
                trace_id,
            )
            logger.info(
                f"[{trace_id}] ADS_DETAIL_INVALID selection={input_text} resolve_status={resolve_status} "
                f"reply_ok={reply_ok} reply_ms={reply_ms}"
            )
            log_total_latency(
                trace_id=trace_id,
                route_name="ads_detail_invalid",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            return "OK", 200

    # =====================================================
    # !ALL
    # =====================================================
    is_all_command, content = parse_all_command(input_text)
    if is_all_command:
        if not is_admin(user_id):
            log_all_audit(
                trace_id=trace_id,
                user_id=user_id,
                source_type=source_type,
                group_id=group_id,
                room_id=room_id,
                raw_input=input_text,
                content=content,
                status="DENY_NOT_ADMIN",
                note="user is not in ADMIN_IDS"
            )
            reply_ok, reply_ms = line_reply(reply_token, i18n_text(language_group, "not_authorized"), trace_id)
            logger.info(f"[{trace_id}] DENY_NOT_ADMIN reply_ok={reply_ok} reply_ms={reply_ms}")
            log_total_latency(
                trace_id=trace_id,
                route_name="all_deny_not_admin",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            return "OK", 200

        if not content:
            log_all_audit(
                trace_id=trace_id,
                user_id=user_id,
                source_type=source_type,
                group_id=group_id,
                room_id=room_id,
                raw_input=input_text,
                content=content,
                status="DENY_EMPTY",
                note="!all without content"
            )
            reply_ok, reply_ms = line_reply(reply_token, i18n_text(language_group, "all_need_content"), trace_id)
            logger.info(f"[{trace_id}] DENY_EMPTY reply_ok={reply_ok} reply_ms={reply_ms}")
            log_total_latency(
                trace_id=trace_id,
                route_name="all_deny_empty",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            return "OK", 200

        if len(content) > MAX_ALL_CHARS:
            log_all_audit(
                trace_id=trace_id,
                user_id=user_id,
                source_type=source_type,
                group_id=group_id,
                room_id=room_id,
                raw_input=input_text,
                content=content,
                status="DENY_TOO_LONG",
                note=f"content length > {MAX_ALL_CHARS}"
            )
            reply_ok, reply_ms = line_reply(
                reply_token,
                i18n_text(language_group, "all_too_long", max_chars=MAX_ALL_CHARS),
                trace_id
            )
            logger.info(f"[{trace_id}] DENY_TOO_LONG reply_ok={reply_ok} reply_ms={reply_ms}")
            log_total_latency(
                trace_id=trace_id,
                route_name="all_deny_too_long",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            return "OK", 200

        allowed, remaining = allow_all_command(user_id, scope_key)
        if not allowed:
            log_all_audit(
                trace_id=trace_id,
                user_id=user_id,
                source_type=source_type,
                group_id=group_id,
                room_id=room_id,
                raw_input=input_text,
                content=content,
                status="DENY_RATE_LIMIT",
                note=f"cooldown_remaining={remaining}s"
            )
            reply_ok, reply_ms = line_reply(
                reply_token,
                i18n_text(language_group, "all_cooldown", remaining=remaining),
                trace_id
            )
            logger.info(f"[{trace_id}] DENY_RATE_LIMIT reply_ok={reply_ok} reply_ms={reply_ms}")
            log_total_latency(
                trace_id=trace_id,
                route_name="all_deny_rate_limit",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            return "OK", 200

        translated, translate_ms, detected_source_language = translate_auto_source(
            content,
            LOCKED_TARGET_LANG,
            trace_id
        )

        final_text = translated or content
        msg = f"{i18n_text(language_group, 'all_broadcast_prefix')}{final_text}"
        reply_ok, reply_ms = line_reply(reply_token, msg, trace_id)

        log_all_audit(
            trace_id=trace_id,
            user_id=user_id,
            source_type=source_type,
            group_id=group_id,
            room_id=room_id,
            raw_input=input_text,
            content=content,
            status="SUCCESS" if reply_ok else "FAILED_REPLY",
            note=(
                f"detected_source_language={detected_source_language or 'unknown'} "
                f"translate_ms={translate_ms} reply_ms={reply_ms}"
            )
        )
        log_total_latency(
            trace_id=trace_id,
            route_name="all_success" if reply_ok else "all_failed_reply",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    # =====================================================
    # PHASE 1 FLOW
    # =====================================================
    if is_worker_entry_command(input_text):
        reply_ok, reply_ms = handle_worker_entry(
            user_id=user_id,
            scope_key=scope_key,
            reply_token=reply_token,
            trace_id=trace_id,
            language_group=language_group,
        )
        logger.info(f"[{trace_id}] WORKER_ENTRY_REPLY reply_ok={reply_ok} reply_ms={reply_ms}")
        log_total_latency(
            trace_id=trace_id,
            route_name="worker_entry_trigger",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    if current_state == STATE_AWAITING_NEED_TYPE:
        reply_ok, reply_ms = handle_need_type_selection(
            user_id=user_id,
            scope_key=scope_key,
            input_text=input_text,
            reply_token=reply_token,
            trace_id=trace_id,
            language_group=language_group,
        )
        logger.info(f"[{trace_id}] NEED_TYPE_SELECTION_REPLY reply_ok={reply_ok} reply_ms={reply_ms}")
        log_total_latency(
            trace_id=trace_id,
            route_name="need_type_selection",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    if current_state == STATE_AWAITING_URGENCY:
        reply_ok, reply_ms = handle_urgency_selection(
            user_id=user_id,
            scope_key=scope_key,
            input_text=input_text,
            reply_token=reply_token,
            trace_id=trace_id,
            language_group=language_group,
        )
        logger.info(f"[{trace_id}] URGENCY_SELECTION_REPLY reply_ok={reply_ok} reply_ms={reply_ms}")
        log_total_latency(
            trace_id=trace_id,
            route_name="urgency_selection",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    if current_state == STATE_AWAITING_JOB_TARGET:
        reply_ok, reply_ms = handle_job_target_selection(
            user_id=user_id,
            scope_key=scope_key,
            input_text=input_text,
            reply_token=reply_token,
            trace_id=trace_id,
            language_group=language_group,
        )
        logger.info(f"[{trace_id}] JOB_TARGET_SELECTION_REPLY reply_ok={reply_ok} reply_ms={reply_ms}")
        log_total_latency(
            trace_id=trace_id,
            route_name="job_target_selection",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    if current_state == STATE_AWAITING_CV_FORM:
        reply_ok, reply_ms = line_reply(
            reply_token,
            i18n_text(language_group, "awaiting_cv_form_text"),
            trace_id,
        )
        logger.info(f"[{trace_id}] AWAITING_CV_FORM_TEXT_PLACEHOLDER reply_ok={reply_ok} reply_ms={reply_ms}")
        log_total_latency(
            trace_id=trace_id,
            route_name="awaiting_cv_form_text_placeholder",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    if current_state == STATE_AWAITING_RESIDENCE_CARD:
        reply_ok, reply_ms = line_reply(
            reply_token,
            i18n_text(language_group, "awaiting_arc_text"),
            trace_id,
        )
        logger.info(f"[{trace_id}] AWAITING_RESIDENCE_CARD_TEXT_PLACEHOLDER reply_ok={reply_ok} reply_ms={reply_ms}")
        log_total_latency(
            trace_id=trace_id,
            route_name="awaiting_residence_card_text_placeholder",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    # =====================================================
    # ROOT-CAUSE PATCH:
    # Nếu user gửi mã giữa chừng khi state đã rơi về idle
    # thì không dịch thường; bắt quay lại /worker
    # =====================================================
    if current_state == STATE_IDLE and is_midflow_code_without_context(input_text) and not get_ads_view_cache(scope_key, trace_id):
        reply_ok, reply_ms = line_reply(
            reply_token,
            i18n_text(language_group, "resume_flow_prompt"),
            trace_id,
        )
        logger.info(f"[{trace_id}] IDLE_MIDFLOW_GUARD reply_ok={reply_ok} reply_ms={reply_ms}")
        log_total_latency(
            trace_id=trace_id,
            route_name="idle_midflow_guard",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    # =====================================================
    # NORMAL TRANSLATE
    # =====================================================
    translated, translate_ms, detected_source_language = translate_auto_source(
        input_text,
        LOCKED_TARGET_LANG,
        trace_id
    )
    reply_ok, reply_ms = line_reply(reply_token, translated or FALLBACK_REPLY_TEXT, trace_id)

    logger.info(
        f"[NORMAL_FLOW] trace_id={trace_id} "
        f"detected_source_language={detected_source_language or 'unknown'} "
        f"translate_ms={translate_ms} reply_ms={reply_ms} "
        f"reply_ok={reply_ok} language_group={language_group}"
    )

    log_total_latency(
        trace_id=trace_id,
        route_name="normal_translate",
        total_ms=ms_since(total_started),
        source_type=source_type,
        group_id=group_id,
        room_id=room_id
    )
    return "OK", 200

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    logger.info(f"[BOOT] Starting app on port={port}")
    app.run(host="0.0.0.0", port=port)
