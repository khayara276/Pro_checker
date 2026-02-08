import json
import time
import random
import threading
import queue
import os
import re
import requests
import sqlite3
import sys
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================
# ⚙️ CORE CONFIGURATION
# ==========================================

TOKEN_CHANNEL_A = os.environ.get("TOKEN_CHANNEL_A")
TOKEN_CHANNEL_B = os.environ.get("TOKEN_CHANNEL_B")
TARGET_CHAT = os.environ.get("TARGET_CHAT")

SESSION_STORE = "runtime_session.db"
INTERVAL_CHECK = 0.05
AUTH_DATA = os.environ.get("AUTH_DATA")
WORKER_COUNT = 50
BATCH_THRESHOLD = 15

BASE_DOMAIN = os.environ.get("BASE_DOMAIN")

ENDPOINT_MAP = {
    'Category_A': {
        'endpoint': os.environ.get("ENDPOINT_CATEGORY_A")
    },
    'Category_B': {
        'endpoint': os.environ.get("ENDPOINT_CATEGORY_B")
    },
    'Category_C': {
        'endpoint': os.environ.get("ENDPOINT_CATEGORY_C")
    }
}

# Session Handler
msg_session = requests.Session()
retry_config = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
msg_session.mount('https://', HTTPAdapter(max_retries=retry_config, pool_connections=200, pool_maxsize=200))

api_session = requests.Session()
api_session.mount('https://', HTTPAdapter(max_retries=retry_config, pool_connections=200, pool_maxsize=200))

# ==========================================
# 🛠️ UTILITY FUNCTIONS
# ==========================================

def log_info(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] ℹ️  {message}")
    sys.stdout.flush()

def log_error(message, exception=None):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    error_msg = f"[{timestamp}] ❌ {message}"
    if exception:
        error_msg += f" | Exception: {str(exception)}"
    print(error_msg)
    sys.stdout.flush()

def log_success(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] ✅ {message}")
    sys.stdout.flush()

def log_warning(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] ⚠️  {message}")
    sys.stdout.flush()

def setup_cookies():
    """Setup cookies from AUTH_DATA with proper headers"""
    try:
        if not AUTH_DATA:
            log_error("No AUTH_DATA provided")
            return False

        cookies_list = json.loads(AUTH_DATA)
        log_info(f"Loading {len(cookies_list)} cookies...")

        # Build cookies dict from list
        cookies_dict = {}
        for cookie in cookies_list:
            name = cookie.get('name')
            value = cookie.get('value')
            if name and value:
                cookies_dict[name] = value

        # Set cookies using update
        api_session.cookies.update(cookies_dict)

        # Setup realistic browser headers
        api_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': f'{BASE_DOMAIN}/',
            'Origin': BASE_DOMAIN,
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
        })

        log_success(f"Cookies and headers configured successfully")
        log_info(f"Session cookies: {len(api_session.cookies)}")
        return True

    except Exception as e:
        log_error("Cookie setup failed", e)
        return False

def send_notification(message, token, image_url=None, action_url=None):
    """Send message via Telegram API"""
    try:
        payload = {
            "chat_id": TARGET_CHAT,
            "parse_mode": "HTML"
        }

        if action_url:
            payload["reply_markup"] = json.dumps({
                "inline_keyboard": [[
                    {"text": "🛍️ VIEW ITEM", "url": action_url}
                ]]
            })

        if image_url:
            url = f"https://api.telegram.org/bot{token}/sendPhoto"
            payload["photo"] = image_url
            payload["caption"] = message
        else:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload["text"] = message
            payload["disable_web_page_preview"] = False

        resp = msg_session.post(url, data=payload, timeout=30)

        if resp.status_code == 200:
            log_success(f"Notification sent")
        else:
            log_error(f"Notification failed: {resp.status_code}")

    except Exception as e:
        log_error("Notification error", e)

def determine_token(category, item_data):
    """Route notification to appropriate channel"""
    if category == 'Category_C':
        return TOKEN_CHANNEL_A
    elif category == 'Category_B':
        return TOKEN_CHANNEL_B
    else:
        segment = item_data.get('segmentNameText', '').lower()
        if 'women' in segment:
            return TOKEN_CHANNEL_B
        elif 'men' in segment:
            return TOKEN_CHANNEL_A
        return TOKEN_CHANNEL_B

# ==========================================
# 🚀 MONITOR ENGINE
# ==========================================

class AutomationEngine:
    def __init__(self):
        log_info("Initializing Automation Engine...")
        self.active = True
        self.detail_queue = queue.Queue()
        self.db_queue = queue.Queue()
        self.memory_cache = set()

        if os.path.exists(SESSION_STORE):
            try:
                os.remove(SESSION_STORE)
                log_info("Previous session cleared")
            except Exception as e:
                log_error("Session clear failed", e)

        self.setup_database()
        log_success("Session system ready")

    def setup_database(self):
        try:
            conn = sqlite3.connect(SESSION_STORE)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS tracked_items (item_id TEXT PRIMARY KEY)")
            conn.commit()
            conn.close()
            log_success("Database ready")
        except Exception as e:
            log_error("Database init failed", e)

    def _database_writer(self):
        conn = sqlite3.connect(SESSION_STORE, check_same_thread=False)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except:
            pass

        while self.active:
            try:
                item_id = self.db_queue.get(timeout=1)
                try:
                    conn.execute("INSERT OR IGNORE INTO tracked_items (item_id) VALUES (?)", (item_id,))
                    conn.commit()
                except:
                    pass
                self.db_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                log_error("DB writer error", e)

        conn.close()

    def is_new_item(self, item_id):
        if item_id in self.memory_cache:
            return False
        self.memory_cache.add(item_id)
        self.db_queue.put(item_id)
        return True

    def fetch_api_data(self, url):
        """Fetch API data using requests with cookies and proper headers"""
        try:
            # Add timestamp to prevent caching
            separator = '&' if '?' in url else '?'
            url_with_ts = f"{url}{separator}_t={int(time.time() * 1000)}"

            response = api_session.get(url_with_ts, timeout=15)

            if response.status_code == 403:
                log_error(f"403 Access Denied (Status: {response.status_code})")
                log_warning(f"Response headers: {dict(response.headers)}")
                return "403_ACCESS_DENIED"

            if response.status_code != 200:
                log_warning(f"API returned status {response.status_code}")
                return f"ERROR_{response.status_code}"

            return response.json()

        except requests.exceptions.Timeout:
            log_error("Request timeout")
            return "TIMEOUT"
        except Exception as e:
            log_error(f"API fetch error", e)
            return None

    def send_batch_alerts(self, items, token):
        log_info(f"⚡ BATCH MODE: Sending {len(items)} rapid alerts")

        for item in items:
            try:
                item_id = item.get('fnlColorVariantData', {}).get('colorGroup') or item.get('code')
                title = item.get('name', 'New Product')
                item_url = f"{BASE_DOMAIN}/p/{item_id}"

                img_url = item.get('url', '')
                if 'images' in item and len(item['images']) > 0:
                    img_url = item['images'][0].get('url')

                message = (
                    f"⚠️ **RAPID ALERT**\n"
                    f"📦 {title}\n"
                    f"🆔 `{item_id}`\n\n"
                    f"*Fetching details...*"
                )

                send_notification(message, token, image_url=img_url, action_url=item_url)
                time.sleep(0.1)

            except Exception as e:
                log_error("Batch alert error", e)

    def _detail_processor(self):
        while self.active:
            try:
                task = self.detail_queue.get(timeout=1)
                item_id = task['id']
                category = task['category']
                token = task['token']
                is_batch = task.get('is_batch', False)
                basic_info = task.get('basic_info', {})

                detail_url = f"{BASE_DOMAIN}/api/p/{item_id}?fields=SITE"

                # Retry mechanism with 5 attempts
                detailed_data = None
                for attempt in range(1, 6):
                    log_info(f"Fetching {item_id} (Attempt {attempt}/5)")
                    detailed_data = self.fetch_api_data(detail_url)

                    if isinstance(detailed_data, dict):
                        log_success(f"Details fetched: {item_id}")
                        break

                    if attempt < 5:
                        time.sleep(1.5)

                if isinstance(detailed_data, dict):
                    self.format_and_dispatch(item_id, category, detailed_data, is_batch, token)
                else:
                    log_warning(f"Using basic data for {item_id}")
                    self.format_and_dispatch(item_id, category, None, is_batch, token, basic_info)

                self.detail_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                log_error("Detail processor error", e)

    def extract_primary_image(self, full_data):
        try:
            img = full_data.get('selected', {}).get('modelImage', {}).get('url')
            if img:
                return img

            base_options = full_data.get('baseOptions', [])
            if base_options:
                img = base_options[0].get('options', [])[0].get('modelImage', {}).get('url')
                if img:
                    return img

            if 'images' in full_data and len(full_data['images']) > 0:
                return full_data['images'][0].get('url')
        except:
            pass
        return None

    def extract_fallback_image(self, basic_info):
        try:
            if 'images' in basic_info and len(basic_info['images']) > 0:
                return basic_info['images'][0].get('url')

            if 'fnlColorVariantData' in basic_info:
                return basic_info['fnlColorVariantData'].get('outfitPictureURL')
        except:
            pass
        return None

    def format_and_dispatch(self, item_id, category, full_data, is_batch, token, basic_info=None):
        try:
            action_url = f"{BASE_DOMAIN}/p/{item_id}"
            capture_time = datetime.now().strftime('%H:%M:%S')

            if full_data:
                title = full_data.get('productRelationID', full_data.get('name', 'Product'))

                price_display = "N/A"
                price_obj = full_data.get('offerPrice') or full_data.get('price')
                if price_obj:
                    raw_value = price_obj.get('value')
                    price_display = f"₹{int(raw_value)}" if raw_value else price_obj.get('formattedValue', 'N/A')

                image_url = self.extract_primary_image(full_data)

                stock_lines = []
                variants = full_data.get('variantOptions', [])

                if variants:
                    for variant in variants:
                        qualifiers = variant.get('variantOptionQualifiers', [])

                        size = next((q['value'] for q in qualifiers if q['qualifier'] == 'size'),
                                  next((q['value'] for q in qualifiers if q['qualifier'] == 'standardSize'), 'N/A'))

                        quantity = variant.get('stock', {}).get('stockLevel', 0)
                        status = variant.get('stock', {}).get('stockLevelStatus', '')

                        if status != 'outOfStock' and quantity > 0:
                            stock_lines.append(f"✅ **{size}** : {quantity} units")
                        elif status == 'inStock':
                            stock_lines.append(f"✅ **{size}** : Available")
                        else:
                            stock_lines.append(f"❌ {size} : OOS")

                    stock_info = "\n".join(stock_lines)
                else:
                    stock_info = "⚠️ Stock parsing check"

            else:
                title = basic_info.get('name', 'Product')
                price_display = "Check Link"
                image_url = self.extract_fallback_image(basic_info)
                stock_info = "⚠️ Details unavailable"

            header = "📦 **STOCK UPDATE**" if is_batch else "🔥 **NEW ARRIVAL**"

            notification_msg = (
                f"{header}\n\n"
                f"👚 **{title}**\n"
                f"💰 **{price_display}**\n\n"
                f"📏 **Availability:**\n"
                f"{stock_info}\n\n"
                f"⚡ Captured: {capture_time}"
            )

            send_notification(notification_msg, token, image_url=image_url, action_url=action_url)
            log_success(f"Alert sent: {item_id}")

        except Exception as e:
            log_error(f"Alert error for {item_id}", e)

    def monitor_category(self, category_name):
        config = ENDPOINT_MAP[category_name]
        base_endpoint = config['endpoint']

        log_info(f"Started monitoring {category_name}")

        while self.active:
            try:
                # Fetch first page
                first_page = re.sub(r'currentPage=\d+', 'currentPage=0', base_endpoint)
                response = self.fetch_api_data(first_page)

                if response == "403_ACCESS_DENIED":
                    log_error(f"[{category_name}] 403 - Check cookies/headers")
                    time.sleep(5)
                    continue

                if not isinstance(response, dict):
                    log_warning(f"[{category_name}] Invalid response")
                    time.sleep(2)
                    continue

                pagination_info = response.get('pagination', {})
                total_pages = pagination_info.get('totalPages', 1)

                log_info(f"[{category_name}] Processing {total_pages} pages")

                collected_items = []

                # Fetch ALL pages first
                for page_idx in range(total_pages):
                    if page_idx == 0:
                        page_items = response.get('products', [])
                    else:
                        page_url = re.sub(r'currentPage=\d+', f'currentPage={page_idx}', base_endpoint)
                        page_response = self.fetch_api_data(page_url)

                        if isinstance(page_response, dict):
                            page_items = page_response.get('products', [])
                        else:
                            log_warning(f"[{category_name}] Page {page_idx} failed")
                            page_items = []

                    collected_items.extend(page_items)
                    log_info(f"[{category_name}] Page {page_idx+1}/{total_pages} done")

                # Filter new items
                new_items = []
                for item in collected_items:
                    item_id = item.get('fnlColorVariantData', {}).get('colorGroup') or item.get('code')

                    if not item_id:
                        url = item.get('url', '')
                        if '/p/' in url:
                            item_id = url.split('/p/')[1].split('.html')[0].split('?')[0]

                    if not item_id:
                        continue

                    if self.is_new_item(item_id):
                        new_items.append(item)

                item_count = len(new_items)

                if item_count > 0:
                    log_success(f"✨ [{category_name}] Found {item_count} NEW items!")

                    use_batch = item_count <= BATCH_THRESHOLD

                    if use_batch:
                        log_info(f"[{category_name}] BATCH mode ({item_count} items)")
                    else:
                        log_warning(f"[{category_name}] DIRECT mode ({item_count} items)")

                    grouped_items = []
                    for item in new_items:
                        token = determine_token(category_name, item)
                        grouped_items.append((item, token))

                    if use_batch:
                        channel_a_batch = [x[0] for x in grouped_items if x[1] == TOKEN_CHANNEL_A]
                        channel_b_batch = [x[0] for x in grouped_items if x[1] == TOKEN_CHANNEL_B]

                        if channel_a_batch:
                            threading.Thread(target=self.send_batch_alerts, 
                                           args=(channel_a_batch, TOKEN_CHANNEL_A), 
                                           daemon=True).start()

                        if channel_b_batch:
                            threading.Thread(target=self.send_batch_alerts, 
                                           args=(channel_b_batch, TOKEN_CHANNEL_B), 
                                           daemon=True).start()

                    for item, token in grouped_items:
                        item_id = item.get('fnlColorVariantData', {}).get('colorGroup') or item.get('code')

                        self.detail_queue.put({
                            'id': item_id,
                            'category': category_name,
                            'is_batch': use_batch,
                            'basic_info': item,
                            'token': token
                        })

                time.sleep(INTERVAL_CHECK)

            except Exception as e:
                log_error(f"[{category_name}] Monitor error", e)
                time.sleep(2)

    def start_monitoring(self):
        log_info("="*60)
        log_info("AUTOMATION ENGINE STARTING")
        log_info("="*60)

        if not setup_cookies():
            log_error("Cookie setup failed - Cannot proceed")
            return

        log_success("Authentication ready")

        threading.Thread(target=self._database_writer, daemon=True).start()

        log_info(f"Launching {WORKER_COUNT} workers")
        for _ in range(WORKER_COUNT):
            threading.Thread(target=self._detail_processor, daemon=True).start()

        log_success(f"{WORKER_COUNT} workers active")

        for category in ENDPOINT_MAP.keys():
            threading.Thread(target=self.monitor_category, args=(category,), daemon=True).start()
            log_info(f"Monitor launched: {category}")

        log_success("All monitors active")
        log_info("="*60)
        log_info("ENGINE FULLY OPERATIONAL")
        log_info("="*60)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log_warning("Shutdown signal")
            self.active = False
            log_info("Engine stopped")

if __name__ == "__main__":
    log_info("Application starting...")

    required_vars = ["TOKEN_CHANNEL_A", "TOKEN_CHANNEL_B", "TARGET_CHAT", "AUTH_DATA", "BASE_DOMAIN"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        log_error(f"Missing env vars: {', '.join(missing_vars)}")
        sys.exit(1)

    log_success("All env vars validated")

    engine = AutomationEngine()
    engine.start_monitoring()
