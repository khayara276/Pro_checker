import json
import time
import random
import threading
import queue
import os
import re
import requests
import sqlite3
import tempfile
import sys
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from DrissionPage import ChromiumPage, ChromiumOptions

# ==========================================
# ⚙️ CORE CONFIGURATION
# ==========================================

# Platform Tokens (From Secrets)
TOKEN_CHANNEL_A = os.environ.get("TOKEN_CHANNEL_A")
TOKEN_CHANNEL_B = os.environ.get("TOKEN_CHANNEL_B")
TARGET_CHAT = os.environ.get("TARGET_CHAT")

# Paths & Settings
SESSION_STORE = "runtime_session.db"
INTERVAL_CHECK = 0.05
AUTH_DATA = os.environ.get("AUTH_DATA")
HEADLESS_ENABLED = True
WORKER_COUNT = 50
BATCH_THRESHOLD = 15

# Endpoint Configurations
BASE_DOMAIN = os.environ.get("BASE_DOMAIN")

ENDPOINT_MAP = {
    'Category_A': {
        'endpoint': os.environ.get("ENDPOINT_CATEGORY_A"),
        'tab': None
    },
    'Category_B': {
        'endpoint': os.environ.get("ENDPOINT_CATEGORY_B"),
        'tab': None
    },
    'Category_C': {
        'endpoint': os.environ.get("ENDPOINT_CATEGORY_C"),
        'tab': None
    }
}

# Session Handler
msg_session = requests.Session()
retry_config = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
msg_session.mount('https://', HTTPAdapter(max_retries=retry_config, pool_connections=200, pool_maxsize=200))

# ==========================================
# 🛠️ UTILITY FUNCTIONS
# ==========================================

def log_info(message):
    """Enhanced logging with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] ℹ️  {message}")
    sys.stdout.flush()

def log_error(message, exception=None):
    """Error logging with exception details"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    error_msg = f"[{timestamp}] ❌ {message}"
    if exception:
        error_msg += f" | Exception: {str(exception)}"
    print(error_msg)
    sys.stdout.flush()

def log_success(message):
    """Success logging"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] ✅ {message}")
    sys.stdout.flush()

def log_warning(message):
    """Warning logging"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] ⚠️  {message}")
    sys.stdout.flush()

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
            log_success(f"Notification sent successfully")
        else:
            log_error(f"Notification failed with status {resp.status_code}")

    except Exception as e:
        log_error("Notification dispatch failed", e)

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
        self.browser = None
        self.active = True
        self.detail_queue = queue.Queue()
        self.db_queue = queue.Queue()
        self.memory_cache = set()

        # Clean previous session
        if os.path.exists(SESSION_STORE):
            try:
                os.remove(SESSION_STORE)
                log_info("Previous session data cleared")
            except Exception as e:
                log_error("Failed to clear session data", e)

        self.setup_database()
        log_success("Session system initialized")

    def setup_database(self):
        """Initialize SQLite database"""
        try:
            conn = sqlite3.connect(SESSION_STORE)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS tracked_items (item_id TEXT PRIMARY KEY)")
            conn.commit()
            conn.close()
            log_success("Database schema ready")
        except Exception as e:
            log_error("Database initialization failed", e)

    def _database_writer(self):
        """Background thread for database writes"""
        conn = sqlite3.connect(SESSION_STORE, check_same_thread=False)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            log_info("Database WAL mode enabled")
        except Exception as e:
            log_warning(f"WAL mode setup issue: {e}")

        while self.active:
            try:
                item_id = self.db_queue.get(timeout=1)
                try:
                    conn.execute("INSERT OR IGNORE INTO tracked_items (item_id) VALUES (?)", (item_id,))
                    conn.commit()
                except Exception as e:
                    log_error(f"DB write error for {item_id}", e)
                self.db_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                log_error("Database writer thread error", e)

        conn.close()
        log_info("Database writer thread stopped")

    def is_new_item(self, item_id):
        """Check if item is new and mark as seen"""
        if item_id in self.memory_cache:
            return False

        self.memory_cache.add(item_id)
        self.db_queue.put(item_id)
        return True

    def get_browser_config(self, port, headless=HEADLESS_ENABLED):
        """Configure browser options"""
        log_info(f"Configuring browser on port {port} (headless={headless})")
        co = ChromiumOptions()
        co.set_local_port(port)
        co.set_user_data_path(os.path.join(tempfile.gettempdir(), f"automation_profile_{port}"))
        co.set_argument('--no-sandbox')
        co.set_argument('--disable-dev-shm-usage')
        co.set_argument('--mute-audio')
        co.set_argument('--blink-settings=imagesEnabled=false')
        co.set_argument('--disable-gpu')

        if headless:
            co.set_argument('--headless=new')

        return co

    def initialize_browser(self):
        """Initialize browser with authentication"""
        log_info("Starting browser initialization...")
        port = random.randint(30001, 40000)
        co = self.get_browser_config(port)

        try:
            self.browser = ChromiumPage(co)
            log_success("Browser instance created")
        except Exception as e:
            log_error("Failed to create browser instance", e)
            return False

        # Load authentication data
        if AUTH_DATA:
            try:
                log_info("Loading authentication credentials...")
                auth_cookies = json.loads(AUTH_DATA)
                self.browser.get(BASE_DOMAIN)
                time.sleep(2)
                self.browser.set.cookies(auth_cookies)
                self.browser.refresh()
                time.sleep(2)
                log_success("Authentication cookies applied")
            except Exception as e:
                log_error("Authentication setup failed", e)
                return False
        else:
            log_error("No authentication data provided")
            return False

        # Initialize category tabs
        log_info("Setting up category tabs...")
        try:
            ENDPOINT_MAP['Category_A']['tab'] = self.browser.latest_tab
            log_info("Primary tab assigned to Category_A")

            for cat in ['Category_B', 'Category_C']:
                ENDPOINT_MAP[cat]['tab'] = self.browser.new_tab(BASE_DOMAIN)
                time.sleep(0.5)
                log_info(f"New tab assigned to {cat}")

            log_success("All category tabs initialized")
        except Exception as e:
            log_error("Tab initialization failed", e)
            return False

        return True

    def fetch_data_js(self, tab, url):
        """Fetch data using JavaScript in browser context"""
        js_script = f"""
        return fetch("{url}&_t=" + Date.now(), {{
            headers: {{'Cache-Control': 'no-cache', 'Pragma': 'no-cache'}}
        }}).then(res => {{
            if(res.status === 403) return "403_ACCESS_DENIED";
            if(!res.ok) return "ERROR_" + res.status;
            return res.json();
        }}).catch(err => "NETWORK_ERROR");
        """

        try:
            result = tab.run_js(js_script, timeout=12)
            return result
        except Exception as e:
            log_error(f"JS fetch failed: {url[:100]}...", e)
            return None

    def send_batch_alerts(self, items, token):
        """Send rapid alerts for batch items"""
        log_info(f"⚡ BATCH MODE: Processing {len(items)} items rapidly")

        for item in items:
            try:
                item_id = item.get('fnlColorVariantData', {}).get('colorGroup') or item.get('code')
                title = item.get('name', 'New Product Alert')
                item_url = f"{BASE_DOMAIN}/p/{item_id}"

                img_url = item.get('url', '')
                if 'images' in item and len(item['images']) > 0:
                    img_url = item['images'][0].get('url')

                message = (
                    f"⚠️ **RAPID ALERT**\n"
                    f"📦 {title}\n"
                    f"🆔 `{item_id}`\n\n"
                    f"*Fetching detailed information...*"
                )

                send_notification(message, token, image_url=img_url, action_url=item_url)
                time.sleep(0.1)

            except Exception as e:
                log_error(f"Batch alert failed for item", e)

    def _detail_processor(self):
        """Worker thread for processing item details"""
        log_info("Detail processor thread started")

        while self.active:
            try:
                task = self.detail_queue.get(timeout=1)
                item_id = task['id']
                category = task['category']
                token = task['token']
                is_batch = task.get('is_batch', False)
                basic_info = task.get('basic_info', {})

                detail_endpoint = f"{BASE_DOMAIN}/api/p/{item_id}?fields=SITE"

                # Select random tab for load balancing
                available_tabs = [c['tab'] for c in ENDPOINT_MAP.values() if c['tab']]
                worker_tab = random.choice(available_tabs) if available_tabs else ENDPOINT_MAP['Category_A']['tab']

                # Retry mechanism with 5 attempts
                detailed_data = None
                for attempt in range(1, 6):
                    log_info(f"Fetching details for {item_id} (Attempt {attempt}/5)")
                    detailed_data = self.fetch_data_js(worker_tab, detail_endpoint)

                    if isinstance(detailed_data, dict):
                        log_success(f"Details fetched for {item_id}")
                        break

                    if attempt < 5:
                        time.sleep(1.5)
                        log_warning(f"Retry {attempt} failed for {item_id}, retrying...")

                if isinstance(detailed_data, dict):
                    self.format_and_dispatch(item_id, category, detailed_data, is_batch, token)
                else:
                    log_warning(f"All retries exhausted for {item_id}, using basic data")
                    self.format_and_dispatch(item_id, category, None, is_batch, token, basic_info)

                self.detail_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                log_error("Detail processor error", e)

    def extract_primary_image(self, full_data):
        """Extract best quality image from full data"""
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

        except Exception as e:
            log_error("Image extraction failed", e)

        return None

    def extract_fallback_image(self, basic_info):
        """Extract image from basic data"""
        try:
            if 'images' in basic_info and len(basic_info['images']) > 0:
                return basic_info['images'][0].get('url')

            if 'fnlColorVariantData' in basic_info:
                return basic_info['fnlColorVariantData'].get('outfitPictureURL')

        except Exception as e:
            log_error("Fallback image extraction failed", e)

        return None

    def format_and_dispatch(self, item_id, category, full_data, is_batch, token, basic_info=None):
        """Format data and send notification"""
        try:
            action_url = f"{BASE_DOMAIN}/p/{item_id}"
            capture_time = datetime.now().strftime('%H:%M:%S')

            if full_data:
                title = full_data.get('productRelationID', full_data.get('name', 'Product'))

                # Price extraction
                price_display = "N/A"
                price_obj = full_data.get('offerPrice') or full_data.get('price')
                if price_obj:
                    raw_value = price_obj.get('value')
                    price_display = f"₹{int(raw_value)}" if raw_value else price_obj.get('formattedValue', 'N/A')

                image_url = self.extract_primary_image(full_data)

                # Stock availability parsing
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
                    stock_info = "⚠️ Stock data parsing required"

            else:
                # Fallback to basic data
                title = basic_info.get('name', 'Product Alert')
                price_display = "Check Link"
                image_url = self.extract_fallback_image(basic_info)
                stock_info = "⚠️ Detailed stock unavailable (Fetch failed)"

            # Message title logic
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
            log_success(f"Alert dispatched for {item_id}")

        except Exception as e:
            log_error(f"Alert formatting failed for {item_id}", e)

    def monitor_category(self, category_name):
        """Monitor specific category for changes"""
        config = ENDPOINT_MAP[category_name]
        tab = config['tab']
        base_endpoint = config['endpoint']

        log_info(f"Started monitoring {category_name}")

        while self.active:
            try:
                # Fetch first page
                first_page = re.sub(r'currentPage=\d+', 'currentPage=0', base_endpoint)
                response = self.fetch_data_js(tab, first_page)

                if response == "403_ACCESS_DENIED":
                    log_error(f"[{category_name}] 403 Access Denied - Check authentication")
                    time.sleep(5)
                    continue

                if not isinstance(response, dict):
                    log_warning(f"[{category_name}] Invalid response format")
                    time.sleep(2)
                    continue

                # Pagination handling
                pagination_info = response.get('pagination', {})
                total_pages = pagination_info.get('totalPages', 1)

                log_info(f"[{category_name}] Processing {total_pages} pages")

                collected_items = []

                # Fetch all pages
                for page_idx in range(total_pages):
                    if page_idx == 0:
                        page_items = response.get('products', [])
                    else:
                        page_endpoint = re.sub(r'currentPage=\d+', f'currentPage={page_idx}', base_endpoint)
                        page_response = self.fetch_data_js(tab, page_endpoint)

                        if isinstance(page_response, dict):
                            page_items = page_response.get('products', [])
                        else:
                            log_warning(f"[{category_name}] Page {page_idx} fetch failed")
                            page_items = []

                    collected_items.extend(page_items)
                    log_info(f"[{category_name}] Page {page_idx+1}/{total_pages} collected")

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
                    log_success(f"✨ [{category_name}] Discovered {item_count} NEW items!")

                    # Batch logic
                    use_batch = item_count <= BATCH_THRESHOLD

                    if use_batch:
                        log_info(f"[{category_name}] Using BATCH mode for {item_count} items")
                    else:
                        log_warning(f"[{category_name}] Large batch ({item_count}), skipping rapid alerts for reliability")

                    # Group by token
                    grouped_items = []
                    for item in new_items:
                        token = determine_token(category_name, item)
                        grouped_items.append((item, token))

                    # Send batch alerts if enabled
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

                    # Queue all items for detailed processing
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
                log_error(f"[{category_name}] Monitoring loop error", e)
                time.sleep(2)

    def start_monitoring(self):
        """Start the monitoring engine"""
        log_info("="*60)
        log_info("AUTOMATION ENGINE STARTING")
        log_info("="*60)

        if not self.initialize_browser():
            log_error("Browser initialization failed - Cannot proceed")
            return

        log_success("Browser ready - Starting worker threads")

        # Start database writer
        threading.Thread(target=self._database_writer, daemon=True).start()

        # Start detail processors
        log_info(f"Launching {WORKER_COUNT} worker threads")
        for worker_id in range(WORKER_COUNT):
            threading.Thread(target=self._detail_processor, daemon=True).start()

        log_success(f"{WORKER_COUNT} workers active")

        # Start category monitors
        for category in ENDPOINT_MAP.keys():
            threading.Thread(target=self.monitor_category, args=(category,), daemon=True).start()
            log_info(f"Monitor thread launched for {category}")

        log_success("All monitoring threads active")
        log_info("="*60)
        log_info("ENGINE FULLY OPERATIONAL - Monitoring in progress...")
        log_info("="*60)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log_warning("Shutdown signal received")
            self.active = False
            if self.browser:
                self.browser.quit()
                log_info("Browser closed")
            log_info("Engine stopped gracefully")

# ==========================================
# 🎯 MAIN EXECUTION
# ==========================================

if __name__ == "__main__":
    log_info("Application starting...")

    # Validate environment variables
    required_vars = ["TOKEN_CHANNEL_A", "TOKEN_CHANNEL_B", "TARGET_CHAT", "AUTH_DATA", "BASE_DOMAIN"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        log_error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    log_success("All environment variables validated")

    engine = AutomationEngine()
    engine.start_monitoring()
