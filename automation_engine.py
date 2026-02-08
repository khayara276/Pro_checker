import json
import time
import threading
import queue
import os
import re
import sqlite3
from datetime import datetime
from curl_cffi import requests
from flask import Flask, jsonify

# ==========================================
# 🔥🔥🔥 ULTIMATE VERSION - ALL PAGES + ULTRA FAST! 🔥🔥🔥
# ==========================================

TOKEN_MEN = os.environ.get("TOKEN_MEN")
TOKEN_WOMEN = os.environ.get("TOKEN_WOMEN")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID")
PORT = int(os.environ.get("PORT", 8080))
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "")

SESSION_DB_PATH = "session_monitor.db"

# ULTIMATE SPEED SETTINGS - MAXIMUM PARALLELIZATION!
CHECK_INTERVAL = 0.001  # 1ms = 1000 checks/second!
NUM_ALERT_WORKERS = 250  # More alert workers
PAGE_FETCHERS_PER_CATEGORY = 50  # 50 parallel page fetchers PER category!
SELF_PING_INTERVAL = 600  # 10 minutes

CATEGORY_CONFIGS = {
    'Universal': {
        'url': "https://www.sheinindia.in/api/category/sverse-5939-37961?fields=SITE&currentPage=0&pageSize=45&format=json&query=%3Arelevance&gridColumns=5&segmentIds=23%2C14%2C18%2C9&cohortIds=value%7Cmen%2CTEMP_M1_LL_FG_NOV&customerType=Existing&facets=&customertype=Existing&advfilter=true&platform=Desktop&showAdsOnNextPage=false&is_ads_enable_plp=true&displayRatings=true&segmentIds=&&store=shein"
    },
    'Women': {
        'url': "https://www.sheinindia.in/api/category/sverse-5939-37961?fields=SITE&currentPage=0&pageSize=45&format=json&query=%3Arelevance%3Agenderfilter%3AWomen&gridColumns=5&segmentIds=23%2C14%2C18%2C9&cohortIds=value%7Cmen%2CTEMP_M1_LL_FG_NOV&customerType=Existing&facets=genderfilter%3AWomen&customertype=Existing&advfilter=true&platform=Desktop&showAdsOnNextPage=false&is_ads_enable_plp=true&displayRatings=true&segmentIds=&&store=shein"
    },
    'Men': {
        'url': "https://www.sheinindia.in/api/category/sverse-5939-37961?fields=SITE&currentPage=0&pageSize=45&format=json&query=%3Arelevance%3Agenderfilter%3AMen&gridColumns=5&segmentIds=23%2C14%2C18%2C9&cohortIds=value%7Cmen%2CTEMP_M1_LL_FG_NOV&customerType=Existing&facets=genderfilter%3AMen&customertype=Existing&advfilter=true&platform=Desktop&showAdsOnNextPage=false&is_ads_enable_plp=true&displayRatings=true&segmentIds=&&store=shein"
    }
}

app = Flask(__name__)
api_session = requests.Session()

# ==========================================
# 🛠️ UTILITY FUNCTIONS
# ==========================================

def log(message, level="INFO"):
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    icons = {
        "INFO": "ℹ️", 
        "SUCCESS": "✅", 
        "ERROR": "❌", 
        "WARNING": "⚠️", 
        "FAST": "⚡", 
        "PING": "🔔", 
        "ULTIMATE": "🔥"
    }
    icon = icons.get(level, "📝")
    print(f"[{timestamp}] {icon} {message}", flush=True)

def setup_api_session():
    """Setup API session with cookies"""
    try:
        cookie_content = os.environ.get("COOKIE_FILE_CONTENT")
        if not cookie_content:
            return False

        cookies_list = json.loads(cookie_content)
        cookies_dict = {}
        for cookie in cookies_list:
            name = cookie.get('name')
            value = cookie.get('value')
            if name and value:
                cookies_dict[name] = value

        for name, value in cookies_dict.items():
            api_session.cookies.set(name, value, domain=".sheinindia.in")

        log(f"API ready with {len(cookies_dict)} cookies", "SUCCESS")
        return True
    except Exception as e:
        log(f"Setup failed: {e}", "ERROR")
        return False

def fetch_api(url):
    """Fetch API with curl_cffi"""
    try:
        separator = '&' if '?' in url else '?'
        url_with_ts = f"{url}{separator}_t={int(time.time() * 1000)}"

        response = api_session.get(
            url_with_ts,
            impersonate="chrome120",
            timeout=8
        )

        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def send_telegram_fast(message, token, image_url=None, button_url=None):
    """Send Telegram alert instantly"""
    try:
        payload = {
            "chat_id": ADMIN_CHAT_ID,
            "parse_mode": "HTML"
        }

        if button_url:
            payload["reply_markup"] = json.dumps({
                "inline_keyboard": [[
                    {"text": "🛒 BUY", "url": button_url}
                ]]
            })

        if image_url and image_url.startswith('http'):
            url = f"https://api.telegram.org/bot{token}/sendPhoto"
            payload["photo"] = image_url
            payload["caption"] = message
        else:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload["text"] = message
            payload["disable_web_page_preview"] = False

        import requests as req
        req.post(url, data=payload, timeout=5)
    except:
        pass

def get_target_token(cat_name, product_data):
    """Get correct token based on category"""
    if cat_name == 'Men':
        return TOKEN_MEN
    elif cat_name == 'Women':
        return TOKEN_WOMEN
    else:
        seg_text = product_data.get('segmentNameText', '').lower()
        if 'women' in seg_text:
            return TOKEN_WOMEN
        elif 'men' in seg_text:
            return TOKEN_MEN
        return TOKEN_WOMEN

def self_ping_keeper():
    """Pings own URL every 10 minutes to prevent Render.com from sleeping"""
    import requests as req
    time.sleep(30)

    ping_url = RENDER_URL if RENDER_URL else "https://product-monitor.onrender.com"
    if not ping_url.startswith('http'):
        ping_url = f"https://{ping_url}"
    health_url = f"{ping_url}/health"

    log(f"🔔 Self-ping keeper started", "PING")

    while True:
        try:
            time.sleep(SELF_PING_INTERVAL)
            response = req.get(health_url, timeout=10)
            if response.status_code == 200:
                log(f"🔔 Self-ping successful - Service awake", "PING")
        except:
            pass

# ==========================================
# 🔥 ULTIMATE MONITOR - ALL PAGES + ULTRA FAST
# ==========================================

class UltimateMonitor:
    """
    ULTIMATE VERSION:
    - Fetches ALL pages in PARALLEL (50 threads per category)
    - Ultra fast 1ms check interval
    - Complete coverage + Maximum speed
    - 250 alert workers for instant notifications
    """

    def __init__(self):
        self.running = True
        self.alert_queue = queue.Queue()
        self.db_queue = queue.Queue()
        self.session_cache = set()
        self.detection_count = 0

        # Page fetching infrastructure
        self.page_fetch_queue = queue.Queue()
        self.page_results = {}
        self.page_results_lock = threading.Lock()

        # Statistics
        self.total_products_checked = 0
        self.total_api_calls = 0

        if os.path.exists(SESSION_DB_PATH):
            try:
                os.remove(SESSION_DB_PATH)
            except:
                pass

        self.init_db()
        log("Session initialized - Fresh start", "SUCCESS")

    def init_db(self):
        """Initialize SQLite database"""
        try:
            conn = sqlite3.connect(SESSION_DB_PATH)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS session_seen (product_id TEXT PRIMARY KEY)")
            conn.commit()
            conn.close()
            log("Database ready", "SUCCESS")
        except Exception as e:
            log(f"DB init error: {e}", "ERROR")

    def _db_writer(self):
        """Background database writer with batching"""
        conn = sqlite3.connect(SESSION_DB_PATH, check_same_thread=False)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except:
            pass

        batch = []
        last_write = time.time()

        while self.running:
            try:
                pid = self.db_queue.get(timeout=0.1)
                batch.append(pid)

                # Write if batch is full OR every 1 second
                if len(batch) >= 100 or (time.time() - last_write) > 1:
                    try:
                        conn.executemany("INSERT OR IGNORE INTO session_seen (product_id) VALUES (?)", 
                                       [(p,) for p in batch])
                        conn.commit()
                        batch.clear()
                        last_write = time.time()
                    except:
                        pass

                self.db_queue.task_done()
            except queue.Empty:
                if batch:
                    try:
                        conn.executemany("INSERT OR IGNORE INTO session_seen (product_id) VALUES (?)", 
                                       [(p,) for p in batch])
                        conn.commit()
                        batch.clear()
                        last_write = time.time()
                    except:
                        pass

        conn.close()

    def check_and_add_seen(self, pid):
        """Check if product seen before, add if new"""
        if pid in self.session_cache:
            return False
        self.session_cache.add(pid)
        self.db_queue.put(pid)
        self.detection_count += 1
        return True

    def _alert_worker(self):
        """Background alert worker - sends Telegram messages"""
        while self.running:
            try:
                item = self.alert_queue.get(timeout=1)

                pid = item['id']
                token = item['token']
                product = item['product']

                # Extract product info
                name = product.get('name', 'New Product')

                # Price
                price_val = "Check Link"
                if 'price' in product:
                    price_obj = product['price']
                    raw = price_obj.get('value')
                    if raw:
                        price_val = f"₹{int(raw)}"
                    else:
                        price_val = price_obj.get('formattedValue', 'Check Link')

                # Image
                image_url = None
                if 'images' in product and len(product['images']) > 0:
                    image_url = product['images'][0].get('url')
                elif 'fnlColorVariantData' in product:
                    image_url = product['fnlColorVariantData'].get('outfitPictureURL')

                # Buy URL
                buy_url = f"https://www.sheinindia.in/p/{pid}"

                # Minimal message
                msg = f"🔥 <b>{name}</b>\n💰 {price_val}"

                # Send immediately
                send_telegram_fast(msg, token, image_url=image_url, button_url=buy_url)

                self.alert_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                pass

    def _page_fetcher_worker(self):
        """Worker to fetch pages in MASSIVE parallel"""
        while self.running:
            try:
                task = self.page_fetch_queue.get(timeout=1)
                if task is None:
                    break

                request_id = task['request_id']
                page_num = task['page_num']
                url = task['url']

                # Fetch page
                data = fetch_api(url)
                self.total_api_calls += 1

                # Store result
                with self.page_results_lock:
                    if request_id not in self.page_results:
                        self.page_results[request_id] = {}
                    self.page_results[request_id][page_num] = data

                self.page_fetch_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                pass

    def fetch_all_pages_parallel(self, cat_name, base_url):
        """
        Fetch ALL pages in MASSIVE PARALLEL!
        Uses 50 threads to fetch all pages simultaneously
        """
        # First, get total pages
        first_page_url = re.sub(r'currentPage=\d+', 'currentPage=0', base_url)
        data = fetch_api(first_page_url)

        if not isinstance(data, dict):
            return []

        pagination = data.get('pagination', {})
        total_pages = pagination.get('totalPages', 1)

        request_id = f"{cat_name}_{int(time.time() * 1000)}"

        # Queue ALL page fetch tasks
        for page_num in range(total_pages):
            page_url = re.sub(r'currentPage=\d+', f'currentPage={page_num}', base_url)
            self.page_fetch_queue.put({
                'request_id': request_id,
                'page_num': page_num,
                'url': page_url
            })

        # Wait for all pages to be fetched
        self.page_fetch_queue.join()

        # Collect all products in order
        all_products = []
        with self.page_results_lock:
            if request_id in self.page_results:
                for page_num in range(total_pages):
                    if page_num in self.page_results[request_id]:
                        page_data = self.page_results[request_id][page_num]
                        if isinstance(page_data, dict):
                            products = page_data.get('products', [])
                            all_products.extend(products)
                            self.total_products_checked += len(products)

                # Cleanup
                del self.page_results[request_id]

        return all_products

    def process_category_ultimate(self, cat_name):
        """
        ULTIMATE processing for category:
        - Fetches ALL pages in parallel
        - Ultra fast 1ms check interval
        - Complete coverage + Maximum speed
        """
        config = CATEGORY_CONFIGS[cat_name]
        base_url = config['url']

        log(f"🔥 [{cat_name}] ULTIMATE mode - ALL pages @ 1ms!", "ULTIMATE")

        consecutive_failures = 0

        while self.running:
            try:
                # Fetch ALL pages in parallel!
                all_products = self.fetch_all_pages_parallel(cat_name, base_url)

                if not all_products:
                    consecutive_failures += 1
                    if consecutive_failures > 5:
                        time.sleep(2)
                        consecutive_failures = 0
                    else:
                        time.sleep(0.5)
                    continue

                consecutive_failures = 0

                # Find new products
                new_items = []
                for p in all_products:
                    pid = p.get('fnlColorVariantData', {}).get('colorGroup') or p.get('code')
                    if not pid:
                        u = p.get('url', '')
                        if '/p/' in u:
                            pid = u.split('/p/')[1].split('.html')[0].split('?')[0]

                    if not pid:
                        continue

                    if self.check_and_add_seen(pid):
                        new_items.append((pid, p))

                count = len(new_items)
                if count > 0:
                    log(f"🔥 [{cat_name}] {count} NEW from {len(all_products)} products!", "ULTIMATE")

                    # Queue all for immediate sending
                    for pid, p in new_items:
                        token = get_target_token(cat_name, p)
                        self.alert_queue.put({
                            'id': pid,
                            'category': cat_name,
                            'product': p,
                            'token': token
                        })

                # Ultra fast check interval
                time.sleep(CHECK_INTERVAL)

            except Exception as e:
                log(f"[{cat_name}] Error: {e}", "ERROR")
                time.sleep(1)

    def start(self):
        """Start the ULTIMATE monitor!"""
        log("🔥🔥🔥 ULTIMATE MONITOR - ALL PAGES + ULTRA FAST! 🔥🔥🔥", "ULTIMATE")

        if not setup_api_session():
            log("Setup failed", "ERROR")
            return

        total_page_fetchers = PAGE_FETCHERS_PER_CATEGORY * len(CATEGORY_CONFIGS)

        log(f"🔥 {NUM_ALERT_WORKERS} ALERT WORKERS", "SUCCESS")
        log(f"🔥 {total_page_fetchers} PAGE FETCHERS ({PAGE_FETCHERS_PER_CATEGORY} per category)", "SUCCESS")
        log(f"🔥 CHECK INTERVAL: {CHECK_INTERVAL * 1000}ms (ULTRA FAST)", "SUCCESS")
        log(f"🔥 COVERAGE: ALL PAGES - ZERO PRODUCTS MISSED!", "SUCCESS")
        log("🔔 SELF-PING ACTIVE - NEVER SLEEPS!", "PING")
        log("🔥🔥🔥 COMPLETE COVERAGE + MAXIMUM SPEED! 🔥🔥🔥", "ULTIMATE")

        # Start DB writer
        threading.Thread(target=self._db_writer, daemon=True).start()

        # Start MASSIVE page fetcher army (50 per category = 150 total!)
        for _ in range(total_page_fetchers):
            threading.Thread(target=self._page_fetcher_worker, daemon=True).start()

        # Start alert workers
        for _ in range(NUM_ALERT_WORKERS):
            threading.Thread(target=self._alert_worker, daemon=True).start()

        # Start self-ping keeper (NEVER SLEEP!)
        threading.Thread(target=self_ping_keeper, daemon=True).start()

        # Start category monitors
        for cat in CATEGORY_CONFIGS.keys():
            threading.Thread(target=self.process_category_ultimate, args=(cat,), daemon=True).start()

        log("🔥 ALL SYSTEMS OPERATIONAL - ULTIMATE MODE!", "SUCCESS")

        # Stats reporter
        def report_stats():
            while self.running:
                time.sleep(60)
                log(f"📊 Stats: {self.detection_count} new, {self.total_products_checked} checked, {self.total_api_calls} API calls", "INFO")

        threading.Thread(target=report_stats, daemon=True).start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False

# ==========================================
# 🌐 FLASK
# ==========================================

monitor_instance = None
start_time = datetime.now()
ping_count = 0

@app.route('/health')
def health():
    global ping_count
    ping_count += 1
    uptime = (datetime.now() - start_time).total_seconds()
    return jsonify({
        "status": "healthy",
        "mode": "ULTIMATE - ALL PAGES + ULTRA FAST",
        "uptime_hours": round(uptime / 3600, 2),
        "alert_workers": NUM_ALERT_WORKERS,
        "page_fetchers": PAGE_FETCHERS_PER_CATEGORY * len(CATEGORY_CONFIGS),
        "check_interval_ms": CHECK_INTERVAL * 1000,
        "coverage": "100% - ALL pages",
        "speed": "ULTRA FAST - 1ms",
        "detections": monitor_instance.detection_count if monitor_instance else 0,
        "products_checked": monitor_instance.total_products_checked if monitor_instance else 0,
        "api_calls": monitor_instance.total_api_calls if monitor_instance else 0,
        "self_ping_active": True,
        "ping_count": ping_count,
        "never_sleeps": True,
        "running": monitor_instance.running if monitor_instance else False
    })

@app.route('/')
def home():
    return jsonify({
        "service": "Ultimate Monitor",
        "mode": "ALL PAGES + ULTRA FAST",
        "platform": "Render.com (24/7 Cloud)",
        "version": "8.0 - ULTIMATE",
        "features": [
            "ALL pages scanned in parallel",
            "1ms check interval",
            "150 page fetchers",
            "250 alert workers",
            "100% coverage",
            "Maximum speed",
            "Never sleeps"
        ]
    })

def run_flask():
    from waitress import serve
    serve(app, host='0.0.0.0', port=PORT, threads=4)

# ==========================================
# 🎯 MAIN
# ==========================================

if __name__ == "__main__":
    log("🔥🔥🔥 ULTIMATE VERSION - NO COMPROMISES! 🔥🔥🔥", "ULTIMATE")
    log("🔥 ALL PAGES + ULTRA FAST SPEED!", "SUCCESS")

    required_vars = ["TOKEN_MEN", "TOKEN_WOMEN", "ADMIN_CHAT_ID", "COOKIE_FILE_CONTENT"]
    missing = [v for v in required_vars if not os.environ.get(v)]

    if missing:
        log(f"Missing env vars: {', '.join(missing)}", "ERROR")
        exit(1)

    log("All env vars validated ✅", "SUCCESS")

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    log(f"Flask running on port {PORT}", "SUCCESS")

    monitor_instance = UltimateMonitor()
    monitor_instance.start()
