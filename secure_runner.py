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
import base64
import sys
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from DrissionPage import ChromiumPage, ChromiumOptions

# ==========================================
# 🔐 SECURE CONFIGURATION
# ==========================================

sys.stdout.reconfigure(encoding='utf-8')

def d(s):
    return base64.b64decode(s).decode('utf-8')

# Secrets Load
TOKEN_1 = os.environ.get("S_KEY_1")
TOKEN_2 = os.environ.get("S_KEY_2")
ADMIN_ID = os.environ.get("A_ID")
RAW_COOKIES = os.environ.get("COOKIES_JSON")

# Check Credentials at Startup
if not TOKEN_1 or not TOKEN_2 or not ADMIN_ID:
    print("❌ CRITICAL ERROR: Telegram Tokens or Admin ID missing in Secrets!", flush=True)

DOMAIN_ENC = "d3d3LnNoZWluaW5kaWEuaW4="
API_CAT_ENC = "L2FwaS9jYXRlZ29yeS9zdmVyc2UtNTkzOS0zNzk2MQ=="
API_P_ENC = "L2FwaS9wLw=="

BASE_DOMAIN = d(DOMAIN_ENC)
CAT_API = d(API_CAT_ENC)
PROD_API = d(API_P_ENC)

SESSION_DB_PATH = "secure_session.db"
CHECK_INTERVAL = 0.05
COOKIE_FILE = "runtime_auth.json"

# 🔥 OPTIMIZED FOR GITHUB SERVER (Prevent Crashing)
NUM_WORKERS = 30  
BURST_THRESHOLD = 15

MAX_RUNTIME = 5 * 3600 + 50 * 60 

URL_PARAMS_UNI = "?fields=SITE&currentPage=0&pageSize=45&format=json&query=%3Arelevance&gridColumns=5&segmentIds=23%2C14%2C18%2C9&cohortIds=value%7Cmen%2CTEMP_M1_LL_FG_NOV&customerType=Existing&facets=&customertype=Existing&advfilter=true&platform=Desktop&showAdsOnNextPage=false&is_ads_enable_plp=true&displayRatings=true&segmentIds=&&store=shein"
URL_PARAMS_W = "?fields=SITE&currentPage=0&pageSize=45&format=json&query=%3Arelevance%3Agenderfilter%3AWomen&gridColumns=5&segmentIds=23%2C14%2C18%2C9&cohortIds=value%7Cmen%2CTEMP_M1_LL_FG_NOV&customerType=Existing&facets=genderfilter%3AWomen&customertype=Existing&advfilter=true&platform=Desktop&showAdsOnNextPage=false&is_ads_enable_plp=true&displayRatings=true&segmentIds=&&store=shein"
URL_PARAMS_M = "?fields=SITE&currentPage=0&pageSize=45&format=json&query=%3Arelevance%3Agenderfilter%3AMen&gridColumns=5&segmentIds=23%2C14%2C18%2C9&cohortIds=value%7Cmen%2CTEMP_M1_LL_FG_NOV&customerType=Existing&facets=genderfilter%3AMen&customertype=Existing&advfilter=true&platform=Desktop&showAdsOnNextPage=false&is_ads_enable_plp=true&displayRatings=true&segmentIds=&&store=shein"

CATEGORY_CONFIGS = {
    'Universal': {'url': f"https://{BASE_DOMAIN}{CAT_API}{URL_PARAMS_UNI}", 'tab': None},
    'TypeA':     {'url': f"https://{BASE_DOMAIN}{CAT_API}{URL_PARAMS_W}", 'tab': None}, 
    'TypeB':     {'url': f"https://{BASE_DOMAIN}{CAT_API}{URL_PARAMS_M}", 'tab': None}  
}

tg_session = requests.Session()
retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
tg_session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100))

# ==========================================
# 🛠️ UTILITY FUNCTIONS
# ==========================================

def log(msg):
    try: print(msg, flush=True)
    except: pass

def send_signal(message, token, image_url=None, button_url=None):
    if not token or not ADMIN_ID:
        log("❌ Signal Failed: Missing Token or Admin ID")
        return
    try:
        payload = {"chat_id": ADMIN_ID, "parse_mode": "HTML"}
        if button_url:
            payload["reply_markup"] = json.dumps({"inline_keyboard": [[{"text": "🛍️ VIEW ITEM", "url": button_url}]]})

        if image_url:
            url = f"https://api.telegram.org/bot{token}/sendPhoto"
            payload["photo"] = image_url
            payload["caption"] = message
        else:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload["text"] = message
            payload["disable_web_page_preview"] = False
        
        # Increased timeout for stability
        resp = tg_session.post(url, data=payload, timeout=45)
        if resp.status_code != 200:
            log(f"⚠️ TG API Error: {resp.text}")

    except Exception as e:
        log(f"⚠️ Signal Network Error: {e}")

def resolve_token(cat_name, p_data):
    if cat_name == 'TypeB': return TOKEN_1
    elif cat_name == 'TypeA': return TOKEN_2
    else:
        seg = p_data.get('segmentNameText', '').lower()
        if 'men' in seg and 'women' not in seg: return TOKEN_1
        return TOKEN_2

# ==========================================
# 🚀 SECURE MONITOR CORE
# ==========================================

class SecureMonitor:
    def __init__(self):
        self.browser = None
        self.running = True
        self.q = queue.Queue()
        self.db_q = queue.Queue()
        self.cache = set()
        self.start_ts = time.time()
        
        # 🔥 RAM LOGIC: Fresh Start
        if os.path.exists(SESSION_DB_PATH):
            try:
                os.remove(SESSION_DB_PATH)
                log("🗑️ RAM Cleared: Starting Fresh Session.")
            except: pass
        self.init_storage()

        if RAW_COOKIES:
            try:
                with open(COOKIE_FILE, 'w') as f:
                    f.write(RAW_COOKIES)
                log("🍪 Cookies Loaded.")
            except: pass

    def check_time(self):
        elapsed = time.time() - self.start_ts
        if elapsed > MAX_RUNTIME:
            log(f"🛑 Time Limit Reached. Restarting...")
            self.running = False
            return False
        return True

    def init_storage(self):
        try:
            conn = sqlite3.connect(SESSION_DB_PATH)
            conn.cursor().execute("CREATE TABLE IF NOT EXISTS seen_items (pid TEXT PRIMARY KEY)")
            conn.commit()
            conn.close()
        except: pass

    def _storage_worker(self):
        conn = sqlite3.connect(SESSION_DB_PATH, check_same_thread=False)
        try: conn.execute("PRAGMA journal_mode=WAL;")
        except: pass
        while self.running:
            try:
                pid = self.db_q.get(timeout=1)
                try:
                    conn.execute("INSERT OR IGNORE INTO seen_items (pid) VALUES (?)", (pid,))
                    conn.commit()
                except: pass
                self.db_q.task_done()
            except queue.Empty: continue
        conn.close()

    def is_new(self, pid):
        if pid in self.cache: return False
        self.cache.add(pid)
        self.db_q.put(pid)
        return True

    def get_opts(self, port):
        co = ChromiumOptions()
        co.set_local_port(port)
        co.set_user_data_path(os.path.join(tempfile.gettempdir(), f"secure_profile_{port}"))
        co.set_argument('--no-sandbox')
        co.set_argument('--disable-dev-shm-usage')
        co.set_argument('--disable-gpu')
        co.set_argument('--mute-audio')
        co.set_argument('--blink-settings=imagesEnabled=false')
        return co

    def launch(self):
        log("🚀 Launching Browser...")
        port = random.randint(30001, 40000)
        co = self.get_opts(port)
        self.browser = ChromiumPage(co)
        
        home = f"https://{BASE_DOMAIN}/"
        
        if os.path.exists(COOKIE_FILE):
            try:
                with open(COOKIE_FILE, 'r') as f:
                    c_data = json.load(f)
                    self.browser.get(home)
                    self.browser.set.cookies(c_data)
                    self.browser.refresh()
                    time.sleep(2)
                    log("✅ Auth Success.")
            except Exception as e:
                log(f"⚠️ Auth Error: {e}")

        CATEGORY_CONFIGS['Universal']['tab'] = self.browser.latest_tab
        for cat in ['TypeA', 'TypeB']:
            CATEGORY_CONFIGS[cat]['tab'] = self.browser.new_tab(home)
            time.sleep(0.5)
        return True

    def js_fetch(self, tab, url):
        js = f"""
            return fetch("{url}&_t=" + Date.now(), {{
                headers: {{'Cache-Control': 'no-cache', 'Pragma': 'no-cache'}}
            }}).then(res => {{
                if(res.status === 403) return "403";
                if(!res.ok) return "ERR_" + res.status;
                return res.json();
            }}).catch(err => "ERR_NETWORK");
        """
        try: return tab.run_js(js, timeout=12)
        except: return None

    # 🔥 BURST (Basic Info)
    def fast_alert(self, items, token):
        log(f"⚡ Bursting {len(items)} items...")
        for p in items:
            try:
                pid = p.get('fnlColorVariantData', {}).get('colorGroup') or p.get('code')
                name = p.get('name', 'New Item')
                link = f"https://{BASE_DOMAIN}/p/{pid}"
                img = p.get('url', '')
                if 'images' in p and p['images']: img = p['images'][0].get('url')
                
                msg = f"⚠️ <b>FAST ALERT</b>\n📦 {name}\n🆔 <code>{pid}</code>\n<i>Fetching details...</i>"
                send_signal(msg, token, image_url=img, button_url=link)
                time.sleep(0.1) # Safe throttle
            except: pass

    # 🔥 DETAILED PROCESSOR
    def _processor(self):
        while self.running:
            try:
                task = self.q.get(timeout=1)
                full_d = None
                api_url = f"https://{BASE_DOMAIN}{PROD_API}{task['id']}?fields=SITE"
                tabs = [v['tab'] for v in CATEGORY_CONFIGS.values() if v['tab']]
                use_tab = random.choice(tabs) if tabs else CATEGORY_CONFIGS['Universal']['tab']
                
                # Robust Retry
                for _ in range(5):
                    full_d = self.js_fetch(use_tab, api_url)
                    if isinstance(full_d, dict): break
                    time.sleep(1.5)
                
                # Proceed even if fetch failed (send basic data)
                self.compose_alert(task['id'], full_d, task['burst'], task['tkn'], task['base'])
                self.q.task_done()
            except queue.Empty: continue
            except Exception as e:
                log(f"❌ Worker Error: {e}")

    def get_img(self, data):
        try:
            return data.get('selected', {}).get('modelImage', {}).get('url') or \
                   data['baseOptions'][0]['options'][0]['modelImage']['url'] or \
                   data['images'][0]['url']
        except: return None

    def compose_alert(self, pid, data, burst, tkn, b_data):
        try:
            link = f"https://{BASE_DOMAIN}/p/{pid}"
            ts = datetime.now().strftime('%H:%M:%S')
            if data:
                name = data.get('name', 'Item')
                price_d = data.get('offerPrice') or data.get('price')
                price = f"₹{int(price_d['value'])}" if price_d and price_d.get('value') else "N/A"
                img = self.get_img(data)
                s_list = []
                for v in data.get('variantOptions', []):
                    qs = v.get('variantOptionQualifiers', [])
                    sz = next((q['value'] for q in qs if q['qualifier'] in ['size', 'standardSize']), 'N/A')
                    stk = v.get('stock', {}).get('stockLevel', 0)
                    stat = v.get('stock', {}).get('stockLevelStatus', '')
                    if stat != 'outOfStock' and stk > 0: s_list.append(f"✅ <b>{sz}</b> : {stk}")
                    elif stat == 'inStock': s_list.append(f"✅ <b>{sz}</b> : In Stock")
                    else: s_list.append(f"❌ {sz} : Out")
                stk_txt = "\n".join(s_list) if s_list else "⚠️ Stock Check"
            else:
                name = b_data.get('name', 'Item')
                price = "Check Link"
                try: img = b_data['images'][0]['url']
                except: img = None
                stk_txt = "⚠️ Details Fetch Failed"

            head = "<b>📦 STOCK INFO</b>" if burst else "<b>🔥 NEW ARRIVAL</b>"
            
            msg = f"{head}\n\nDf <b>{name}</b>\n💰 <b>{price}</b>\n\n📏 <b>Status:</b>\n<pre>{stk_txt}</pre>\n\n⚡ Time: {ts}"
            send_signal(msg, tkn, image_url=img, button_url=link)
            log(f"✅ Alert Sent: {pid}")
        except Exception as e:
            log(f"❌ Alert Gen Error: {e}")

    def scanner(self, cat_key):
        cfg = CATEGORY_CONFIGS[cat_key]
        base_url = cfg['url']
        
        while self.running:
            if not self.check_time(): break
            
            try:
                # 1. Page 0 Scan
                first_page_url = re.sub(r'currentPage=\d+', 'currentPage=0', base_url)
                data = self.js_fetch(cfg['tab'], first_page_url)
                
                if data == "403":
                    time.sleep(5); continue
                if not isinstance(data, dict):
                    time.sleep(1); continue

                # 2. Pagination Logic
                pagination = data.get('pagination', {})
                total_pages = pagination.get('totalPages', 1)
                
                all_products = []
                
                # 3. Collect ALL Items (Page 0 to N)
                for page_num in range(total_pages):
                    if page_num == 0:
                        page_products = data.get('products', [])
                    else:
                        page_url = re.sub(r'currentPage=\d+', f'currentPage={page_num}', base_url)
                        page_data = self.js_fetch(cfg['tab'], page_url)
                        if isinstance(page_data, dict):
                            page_products = page_data.get('products', [])
                        else:
                            page_products = []
                    
                    all_products.extend(page_products)

                # 4. Filter New
                new_session_items = []
                for p in all_products:
                    pid = p.get('fnlColorVariantData', {}).get('colorGroup') or p.get('code')
                    if not pid and '/p/' in p.get('url', ''):
                        pid = p['url'].split('/p/')[1].split('.html')[0].split('?')[0]
                    if pid and self.is_new(pid):
                        new_session_items.append(p)

                count = len(new_session_items)
                if count > 0:
                    log(f"✨ [{cat_key}] Detected {count} items (Pages: {total_pages})")
                    
                    do_burst = count <= BURST_THRESHOLD
                    token_pairs = [(p, resolve_token(cat_key, p)) for p in new_session_items]

                    # BURST (Fast Messages)
                    if do_burst:
                        t1_items = [x[0] for x in token_pairs if x[1] == TOKEN_1]
                        t2_items = [x[0] for x in token_pairs if x[1] == TOKEN_2]
                        if t1_items: threading.Thread(target=self.fast_alert, args=(t1_items, TOKEN_1)).start()
                        if t2_items: threading.Thread(target=self.fast_alert, args=(t2_items, TOKEN_2)).start()
                    else:
                        log(f"⚠️ Large Batch ({count}). Sending Detailed Alerts Directly.")

                    # QUEUE (Detailed Messages - ALWAYS RUNS)
                    for p, tkn in token_pairs:
                        pid = p.get('fnlColorVariantData', {}).get('colorGroup') or p.get('code')
                        self.q.put({'id': pid, 'cat': cat_key, 'burst': do_burst, 'base': p, 'tkn': tkn})

                time.sleep(CHECK_INTERVAL)
            except Exception as e:
                log(f"❌ Scanner Error [{cat_key}]: {e}")
                time.sleep(2)

    def run(self):
        if not self.launch(): return
        log(f"🚀 Started. Workers: {NUM_WORKERS}. Max Runtime: 5h 50m")
        
        threading.Thread(target=self._storage_worker, daemon=True).start()
        for _ in range(NUM_WORKERS):
            threading.Thread(target=self._processor, daemon=True).start()
        for k in CATEGORY_CONFIGS.keys():
            threading.Thread(target=self.scanner, args=(k,), daemon=True).start()
            
        try:
            while self.running:
                if not self.check_time(): break
                time.sleep(1)
        except: pass
        
        self.running = False
        if self.browser: self.browser.quit()
        log("👋 Service Shutdown.")

if __name__ == "__main__":
    monitor = SecureMonitor()
    monitor.run()
