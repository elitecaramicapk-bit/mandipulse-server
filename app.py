# ================================================================
# MANDIPULSE SERVER — app.py FINAL v9.0
# Complete Production-Ready Server
# ================================================================
#
# FEATURES:
# 1. 20 States x Local Crops + Price Ranges
# 2. 6 Data Sources (GOV API, Agmarknet, eNAM, Local, RSS, SOPA)
# 3. Fake Price Detection (Multi-Mandi Cross-Check)
# 4. 3-Day SQLite Price History
# 5. Demand-Supply Analysis with Arrival Data
# 6. Auto-Discovery Live List (Poll every 30s)
# 7. Smart RAM Cache (4hr fresh, 6hr stale)
# 8. Background Auto-Refresh (every 4 hours)
# 9. Seasonal Intelligence (Last Season vs This Season)
# 10. 30/60/90 Day Price Predictions
# 11. Startup Pre-load (Top mandis auto-loaded)
#
# ENDPOINTS:
# GET  /                              → Server info
# GET  /health                        → Health check
# GET  /api/bulk-predictions          → All crops for a state/city
# GET  /api/mandi-predictions         → Single commodity price
# GET  /api/live-list                 → Auto-discovery list
# GET  /api/live-list/poll            → Poll for updates (30s interval)
# POST /api/scan-mandi                → Force scan all crops
# GET  /api/seasonal-analysis         → Full seasonal + weather analysis
# GET  /api/market-outlook            → All crops outlook summary
# GET  /api/price-history             → 3-day chart data
# GET  /api/state-crops               → Crops for a state
# GET  /api/all-states                → All 20 states
# GET  /api/markets                   → Mandis for a state
# GET  /api/cache-status              → Cache info
# POST /api/cache-clear               → Clear cache
# GET  /api/mandipulse/dashboard      → Dashboard data
# GET  /api/mandipulse/calculate      → Calculate endpoint
# ================================================================

from fastapi import FastAPI, Query, BackgroundTasks, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import feedparser
import re
import logging
import concurrent.futures
import threading
import time
import sqlite3
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("MandiPulse")

# ── Simple in-memory rate limiter ────────────────────────────────
# Per IP: max 30 requests per minute on heavy endpoints
_rate_data: Dict[str, list] = {}   # ip -> [timestamps]
_rate_lock = threading.Lock()

def rate_check(ip: str, max_req: int = 30, window: int = 60) -> bool:
    """Returns True if allowed, False if rate limited."""
    now = time.time()
    with _rate_lock:
        times = _rate_data.get(ip, [])
        # Remove old timestamps outside window
        times = [t for t in times if now - t < window]
        if len(times) >= max_req:
            return False
        times.append(now)
        _rate_data[ip] = times
        # Cleanup old IPs every ~500 calls
        if len(_rate_data) > 500:
            old_ips = [k for k, v in _rate_data.items()
                       if not v or (now - v[-1]) > window * 2]
            for k in old_ips:
                del _rate_data[k]
        return True

# ── Global thread pool (reuse karo, har request pe naya mat banao) ──
_GLOBAL_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=12,   # Total 12 threads shared across all requests
    thread_name_prefix="mp"
)

# ── Active request counter (RAM guard) ───────────────────────────
_active_requests = 0
_active_lock     = threading.Lock()
MAX_CONCURRENT   = 10   # Max 10 requests at same time

app = FastAPI(title="MandiPulse API", version="9.0")
app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.middleware("http")
async def overload_guard(request: Request, call_next):
    """SCALE FIX: Reject requests when server is overloaded."""
    global _active_requests
    # Skip guard for health check
    if request.url.path in ("/health", "/"):
        return await call_next(request)
    with _active_lock:
        if _active_requests >= MAX_CONCURRENT:
            return JSONResponse(
                status_code=503,
                content={"error": "Server busy", "message": "थोड़ी देर बाद try करें", "retry_after": 5}
            )
        _active_requests += 1
    try:
        response = await call_next(request)
        return response
    finally:
        with _active_lock:
            _active_requests = max(0, _active_requests - 1)

HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
       "Accept-Encoding": "gzip, deflate",
       "Connection": "keep-alive"}
DATA_GOV_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"

# ── Shared HTTP Session with connection pool (TCP reuse = fast) ──
def _make_session():
    s = requests.Session()
    retry = Retry(total=1, backoff_factor=0.1,
                  status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry,
                          pool_connections=10, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HDR)
    return s

SESSION = _make_session()  # Global shared session
import os as _os
DB_PATH = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "mandipulse_v9.db")

# ================================================================
# SECTION 1: STATIC DATA
# ================================================================

HINDI = {
    "Wheat": "गेहूँ", "Rice": "चावल", "Paddy": "धान",
    "Onion": "प्याज", "Potato": "आलू", "Tomato": "टमाटर",
    "Garlic": "लहसुन", "Mustard": "सरसों", "Soybean": "सोयाबीन",
    "Cotton": "कपास", "Maize": "मक्का", "Chana": "चना",
    "Moong": "मूँग", "Urad": "उड़द", "Arhar": "अरहर",
    "Masoor": "मसूर", "Jeera": "जीरा", "Coriander": "धनिया",
    "Fennel": "सौंफ", "Methi": "मेथी", "Groundnut": "मूँगफली",
    "Barley": "जौ", "Jowar": "ज्वार", "Bajra": "बाजरा",
    "Sugarcane": "गन्ना", "Guar": "ग्वार", "Castor": "अरंडी",
    "Taramira": "तारामीरा", "Moath": "मोठ", "Turmeric": "हल्दी",
    "Chilli": "मिर्च", "Ginger": "अदरक", "Banana": "केला",
    "Coconut": "नारियल", "Coffee": "कॉफी", "Sunflower": "सूरजमुखी",
    "Sesame": "तिल", "Jute": "पटसन", "Ragi": "रागी",
    "Apple": "सेब", "Tobacco": "तम्बाकू", "Isabgul": "इसबगोल",
    "Ajwain": "अजवाइन", "Niger Seed": "रामतिल", "Linseed": "अलसी",
    "Peas": "मटर", "Walnut": "अखरोट", "Tea": "चाय",
}

MSP = {
    "Wheat": 2425, "Paddy": 2300, "Rice": 2300, "Maize": 2090,
    "Jowar": 3371, "Bajra": 2625, "Ragi": 4290, "Barley": 1735,
    "Chana": 5440, "Moong": 8780, "Urad": 7400, "Arhar": 7550,
    "Masoor": 6700, "Mustard": 5950, "Soybean": 4892,
    "Cotton": 7121, "Groundnut": 6783, "Sunflower": 7280,
    "Sesame": 9267, "Sugarcane": 340, "Jute": 5335,
}

NEARBY_MANDIS = {
    "Nagaur":    ["Jodhpur", "Merta City", "Bikaner", "Degana"],
    "Jodhpur":   ["Nagaur", "Barmer", "Pali", "Bilara"],
    "Jaipur":    ["Alwar", "Tonk", "Dausa", "Sikar"],
    "Indore":    ["Ujjain", "Dewas", "Sehore", "Bhopal"],
    "Bhopal":    ["Indore", "Sehore", "Vidisha", "Raisen"],
    "Ujjain":    ["Indore", "Dewas", "Ratlam", "Mandsaur"],
    "Nashik":    ["Pune", "Lasalgaon", "Nanded", "Aurangabad"],
    "Ludhiana":  ["Amritsar", "Patiala", "Jalandhar", "Moga"],
    "Karnal":    ["Hisar", "Panipat", "Sonipat", "Kaithal"],
    "Unjha":     ["Mehsana", "Patan", "Rajkot", "Ahmedabad"],
    "Guntur":    ["Kurnool", "Vijayawada", "Ongole", "Nellore"],
    "Hubli":     ["Belgaum", "Davangere", "Raichur", "Bijapur"],
}

STATE_DATA = {
    "Rajasthan": {
        "crops": ["Mustard","Wheat","Chana","Moong","Guar","Jeera",
                  "Fennel","Taramira","Barley","Bajra","Coriander",
                  "Castor","Isabgul","Ajwain","Moath","Groundnut",
                  "Maize","Sesame","Onion","Garlic"],
        "mandis": ["Nagaur","Jodhpur","Jaipur","Merta City","Bikaner",
                   "Kota","Barmer","Sikar","Alwar","Sriganganagar",
                   "Hanumangarh","Tonk","Bundi","Chittorgarh","Pali"],
        "ranges": {
            "Mustard": (5000,10000), "Wheat": (1800,4200),
            "Chana": (4000,9000), "Moong": (5000,14000),
            "Guar": (3500,9000), "Jeera": (15000,80000),
            "Fennel": (6000,30000), "Taramira": (4000,9000),
            "Barley": (1200,3500), "Bajra": (1500,4500),
            "Coriander": (5000,25000), "Castor": (5000,12000),
            "Isabgul": (8000,25000), "Ajwain": (8000,30000),
            "Moath": (3000,8000), "Groundnut": (4500,10000),
            "Maize": (1500,3500), "Sesame": (10000,30000),
            "Onion": (500,8000), "Garlic": (2000,40000),
        }
    },
    "Madhya Pradesh": {
        "crops": ["Soybean","Wheat","Chana","Maize","Arhar","Moong",
                  "Urad","Cotton","Mustard","Masoor","Paddy","Rice",
                  "Garlic","Onion","Coriander","Ajwain"],
        "mandis": ["Indore","Bhopal","Ujjain","Sehore","Dewas",
                   "Ratlam","Mandsaur","Neemuch","Gwalior","Vidisha",
                   "Harda","Khandwa","Sagar","Chhindwara","Jabalpur"],
        "ranges": {
            "Soybean": (4000,8000), "Wheat": (1900,4200),
            "Chana": (4000,9000), "Maize": (1500,3500),
            "Arhar": (5500,10000), "Moong": (5500,12000),
            "Urad": (5000,11000), "Cotton": (5500,10000),
            "Mustard": (5000,9500), "Masoor": (4500,9000),
            "Garlic": (2000,40000), "Onion": (300,8000),
        }
    },
    "Punjab": {
        "crops": ["Wheat","Paddy","Rice","Maize","Cotton","Potato",
                  "Onion","Sugarcane","Arhar","Chana","Mustard","Barley"],
        "mandis": ["Ludhiana","Amritsar","Patiala","Bathinda",
                   "Jalandhar","Firozpur","Moga","Barnala","Sangrur"],
        "ranges": {
            "Wheat": (2000,4500), "Paddy": (1800,4500),
            "Rice": (2000,6000), "Maize": (1500,3500),
            "Cotton": (5500,10000), "Potato": (400,3000),
        }
    },
    "Haryana": {
        "crops": ["Wheat","Paddy","Rice","Mustard","Cotton","Bajra",
                  "Maize","Barley","Sugarcane","Arhar","Chana","Moong"],
        "mandis": ["Karnal","Hisar","Rohtak","Sirsa","Ambala",
                   "Panipat","Sonipat","Fatehabad","Jind","Kaithal"],
        "ranges": {
            "Wheat": (2000,4500), "Paddy": (1800,4500),
            "Mustard": (5000,9500), "Cotton": (5500,10000),
        }
    },
    "Uttar Pradesh": {
        "crops": ["Wheat","Paddy","Rice","Sugarcane","Potato","Onion",
                  "Tomato","Mustard","Arhar","Chana","Moong","Masoor",
                  "Maize","Barley","Garlic"],
        "mandis": ["Agra","Lucknow","Kanpur","Varanasi","Meerut",
                   "Hapur","Mathura","Aligarh","Bareilly","Gorakhpur"],
        "ranges": {
            "Wheat": (1900,4200), "Paddy": (1700,4000),
            "Sugarcane": (280,400), "Potato": (300,2500),
        }
    },
    "Maharashtra": {
        "crops": ["Onion","Soybean","Cotton","Jowar","Bajra","Wheat",
                  "Arhar","Chana","Groundnut","Sugarcane","Rice",
                  "Maize","Turmeric","Chilli","Tomato"],
        "mandis": ["Pune","Nashik","Nagpur","Solapur","Latur",
                   "Lasalgaon","Nanded","Aurangabad","Kolhapur",
                   "Sangli","Akola","Yavatmal","Amravati"],
        "ranges": {
            "Onion": (200,12000), "Soybean": (4000,8000),
            "Cotton": (5500,10000), "Turmeric": (7000,25000),
            "Chilli": (5000,35000), "Tomato": (200,18000),
        }
    },
    "Gujarat": {
        "crops": ["Groundnut","Cotton","Wheat","Bajra","Castor",
                  "Soybean","Sugarcane","Rice","Garlic","Onion",
                  "Jeera","Fennel","Sesame","Isabgul"],
        "mandis": ["Ahmedabad","Rajkot","Surat","Vadodara","Unjha",
                   "Gondal","Junagadh","Amreli","Jamnagar","Mehsana"],
        "ranges": {
            "Groundnut": (4500,10000), "Cotton": (5500,10000),
            "Castor": (5500,12000), "Jeera": (15000,80000),
            "Fennel": (6000,30000), "Isabgul": (8000,25000),
        }
    },
    "Karnataka": {
        "crops": ["Rice","Paddy","Maize","Ragi","Arhar","Moong",
                  "Urad","Groundnut","Sunflower","Cotton","Soybean",
                  "Sugarcane","Onion","Tomato","Chilli","Turmeric","Coconut"],
        "mandis": ["Bangalore","Hubli","Belgaum","Davangere",
                   "Tumkur","Mysore","Gulbarga","Raichur","Bijapur"],
        "ranges": {
            "Rice": (2000,7000), "Ragi": (3000,6000),
            "Turmeric": (7000,25000), "Coconut": (1500,12000),
        }
    },
    "Andhra Pradesh": {
        "crops": ["Rice","Paddy","Maize","Chilli","Cotton","Groundnut",
                  "Tobacco","Sugarcane","Turmeric","Onion","Tomato","Arhar"],
        "mandis": ["Kurnool","Guntur","Nellore","Tirupati","Kadapa",
                   "Ongole","Eluru","Rajahmundry","Vijayawada"],
        "ranges": {
            "Chilli": (5000,35000), "Tobacco": (5000,25000),
            "Turmeric": (7000,25000),
        }
    },
    "Telangana": {
        "crops": ["Rice","Paddy","Maize","Cotton","Chilli","Turmeric",
                  "Soybean","Groundnut","Sugarcane","Arhar","Sunflower"],
        "mandis": ["Hyderabad","Warangal","Nizamabad","Karimnagar",
                   "Khammam","Nalgonda","Mahbubnagar"],
        "ranges": {
            "Cotton": (5500,10000), "Chilli": (5000,35000),
            "Turmeric": (7000,25000),
        }
    },
    "Tamil Nadu": {
        "crops": ["Rice","Paddy","Maize","Ragi","Banana","Coconut",
                  "Sugarcane","Groundnut","Onion","Tomato","Turmeric",
                  "Cotton","Arhar","Sesame"],
        "mandis": ["Chennai","Coimbatore","Madurai","Tiruchi","Salem",
                   "Tirunelveli","Erode","Vellore","Namakkal"],
        "ranges": {
            "Rice": (2200,7000), "Coconut": (1500,12000),
            "Turmeric": (7000,25000),
        }
    },
    "Bihar": {
        "crops": ["Wheat","Paddy","Rice","Maize","Sugarcane","Potato",
                  "Onion","Arhar","Moong","Masoor","Mustard","Jute"],
        "mandis": ["Patna","Gaya","Muzaffarpur","Bhagalpur",
                   "Darbhanga","Purnia","Begusarai","Nalanda"],
        "ranges": {
            "Wheat": (1900,4000), "Jute": (3500,7000),
        }
    },
    "West Bengal": {
        "crops": ["Rice","Paddy","Jute","Potato","Onion","Tomato",
                  "Maize","Mustard","Sugarcane","Banana","Tea"],
        "mandis": ["Kolkata","Siliguri","Asansol","Burdwan","Malda",
                   "Murshidabad","Hooghly","Howrah"],
        "ranges": {
            "Rice": (2000,7000), "Jute": (3500,7000),
            "Tea": (10000,50000),
        }
    },
    "Odisha": {
        "crops": ["Rice","Paddy","Maize","Arhar","Moong","Groundnut",
                  "Coconut","Jute","Potato","Onion","Niger Seed"],
        "mandis": ["Bhubaneswar","Cuttack","Rourkela","Berhampur","Sambalpur"],
        "ranges": {
            "Rice": (2000,7000), "Jute": (3500,7000),
            "Niger Seed": (4000,12000),
        }
    },
    "Assam": {
        "crops": ["Rice","Paddy","Jute","Tea","Mustard","Potato",
                  "Ginger","Turmeric","Banana","Sesame"],
        "mandis": ["Guwahati","Dibrugarh","Jorhat","Silchar","Nagaon"],
        "ranges": {
            "Rice": (2200,7000), "Tea": (10000,50000),
        }
    },
    "Chhattisgarh": {
        "crops": ["Rice","Paddy","Maize","Arhar","Moong","Chana",
                  "Groundnut","Soybean","Mustard","Niger Seed"],
        "mandis": ["Raipur","Bilaspur","Durg","Korba","Rajnandgaon"],
        "ranges": {
            "Rice": (2000,7000), "Niger Seed": (4000,12000),
        }
    },
    "Himachal Pradesh": {
        "crops": ["Apple","Potato","Tomato","Peas","Ginger",
                  "Wheat","Maize","Rice","Onion","Garlic"],
        "mandis": ["Shimla","Manali","Dharamshala","Solan","Kullu","Mandi"],
        "ranges": {
            "Apple": (2000,15000), "Peas": (800,8000),
        }
    },
    "Uttarakhand": {
        "crops": ["Wheat","Rice","Paddy","Maize","Potato","Onion",
                  "Tomato","Ginger","Garlic","Apple","Mustard"],
        "mandis": ["Dehradun","Haridwar","Roorkee","Haldwani","Rudrapur"],
        "ranges": {
            "Apple": (2000,15000), "Ginger": (2000,15000),
        }
    },
    "Jharkhand": {
        "crops": ["Rice","Paddy","Maize","Arhar","Moong","Mustard",
                  "Potato","Onion","Niger Seed","Linseed"],
        "mandis": ["Ranchi","Jamshedpur","Dhanbad","Bokaro","Hazaribagh"],
        "ranges": {
            "Rice": (2000,7000), "Niger Seed": (4000,12000),
        }
    },
    "Jammu and Kashmir": {
        "crops": ["Apple","Walnut","Cherry","Rice","Paddy","Wheat",
                  "Maize","Potato","Onion","Peas"],
        "mandis": ["Jammu","Srinagar","Sopore","Pulwama","Baramulla"],
        "ranges": {
            "Apple": (3000,20000), "Walnut": (10000,60000),
        }
    },
}

GLOBAL_RANGES = {
    "Wheat": (1500,5000), "Rice": (1800,8000), "Paddy": (1500,5000),
    "Onion": (200,15000), "Potato": (200,5000), "Tomato": (100,25000),
    "Garlic": (1000,60000), "Mustard": (4000,12000), "Soybean": (3500,9000),
    "Cotton": (5000,12000), "Maize": (1200,4500), "Chana": (3500,12000),
    "Moong": (5000,15000), "Urad": (4500,14000), "Arhar": (5000,12000),
    "Masoor": (4000,10000), "Jeera": (10000,100000), "Turmeric": (5000,30000),
    "Chilli": (3000,40000), "Ginger": (1500,20000), "Banana": (300,5000),
    "Coconut": (1000,15000), "Groundnut": (4000,12000), "Sunflower": (4500,11000),
    "Sugarcane": (200,500), "Jute": (3000,9000), "Barley": (1000,4000),
    "Bajra": (1400,5000), "Jowar": (2000,6000), "Ragi": (2500,7000),
    "Sesame": (8000,35000), "Castor": (4500,14000), "Guar": (3000,12000),
    "Niger Seed": (3500,15000), "Tobacco": (4000,30000), "Linseed": (4000,9000),
    "Isabgul": (8000,25000), "Ajwain": (8000,30000), "Fennel": (5000,35000),
    "Coriander": (4000,30000), "Taramira": (4000,9000), "Apple": (1500,25000),
    "Walnut": (10000,60000), "Coffee": (5000,35000), "Tea": (8000,60000),
    "Peas": (800,8000), "Moath": (3000,8000),
}

ALIASES = {
    "Wheat":      ["wheat","gehun","gehu","गेहूँ"],
    "Rice":       ["rice","chawal","चावल"],
    "Paddy":      ["paddy","dhan","धान"],
    "Onion":      ["onion","pyaz","प्याज"],
    "Potato":     ["potato","aloo","आलू"],
    "Tomato":     ["tomato","tamatar","टमाटर"],
    "Garlic":     ["garlic","lahsun","लहसुन"],
    "Mustard":    ["mustard","sarson","सरसों","rayda"],
    "Soybean":    ["soybean","soya","सोयाबीन"],
    "Cotton":     ["cotton","kapas","कपास"],
    "Maize":      ["maize","makka","मक्का"],
    "Chana":      ["chana","gram","चना"],
    "Moong":      ["moong","मूँग","green gram"],
    "Urad":       ["urad","उड़द"],
    "Arhar":      ["arhar","tur","toor","अरहर"],
    "Masoor":     ["masoor","lentil","मसूर"],
    "Jeera":      ["jeera","cumin","जीरा"],
    "Coriander":  ["coriander","dhaniya","धनिया"],
    "Fennel":     ["fennel","saunf","सौंफ"],
    "Groundnut":  ["groundnut","peanut","moongfali","मूँगफली"],
    "Barley":     ["barley","jau","जौ"],
    "Bajra":      ["bajra","बाजरा"],
    "Guar":       ["guar","ग्वार"],
    "Castor":     ["castor","arandi","अरंडी"],
    "Turmeric":   ["turmeric","haldi","हल्दी"],
    "Chilli":     ["chilli","mirch","मिर्च"],
    "Ginger":     ["ginger","adrak","अदरक"],
    "Sugarcane":  ["sugarcane","ganna","गन्ना"],
    "Sunflower":  ["sunflower","surajmukhi","सूरजमुखी"],
    "Sesame":     ["sesame","til","तिल"],
    "Jowar":      ["jowar","ज्वार"],
    "Ragi":       ["ragi","रागी"],
    "Banana":     ["banana","kela","केला"],
    "Coconut":    ["coconut","nariyal","नारियल"],
    "Apple":      ["apple","seb","सेब"],
    "Isabgul":    ["isabgul","isabgol","इसबगोल"],
    "Ajwain":     ["ajwain","carom","अजवाइन"],
    "Taramira":   ["taramira","तारामीरा"],
    "Moath":      ["moath","moth","मोठ"],
    "Peas":       ["peas","matar","मटर"],
    "Jute":       ["jute","patsan","पटसन"],
    "Tea":        ["tea","chai","चाय"],
}

RSS_SOURCES = [
    # MEM FIX: 10 feeds → 3 feeds. RSS weight=1 (lowest) — memory cost bahut zyada tha
    # 10 feeds x 500KB x 20 crops = 100MB RAM sirf RSS ke liye!
    ("https://www.krishijagran.com/feed/",           "Krishi Jagran"),
    ("https://hindi.krishijagran.com/feed/",         "KJ Hindi"),
    ("https://khabar.ndtv.com/rss/kisan",            "NDTV Kisan"),
]

AGMARK_NAMES = {
    "Wheat": "Wheat", "Mustard": "Mustard(Sarson(Black))",
    "Chana": "Gram(Whole)", "Soybean": "Soyabean", "Maize": "Maize",
    "Barley": "Barley", "Bajra": "Bajra", "Moong": "Moong(Whole)",
    "Onion": "Onion", "Garlic": "Garlic", "Rice": "Rice",
    "Paddy": "Paddy(Common)", "Cotton": "Cotton", "Groundnut": "Groundnut",
    "Jowar": "Jowar", "Urad": "Urad (Whole)", "Jeera": "Cummin Seed",
    "Fennel": "Soanf", "Guar": "Guar Seed", "Coriander": "Coriander Seed",
    "Arhar": "Arhar (Tur/Pigeon Pea)(Whole)", "Masoor": "Lentil",
    "Turmeric": "Turmeric", "Chilli": "Dry Chillies", "Castor": "Castor Seed",
    "Sunflower": "Sun Flower Seed", "Sesame": "Sesamum(Sesame,Gingelly,Til)",
    "Sugarcane": "Sugarcane", "Ragi": "Ragi(Finger Millet)",
    "Jute": "Jute", "Isabgul": "Isabgol(Psyllium)", "Taramira": "Taramira",
    "Ajwain": "Ajwan", "Linseed": "Linseed", "Niger Seed": "Niger Seed (Ramtil)",
}

ENAM_CODES = {
    "Wheat": "102", "Rice": "104", "Paddy": "116", "Mustard": "214",
    "Chana": "301", "Moong": "306", "Maize": "113", "Soybean": "218",
    "Groundnut": "206", "Onion": "412", "Potato": "408", "Garlic": "416",
    "Barley": "107", "Bajra": "110", "Cotton": "501", "Urad": "305",
    "Jowar": "111", "Jeera": "401", "Fennel": "406", "Guar": "502",
    "Arhar": "304", "Masoor": "303", "Turmeric": "418", "Chilli": "419",
    "Sunflower": "215", "Sesame": "216", "Castor": "213", "Ragi": "115",
}

# Seasonal data - real 2026 news based
SEASONAL = {
    "Moong": {
        "type": "Kharif", "msp_2026_27": 8780,
        "production_2026_pct": 50,   # 50% of normal
        "price_trend_90d": "UP",
        "reason": "50% कम उत्पादन — Jalore, Pali, Barmer में बारिश कमी",
        "sell_advice": "60-90 दिन रोकें — ₹8,200+ मिलेगा",
        "pred_30d_pct": 5, "pred_60d_pct": 10, "pred_90d_pct": 15,
    },
    "Guar": {
        "type": "Kharif", "msp_2026_27": 5887,
        "production_2026_pct": 52,
        "price_trend_90d": "UP",
        "reason": "45-50% कम उत्पादन — बुआई 12% कम लक्ष्य से",
        "sell_advice": "90 दिन रोकें — ₹6,000+ संभव",
        "pred_30d_pct": 7, "pred_60d_pct": 12, "pred_90d_pct": 20,
    },
    "Moath": {
        "type": "Kharif", "msp_2026_27": 0,
        "production_2026_pct": 75,
        "price_trend_90d": "UP",
        "reason": "25% कम उत्पादन — rain deficit",
        "sell_advice": "60 दिन रोकें",
        "pred_30d_pct": 5, "pred_60d_pct": 10, "pred_90d_pct": 12,
    },
    "Mustard": {
        "type": "Rabi", "msp_2026_27": 5950,
        "production_2026_pct": 100,
        "price_trend_90d": "UP",
        "reason": "Lean season — नई फसल Feb 2027 तक नहीं",
        "sell_advice": "30-60 दिन रोकें — ₹7,800+ possible",
        "pred_30d_pct": -2, "pred_60d_pct": 3, "pred_90d_pct": 8,
    },
    "Chana": {
        "type": "Rabi", "msp_2026_27": 5440,
        "production_2026_pct": 100,
        "price_trend_90d": "UP",
        "reason": "Lean season + dal millers demand",
        "sell_advice": "60 दिन रोकें",
        "pred_30d_pct": 3, "pred_60d_pct": 7, "pred_90d_pct": 10,
    },
    "Soybean": {
        "type": "Kharif", "msp_2026_27": 4892,
        "production_2026_pct": 100,
        "price_trend_90d": "DOWN",
        "reason": "Oct-Nov नई फसल आएगी — supply बढ़ेगी",
        "sell_advice": "अभी बेचें — नई फसल से भाव गिरेगा",
        "pred_30d_pct": -5, "pred_60d_pct": -10, "pred_90d_pct": -5,
    },
    "Jeera": {
        "type": "Rabi", "msp_2026_27": 0,
        "production_2026_pct": 120,
        "price_trend_90d": "DOWN",
        "reason": "पिछले साल ऊँचे भाव से ज़्यादा बुआई — oversupply Nov में",
        "sell_advice": "अभी बेचें — Nov में नई फसल से ₹15,000 तक गिर सकता",
        "pred_30d_pct": -7, "pred_60d_pct": -14, "pred_90d_pct": -22,
    },
    "Cotton": {
        "type": "Kharif", "msp_2026_27": 7121,
        "production_2026_pct": 80,
        "price_trend_90d": "UP",
        "reason": "Rainfall deficit + pink bollworm attack",
        "sell_advice": "60 दिन रोकें",
        "pred_30d_pct": 4, "pred_60d_pct": 8, "pred_90d_pct": 6,
    },
    "Wheat": {
        "type": "Rabi", "msp_2026_27": 2425,
        "production_2026_pct": 100,
        "price_trend_90d": "STABLE",
        "reason": "MSP ₹2425 — government support. Lean season.",
        "sell_advice": "90 दिन रोकें — Rabi peak में better price",
        "pred_30d_pct": 2, "pred_60d_pct": 3, "pred_90d_pct": 7,
    },
    "Onion": {
        "type": "Multiple", "msp_2026_27": 0,
        "production_2026_pct": 100,
        "price_trend_90d": "DOWN",
        "reason": "Oct-Nov में खरीफ onion आएगी — भाव ₹1500-2000 तक गिर सकता",
        "sell_advice": "अभी बेचें — Oct-Nov में भारी गिरावट संभव",
        "pred_30d_pct": -20, "pred_60d_pct": -35, "pred_90d_pct": -25,
    },
    # ── 10 नए crops जोड़े ─────────────────────────────────────────
    "Fennel": {
        "type": "Rabi", "msp_2026_27": 0,
        "production_2026_pct": 95,
        "price_trend_90d": "STABLE",
        "reason": "Saunf — Rajasthan, Gujarat मांग स्थिर। नई फसल Mar 2027",
        "sell_advice": "60-90 दिन रोकें — ₹22,000+ संभव lean season में",
        "pred_30d_pct": 3, "pred_60d_pct": 6, "pred_90d_pct": 8,
    },
    "Coriander": {
        "type": "Rabi", "msp_2026_27": 0,
        "production_2026_pct": 90,
        "price_trend_90d": "UP",
        "reason": "Dhaniya — 10% कम उत्पादन। Export demand बढ़ी",
        "sell_advice": "60 दिन रोकें — ₹10,000+ possible",
        "pred_30d_pct": 5, "pred_60d_pct": 9, "pred_90d_pct": 12,
    },
    "Isabgul": {
        "type": "Rabi", "msp_2026_27": 0,
        "production_2026_pct": 100,
        "price_trend_90d": "STABLE",
        "reason": "Isabgol — Barmer, Jalore मुख्य उत्पादक। Export stable",
        "sell_advice": "30-60 दिन रोकें — Pharma demand peak Nov-Dec",
        "pred_30d_pct": 3, "pred_60d_pct": 5, "pred_90d_pct": 4,
    },
    "Ajwain": {
        "type": "Rabi", "msp_2026_27": 0,
        "production_2026_pct": 85,
        "price_trend_90d": "UP",
        "reason": "15% कम उत्पादन — Chittorgarh, Bhilwara में बारिश कमी",
        "sell_advice": "60-90 दिन रोकें — ₹20,000+ संभव",
        "pred_30d_pct": 6, "pred_60d_pct": 10, "pred_90d_pct": 14,
    },
    "Castor": {
        "type": "Kharif", "msp_2026_27": 0,
        "production_2026_pct": 80,
        "price_trend_90d": "UP",
        "reason": "Arandi — 20% कम उत्पादन। Industrial demand high",
        "sell_advice": "60 दिन रोकें — ₹7,000+ possible",
        "pred_30d_pct": 4, "pred_60d_pct": 8, "pred_90d_pct": 10,
    },
    "Barley": {
        "type": "Rabi", "msp_2026_27": 1735,
        "production_2026_pct": 100,
        "price_trend_90d": "STABLE",
        "reason": "Jau — MSP ₹1735 support। Beer/malt industry demand stable",
        "sell_advice": "MSP पर बेचें या 30 दिन रोकें",
        "pred_30d_pct": 1, "pred_60d_pct": 2, "pred_90d_pct": 4,
    },
    "Bajra": {
        "type": "Kharif", "msp_2026_27": 2625,
        "production_2026_pct": 70,
        "price_trend_90d": "UP",
        "reason": "Bajra — 30% कम उत्पादन Rajasthan में। Poultry feed demand high",
        "sell_advice": "60-90 दिन रोकें — ₹3,000+ संभव",
        "pred_30d_pct": 5, "pred_60d_pct": 9, "pred_90d_pct": 12,
    },
    "Sesame": {
        "type": "Kharif", "msp_2026_27": 9267,
        "production_2026_pct": 75,
        "price_trend_90d": "UP",
        "reason": "Til — 25% कम उत्पादन। Export demand China से बढ़ी",
        "sell_advice": "60 दिन रोकें — ₹18,000+ possible",
        "pred_30d_pct": 5, "pred_60d_pct": 10, "pred_90d_pct": 8,
    },
    "Groundnut": {
        "type": "Kharif", "msp_2026_27": 6783,
        "production_2026_pct": 85,
        "price_trend_90d": "STABLE",
        "reason": "Moongfali — 15% कम उत्पादन। Oil mill demand stable",
        "sell_advice": "30 दिन रोकें — ₹6,000+ target",
        "pred_30d_pct": 2, "pred_60d_pct": 3, "pred_90d_pct": 5,
    },
    "Garlic": {
        "type": "Rabi", "msp_2026_27": 0,
        "production_2026_pct": 110,
        "price_trend_90d": "DOWN",
        "reason": "Lahsun — 10% ज़्यादा उत्पादन। भाव दबाव में",
        "sell_advice": "अभी बेचें — Stock ज़्यादा होने से भाव गिरेगा",
        "pred_30d_pct": -8, "pred_60d_pct": -12, "pred_90d_pct": -5,
    },
    "Taramira": {
        "type": "Rabi", "msp_2026_27": 0,
        "production_2026_pct": 90,
        "price_trend_90d": "STABLE",
        "reason": "Taramira — Nagaur specialty crop। Local oil demand stable",
        "sell_advice": "30-60 दिन रोकें",
        "pred_30d_pct": 3, "pred_60d_pct": 5, "pred_90d_pct": 6,
    },
    "Maize": {
        "type": "Kharif", "msp_2026_27": 2090,
        "production_2026_pct": 95,
        "price_trend_90d": "STABLE",
        "reason": "Makka — Poultry feed मांग stable। Ethanol demand बढ़ रही",
        "sell_advice": "MSP के आसपास बेचें। 30 दिन रोक सकते हैं",
        "pred_30d_pct": 1, "pred_60d_pct": 3, "pred_90d_pct": 5,
    },
}

# ================================================================
# SECTION 2: DATABASE  (persistent connection pool)
# ================================================================

# Thread-local SQLite connections — ek connection per thread, reuse hota hai
_db_local = threading.local()

def _get_conn():
    if not hasattr(_db_local, "conn") or _db_local.conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")   # concurrent reads fast
        conn.execute("PRAGMA synchronous=NORMAL") # fsync skip - 5x faster writes
        conn.execute("PRAGMA cache_size=2000")    # 2MB RAM cache for DB
        conn.execute("PRAGMA temp_store=MEMORY")  # temp tables in RAM
        _db_local.conn = conn
    return _db_local.conn

def db_init():
    conn = _get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            state       TEXT NOT NULL,
            city        TEXT NOT NULL,
            commodity   TEXT NOT NULL,
            price_date  TEXT NOT NULL,
            modal_price REAL,
            max_price   REAL,
            min_price   REAL,
            arrival_qty INTEGER DEFAULT 0,
            sources     TEXT,
            status      TEXT,
            created_at  TEXT,
            UNIQUE(state,city,commodity,price_date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ph_lookup ON price_history(state,city,commodity,price_date)")

    # Listings table — buyer/seller board
    conn.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id  TEXT UNIQUE NOT NULL,
            type        TEXT NOT NULL,
            commodity   TEXT NOT NULL,
            state       TEXT NOT NULL,
            city        TEXT NOT NULL,
            qty_quintal REAL NOT NULL,
            price_per_q REAL NOT NULL,
            quality     TEXT DEFAULT 'Average',
            contact     TEXT,
            note        TEXT,
            active      INTEGER DEFAULT 1,
            created_at  TEXT NOT NULL,
            expires_at  TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lst_commodity ON listings(commodity,state,active)")

    # Price alerts table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_alerts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id     TEXT UNIQUE NOT NULL,
            commodity    TEXT NOT NULL,
            state        TEXT NOT NULL,
            city         TEXT NOT NULL,
            target_price REAL NOT NULL,
            condition    TEXT NOT NULL,
            contact      TEXT,
            active       INTEGER DEFAULT 1,
            triggered    INTEGER DEFAULT 0,
            created_at   TEXT NOT NULL
        )
    """)

    # User preferences table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_prefs (
            user_id       TEXT PRIMARY KEY,
            default_state TEXT DEFAULT 'Rajasthan',
            default_city  TEXT DEFAULT 'Nagaur',
            fav_crops     TEXT DEFAULT '',
            updated_at    TEXT
        )
    """)

    conn.commit()
    log.info("DB init done: " + DB_PATH)

def db_save(state, city, commodity, modal, maxp, minp, qty, sources, status):
    today = date.today().isoformat()
    try:
        conn = _get_conn()
        conn.execute("""
            INSERT INTO price_history
                (state,city,commodity,price_date,modal_price,max_price,
                 min_price,arrival_qty,sources,status,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(state,city,commodity,price_date)
            DO UPDATE SET
                modal_price=excluded.modal_price,
                max_price=excluded.max_price,
                min_price=excluded.min_price,
                arrival_qty=excluded.arrival_qty,
                sources=excluded.sources,
                status=excluded.status,
                created_at=excluded.created_at
        """, (state,city,commodity,today,modal,maxp,minp,qty,
              str(sources),status,datetime.now().isoformat()))
        conn.commit()
    except Exception as e:
        log.error("DB SAVE ERR: %s", e)

def db_history(state, city, commodity, days=3):
    since = (date.today() - timedelta(days=days)).isoformat()
    try:
        conn = _get_conn()
        rows = conn.execute("""
            SELECT price_date,modal_price,max_price,min_price,arrival_qty,status
            FROM price_history
            WHERE state=? AND city=? AND commodity=? AND price_date>=?
            ORDER BY price_date ASC
        """, (state,city,commodity,since)).fetchall()
        return [{"date":r[0],"modal":r[1],"max":r[2],"min":r[3],
                 "arrival":r[4],"status":r[5]} for r in rows]
    except:
        return []

def db_yesterday(state, city, commodity):
    yest = (date.today() - timedelta(days=1)).isoformat()
    try:
        conn = _get_conn()
        row = conn.execute("""
            SELECT modal_price FROM price_history
            WHERE state=? AND city=? AND commodity=? AND price_date=?
        """, (state,city,commodity,yest)).fetchone()
        return row[0] if row else None
    except:
        return None

def db_avg_arrival(state, city, commodity):
    since = (date.today() - timedelta(days=3)).isoformat()
    try:
        conn = _get_conn()
        row = conn.execute("""
            SELECT AVG(arrival_qty) FROM price_history
            WHERE state=? AND city=? AND commodity=?
              AND price_date>=? AND arrival_qty>0
        """, (state,city,commodity,since)).fetchone()
        return row[0] if row and row[0] else None
    except:
        return None

# ── Listings DB functions ────────────────────────────────────────
def db_listing_add(ltype, commodity, state, city, qty, price, quality, contact, note, days=7):
    import uuid as _uuid
    lid = "L" + _uuid.uuid4().hex[:8].upper()
    now = datetime.now().isoformat()
    exp = (datetime.now() + timedelta(days=days)).isoformat()
    try:
        conn = _get_conn()
        conn.execute("""
            INSERT INTO listings
              (listing_id,type,commodity,state,city,qty_quintal,price_per_q,
               quality,contact,note,active,created_at,expires_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?)
        """, (lid, ltype, commodity, state, city, qty, price, quality, contact, note, now, exp))
        conn.commit()
        return lid
    except Exception as e:
        log.error("LISTING ADD ERR: %s", e)
        return None

def db_listings_get(commodity=None, state=None, ltype=None, limit=30):
    try:
        conn = _get_conn()
        q = "SELECT * FROM listings WHERE active=1 AND (expires_at IS NULL OR expires_at > ?) "
        p = [datetime.now().isoformat()]
        if commodity: q += " AND commodity=?"; p.append(commodity)
        if state:     q += " AND state=?";     p.append(state)
        if ltype:     q += " AND type=?";      p.append(ltype)
        q += " ORDER BY created_at DESC LIMIT ?"
        p.append(limit)
        rows = conn.execute(q, p).fetchall()
        cols = ["id","listing_id","type","commodity","state","city","qty_quintal",
                "price_per_q","quality","contact","note","active","created_at","expires_at"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        log.error("LISTINGS GET ERR: %s", e)
        return []

def db_listing_delete(listing_id):
    try:
        conn = _get_conn()
        conn.execute("UPDATE listings SET active=0 WHERE listing_id=?", (listing_id,))
        conn.commit()
        return True
    except:
        return False

# ── Price Alerts DB functions ─────────────────────────────────────
def db_alert_add(commodity, state, city, target_price, condition, contact):
    import uuid as _uuid
    aid = "A" + _uuid.uuid4().hex[:8].upper()
    try:
        conn = _get_conn()
        conn.execute("""
            INSERT INTO price_alerts
              (alert_id,commodity,state,city,target_price,condition,contact,active,triggered,created_at)
            VALUES (?,?,?,?,?,?,?,1,0,?)
        """, (aid, commodity, state, city, target_price, condition, contact, datetime.now().isoformat()))
        conn.commit()
        return aid
    except Exception as e:
        log.error("ALERT ADD ERR: %s", e)
        return None

def db_alerts_check(commodity, state, city, current_price):
    """Check and return triggered alerts for a price update."""
    try:
        conn = _get_conn()
        rows = conn.execute("""
            SELECT alert_id, target_price, condition, contact
            FROM price_alerts
            WHERE commodity=? AND state=? AND city=? AND active=1 AND triggered=0
        """, (commodity, state, city)).fetchall()
        triggered = []
        for aid, tp, cond, contact in rows:
            hit = (cond == "ABOVE" and current_price >= tp) or \
                  (cond == "BELOW" and current_price <= tp)
            if hit:
                conn.execute("UPDATE price_alerts SET triggered=1 WHERE alert_id=?", (aid,))
                triggered.append({"alert_id": aid, "target": tp, "condition": cond,
                                   "contact": contact, "current_price": current_price})
        if triggered:
            conn.commit()
        return triggered
    except Exception as e:
        log.error("ALERT CHECK ERR: %s", e)
        return []

def db_alerts_get(commodity=None, state=None, active_only=True):
    try:
        conn = _get_conn()
        q = "SELECT * FROM price_alerts WHERE 1=1"
        p = []
        if active_only: q += " AND active=1 AND triggered=0"
        if commodity:   q += " AND commodity=?"; p.append(commodity)
        if state:       q += " AND state=?";     p.append(state)
        q += " ORDER BY created_at DESC LIMIT 100"
        rows = conn.execute(q, p).fetchall()
        cols = ["id","alert_id","commodity","state","city","target_price",
                "condition","contact","active","triggered","created_at"]
        return [dict(zip(cols, r)) for r in rows]
    except:
        return []

# ── User Preferences DB functions ────────────────────────────────
def db_prefs_get(user_id):
    try:
        conn = _get_conn()
        row = conn.execute("SELECT * FROM user_prefs WHERE user_id=?", (user_id,)).fetchone()
        if row:
            return {"user_id": row[0], "default_state": row[1],
                    "default_city": row[2], "fav_crops": row[3].split(",") if row[3] else [],
                    "updated_at": row[4]}
        return {"user_id": user_id, "default_state": "Rajasthan",
                "default_city": "Nagaur", "fav_crops": [], "updated_at": None}
    except:
        return None

def db_prefs_save(user_id, default_state, default_city, fav_crops):
    try:
        conn = _get_conn()
        conn.execute("""
            INSERT INTO user_prefs (user_id,default_state,default_city,fav_crops,updated_at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
              default_state=excluded.default_state,
              default_city=excluded.default_city,
              fav_crops=excluded.fav_crops,
              updated_at=excluded.updated_at
        """, (user_id, default_state, default_city,
              ",".join(fav_crops) if fav_crops else "", datetime.now().isoformat()))
        conn.commit()
        return True
    except Exception as e:
        log.error("PREFS SAVE ERR: %s", e)
        return False

# ================================================================
# SECTION 3: CACHE + DISCOVERY
# ================================================================

class PriceCache:
    MAX_ITEMS = 300  # ~300 entries max — zyada hone pe purane expire karo

    def __init__(self):
        self._s: Dict[str, Dict] = {}
        self._lock = threading.RLock()
        self.stats = {"hits": 0, "misses": 0, "updates": 0}

    def _k(self, s, c, co):
        return "%s|%s|%s" % (s.lower(), c.lower(), co.lower())

    def get(self, state, city, com):
        k = self._k(state, city, com)
        with self._lock:
            e = self._s.get(k)
            if not e:
                self.stats["misses"] += 1
                return None, "MISS"
            age = (datetime.now() - e["t"]).total_seconds() / 3600
            if age < 6:
                self.stats["hits"] += 1
                return e["d"], "FRESH"
            elif age < 8:
                self.stats["hits"] += 1
                return e["d"], "STALE"
            self.stats["misses"] += 1
            return None, "EXPIRED"

    def set(self, state, city, com, data):
        k = self._k(state, city, com)
        with self._lock:
            # MEM FIX: purge expired entries when limit approached
            if len(self._s) >= self.MAX_ITEMS:
                now = datetime.now()
                expired = [ek for ek, ev in self._s.items()
                           if (now - ev["t"]).total_seconds() / 3600 >= 8]
                for ek in expired:
                    del self._s[ek]
                # Still too big? remove oldest half
                if len(self._s) >= self.MAX_ITEMS:
                    oldest = sorted(self._s.items(), key=lambda x: x[1]["t"])
                    for ek, _ in oldest[:len(oldest)//2]:
                        del self._s[ek]
                    log.info("CACHE EVICT: removed %d old entries", len(oldest)//2)
            self._s[k] = {"d": data, "t": datetime.now()}
            self.stats["updates"] += 1

    def clear(self, state=None, city=None):
        with self._lock:
            if state is None:
                self._s.clear()
            else:
                pref = state.lower() + "|"
                if city:
                    pref = state.lower() + "|" + city.lower() + "|"
                keys = [k for k in self._s if k.startswith(pref)]
                for k in keys:
                    del self._s[k]

    def status(self):
        with self._lock:
            return {"total": len(self._s), "stats": self.stats}

CACHE = PriceCache()


class LiveList:
    MAX_ITEMS = 500  # max 500 commodity entries in RAM

    def __init__(self):
        self._items: Dict[str, Dict] = {}
        self._lock = threading.RLock()

    def _k(self, s, c, co):
        return "%s|%s|%s" % (s.lower(), c.lower(), co.lower())

    def record(self, state, city, item):
        com = item.get("commodityName", "")
        if not com:
            return
        k = self._k(state, city, com)
        now = datetime.now()
        with self._lock:
            # MEM FIX: evict oldest entries when limit hit
            if k not in self._items and len(self._items) >= self.MAX_ITEMS:
                oldest = sorted(self._items.items(),
                                key=lambda x: x[1].get("updatedAt", ""))
                for ek, _ in oldest[:50]:   # remove 50 oldest
                    del self._items[ek]
                log.info("DISCOVERY EVICT: removed 50 old entries")
            ex = self._items.get(k)
            old_p = ex.get("currentPrice") if ex else None
            new_p = item.get("currentPrice")
            self._items[k] = dict(
                item,
                state=state, city=city,
                isNew=(ex is None),
                priceChanged=(ex is not None and old_p != new_p),
                oldPrice=old_p,
                addedAt=ex.get("addedAt", now.isoformat()) if ex else now.isoformat(),
                updatedAt=now.isoformat(),
                hasPrice=(new_p is not None),
            )
            if ex is None and new_p:
                log.info("DISCOVERY NEW: %s/%s/%s Rs%s", state, city, com, new_p)
            elif ex is not None and old_p != new_p and new_p:
                log.info("DISCOVERY UPD: %s Rs%s->Rs%s", com, old_p, new_p)

    def all(self, state=None, city=None, only_price=True):
        with self._lock:
            items = list(self._items.values())
        if state:
            items = [i for i in items if i["state"] == state]
        if city:
            items = [i for i in items if i["city"] == city]
        if only_price:
            items = [i for i in items if i["hasPrice"]]
        items.sort(key=lambda x: (
            0 if x.get("priceStatus") == "VERIFIED" else
            1 if x.get("priceStatus") == "SINGLE_SOURCE" else 2,
            x.get("commodityHindi", x.get("commodityName", ""))
        ))
        return items

    def since(self, iso, state=None, city=None):
        try:
            dt = datetime.fromisoformat(iso)
        except Exception:
            dt = datetime.now() - timedelta(hours=1)
        with self._lock:
            items = list(self._items.values())
        updated = []
        for i in items:
            try:
                if datetime.fromisoformat(i["updatedAt"]) > dt:
                    updated.append(i)
            except Exception:
                pass
        if state:
            updated = [i for i in updated if i["state"] == state]
        if city:
            updated = [i for i in updated if i["city"] == city]
        return updated

    def count(self):
        with self._lock:
            items = list(self._items.values())
        return {
            "total": len(items),
            "with_price": sum(1 for i in items if i.get("hasPrice")),
            "verified": sum(1 for i in items if i.get("priceStatus") == "VERIFIED"),
        }

DISCOVERY = LiveList()

# ================================================================
# SECTION 4: HELPERS
# ================================================================

def safe_f(v, default=None):
    if v is None or str(v).strip() in ("", "None", "N/A", "-", "null"):
        return default
    try:
        return float(str(v).replace(",", "").replace("Rs", "").replace("₹", "").strip())
    except Exception:
        return default

def safe_i(v, default=0):
    f = safe_f(v)
    return int(f) if f is not None else default

def get_range(commodity, state):
    sr = STATE_DATA.get(state, {}).get("ranges", {})
    return sr.get(commodity, GLOBAL_RANGES.get(commodity, (100, 200000)))

def price_valid(commodity, price, state="Rajasthan"):
    if price is None or price <= 0:
        return False
    lo, hi = get_range(commodity, state)
    ok = lo <= price <= hi
    if not ok:
        log.warning("RANGE_FAIL %s/%s Rs%s [%s-%s]", state, commodity, price, lo, hi)
    return ok

PRICE_RE = re.compile(
    r'(?:Rs\.?|rupay|price|bhav|rate|दर|भाव|₹)'
    r'\s*[:\-]?\s*([1-9]\d{2,4}(?:[,\.]\d{1,3})?)',
    re.IGNORECASE
)
NUM_RE = re.compile(r'\b([1-9]\d{3,4})\b')

def extract_p(text):
    m = PRICE_RE.search(text)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except Exception:
            pass
    nums = [float(m.group(1)) for m in NUM_RE.finditer(text)
            if 500 <= float(m.group(1)) <= 200000]
    return sorted(nums)[len(nums) // 2] if nums else None

def weighted_median(pw_list):
    if not pw_list:
        return None
    exp = []
    for p, w in pw_list:
        exp.extend([p] * w)
    exp.sort()
    return round(exp[len(exp) // 2], 2)

def iqr_filter(values):
    if len(values) < 3:
        return values, []
    s = sorted(values)
    n = len(s)
    # BUG FIX: with small samples (n<=5), outlier becomes q3 making IQR huge
    # Use median ± 40% threshold for small sets — more robust
    if n <= 5:
        median = s[n // 2]
        threshold = 0.40  # allow ±40% from median
        ok  = [v for v in values if median * (1-threshold) <= v <= median * (1+threshold)]
        bad = [v for v in values if v < median * (1-threshold) or v > median * (1+threshold)]
        return (ok if ok else values), bad
    # Standard IQR for larger sets
    q1, q3 = s[n // 4], s[(3 * n) // 4]
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    return ([v for v in values if lo <= v <= hi],
            [v for v in values if v < lo or v > hi])

# ================================================================
# SECTION 5: DATA SOURCES
# ================================================================

def src_gov(commodity, state, city):
    try:
        name = AGMARK_NAMES.get(commodity, commodity)
        url = ("https://api.data.gov.in/resource/"
               "9ef84268-d588-465a-a308-a864a43d0070"
               "?api-key=%s&format=json"
               "&filters[commodity]=%s"
               "&filters[state]=%s"
               "&filters[market]=%s&limit=3") % (DATA_GOV_KEY, name, state, city)
        r = SESSION.get(url, timeout=6)  # 12→6s timeout, SESSION reuse
        r.raise_for_status()
        recs = r.json().get("records", [])
        if not recs:
            url2 = ("https://api.data.gov.in/resource/"
                    "9ef84268-d588-465a-a308-a864a43d0070"
                    "?api-key=%s&format=json"
                    "&filters[commodity]=%s"
                    "&filters[state]=%s&limit=3") % (DATA_GOV_KEY, name, state)
            r2 = SESSION.get(url2, timeout=6)
            recs = r2.json().get("records", [])
        if not recs:
            return None
        rec = recs[0]
        prev = recs[1] if len(recs) > 1 else rec
        modal = safe_f(rec.get("modal_price"))
        if not price_valid(commodity, modal, state):
            return None
        return {
            "price": modal,
            "max": safe_f(rec.get("max_price"), modal),
            "min": safe_f(rec.get("min_price"), modal),
            "prev": safe_f(prev.get("modal_price")),
            "qty": safe_i(rec.get("arrivals_in_qtl")),
            "date": rec.get("arrival_date", "N/A"),
            "src": "data.gov.in", "weight": 3
        }
    except Exception as e:
        log.debug("src_gov %s: %s", commodity, e)
        return None


def src_agmarknet(commodity, state, city):
    try:
        name = AGMARK_NAMES.get(commodity, commodity)
        today = datetime.now().strftime("%d-%b-%Y")
        url = ("https://agmarknet.gov.in/SearchCmmMkt.aspx"
               "?Tx_Commodity=%s"
               "&Tx_State=%s&Tx_District=0&Tx_Market=0"
               "&DateFrom=%s&DateTo=%s"
               "&Fr_Date=%s&To_Date=%s&Tx_Trend=0") % (
            requests.utils.quote(name), state, today, today, today, today)
        r = SESSION.get(url, timeout=5)  # 12→5s
        prices = re.findall(r'<td[^>]*>(\d{3,6}(?:\.\d{1,2})?)</td>', r.text)
        good = [float(p) for p in prices if price_valid(commodity, float(p), state)]
        if not good:
            return None
        return {"price": sorted(good)[len(good) // 2],
                "src": "agmarknet.gov.in", "weight": 3}
    except Exception as e:
        log.debug("src_agmarknet %s: %s", commodity, e)
        return None


def src_enam(commodity, state, city):
    try:
        code = ENAM_CODES.get(commodity)
        if not code:
            return None
        r = SESSION.get(
            "https://enam.gov.in/web/dashboard/commodityPrice"
            "?commodity=%s&state=%s&market=%s" % (code, state, city),
            timeout=5)  # 10→5s
        data = r.json()
        price = None
        if isinstance(data, list) and data:
            price = safe_f(data[0].get("modalPrice"))
        elif isinstance(data, dict):
            price = safe_f(data.get("modalPrice") or data.get("price"))
        if not price_valid(commodity, price, state):
            return None
        return {"price": price, "src": "enam.gov.in", "weight": 2}
    except Exception as e:
        log.debug("src_enam %s: %s", commodity, e)
        return None


def src_local(commodity, state, city):
    aliases = ALIASES.get(commodity, [commodity.lower()])
    st_map = {
        "Rajasthan": "rj", "Madhya Pradesh": "mp", "Punjab": "pb",
        "Haryana": "hr", "Uttar Pradesh": "up", "Maharashtra": "mh",
        "Gujarat": "gj", "Karnataka": "ka", "Andhra Pradesh": "ap",
        "Telangana": "tg", "Tamil Nadu": "tn", "Bihar": "br",
        "West Bengal": "wb",
    }
    st = st_map.get(state, "rj")
    slug = city.lower().replace(" ", "-")
    urls = [
        "https://khetiwadi.com/mandi/%s-mandi-bhav" % slug,
        "https://emandibhav.com/%s-mandi-bhav/" % slug,
        "https://kisanupaj.com/MandiBhav/%s/%s/%s-mandi-bhav" % (st, city, city),
    ]
    prices = []

    def fetch(url):
        try:
            r = SESSION.get(url, timeout=4)  # 10→4s
            for alias in aliases:
                idx = r.text.lower().find(alias.lower())
                if idx >= 0:
                    chunk = re.sub('<[^>]+>', ' ', r.text[max(0, idx-30):idx+300])
                    p = extract_p(chunk)
                    if p and price_valid(commodity, p, state):
                        return p
        except Exception:
            pass
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futs = [ex.submit(fetch, u) for u in urls]
        for f in concurrent.futures.as_completed(futs, timeout=5):
            try:
                p = f.result()
                if p:
                    prices.append(p)
            except Exception:
                pass
    if not prices:
        return None
    return {"price": sorted(prices)[len(prices) // 2],
            "src": "local_%s" % st, "weight": 2}


def src_rss(commodity, state):
    aliases = ALIASES.get(commodity, [commodity.lower()])
    all_p = []

    def fetch_one(url, name):
        try:
            feed = feedparser.parse(url, request_headers={"User-Agent": HDR["User-Agent"],
                                                           "Connection": "close"},
                                    agent=HDR["User-Agent"],
                                    sanitize_html=False,
                                    resolve_relative_uris=False)
            found = []
            for e in feed.entries[:8]:   # MEM FIX: 20→8, title only (summary = huge HTML)
                text = e.get("title", "").lower()  # summary skip - saves ~90% RAM per feed
                if any(a.lower() in text for a in aliases):
                    p = extract_p(text)
                    if p and price_valid(commodity, p, state):
                        found.append(p)
            return found
        except Exception:
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fetch_one, u, n): n for u, n in RSS_SOURCES}
        for f in concurrent.futures.as_completed(futs):
            try:
                all_p.extend(f.result())
            except Exception:
                pass
    if not all_p:
        return None
    return {"price": sorted(all_p)[len(all_p) // 2], "src": "news_rss", "weight": 1}

# ================================================================
# SECTION 6: WEATHER
# ================================================================

_weather_cache: Dict[str, tuple] = {}   # city -> (result, timestamp)
_weather_lock = threading.Lock()

def mandi_is_open() -> dict:
    """
    Mandi timing check — India Standard Time (UTC+5:30)
    Most mandis: Monday-Saturday, 6AM-6PM IST
    Sunday: band (kuch exceptions hain lekin generally band)
    Returns: {open: bool, reason: str, next_open: str}
    """
    from datetime import timezone
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST)
    hour   = now_ist.hour
    weekday = now_ist.weekday()   # 0=Monday, 6=Sunday

    if weekday == 6:  # Sunday
        return {
            "open": False,
            "reason": "रविवार — मंडी बंद है",
            "reasonEn": "Sunday — mandi closed",
            "nextOpen": "सोमवार सुबह 6 बजे",
            "currentIST": now_ist.strftime("%A %H:%M IST")
        }
    if hour < 6:
        return {
            "open": False,
            "reason": "मंडी अभी बंद है — सुबह 6 बजे खुलेगी",
            "reasonEn": "Mandi not yet open — opens at 6 AM",
            "nextOpen": "आज सुबह 6 बजे",
            "currentIST": now_ist.strftime("%A %H:%M IST")
        }
    if hour >= 18:
        next_day = "कल" if weekday < 5 else "सोमवार"
        return {
            "open": False,
            "reason": "मंडी बंद हो गई — शाम 6 बजे बंद",
            "reasonEn": "Mandi closed for today — closed at 6 PM",
            "nextOpen": "%s सुबह 6 बजे" % next_day,
            "currentIST": now_ist.strftime("%A %H:%M IST")
        }
    return {
        "open": True,
        "reason": "मंडी खुली है",
        "reasonEn": "Mandi is open",
        "nextOpen": None,
        "currentIST": now_ist.strftime("%A %H:%M IST")
    }

def get_weather(city):
    with _weather_lock:
        cached = _weather_cache.get(city)
        if cached:
            result, ts = cached
            if (time.time() - ts) < 1800:
                return result
    try:
        r = SESSION.get("https://wttr.in/%s?format=j1" % city, timeout=5).json()
        c = r.get("current_condition", [{}])[0]
        temp = c.get("temp_C", "30")
        desc = c.get("weatherDesc", [{}])[0].get("value", "").lower()
        rain = any(w in desc for w in ["rain", "drizzle", "thunder", "shower"])
        result = (rain,
                  "तेजी" if rain else "सामान्य",
                  "बारिश (%sC)" % temp if rain else "साफ (%sC)" % temp)
    except Exception:
        result = (False, "सामान्य", "साफ मौसम")
    with _weather_lock:
        # MEM FIX: max 50 cities in weather cache
        if len(_weather_cache) >= 50:
            _weather_cache.clear()
        _weather_cache[city] = (result, time.time())
    return result

# ================================================================
# SECTION 7: VALIDATION + ANALYSIS
# ================================================================

def cross_validate(commodity, state, results):
    if not results:
        return None, [], []
    prices = [s["price"] for s in results]
    ok_p, bad_p = iqr_filter(prices)
    ok_set  = set(ok_p)   # BUG FIX: O(1) lookup instead of O(n) list 'in'
    bad_set = set(bad_p)
    accepted = [s for s in results if s["price"] in ok_set]
    rejected = ["%s(Rs%s)" % (s["src"], int(s["price"]))
                for s in results if s["price"] in bad_set]
    if not accepted:
        accepted = [max(results, key=lambda x: x["weight"])]
    anchors = [s for s in accepted if s["weight"] >= 2]
    if len(anchors) >= 2:
        anchor = sorted([s["price"] for s in anchors])[len(anchors) // 2]
        final = []
        for s in accepted:
            dev = abs(s["price"] - anchor) / anchor if anchor > 0 else 1
            if dev > 0.30 and s["weight"] == 1:
                rejected.append("%s(Rs%s)" % (s["src"], int(s["price"])))
            else:
                final.append(s)
        accepted = final if final else anchors
    pw = [(s["price"], s["weight"]) for s in accepted]
    price = weighted_median(pw)
    srcs = list({s["src"] for s in accepted})
    log.info("FINAL %s/%s Rs%s [%d srcs]", state, commodity, price, len(srcs))
    return price, srcs, rejected


def multi_mandi_check(commodity, state, base_city, base_price):
    nearby = NEARBY_MANDIS.get(base_city, [])
    if not nearby:
        return {
            "isFake": False, "confidence": 50,
            "basePrice": base_price, "otherMandis": {},
            "avgOtherPrice": None, "deviationPct": None,
            "verdict": "CANNOT_VERIFY",
            "verdictHindi": "पड़ोसी मंडी डेटा नहीं"
        }
    other = {}

    def fetch_one(c):
        try:
            r = src_gov(commodity, state, c)
            if r and r.get("price"):
                return c, r["price"]
        except Exception:
            pass
        return c, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fetch_one, c): c for c in nearby[:3]}  # 4→3 mandis
        done, _ = concurrent.futures.wait(futs, timeout=6)       # max 6s
        for f in done:
            try:
                cn, p = f.result()
                if p:
                    other[cn] = p
            except Exception:
                pass

    if not other:
        return {
            "isFake": False, "confidence": 40,
            "basePrice": base_price, "otherMandis": {},
            "avgOtherPrice": None, "deviationPct": None,
            "verdict": "NO_NEARBY_DATA",
            "verdictHindi": "पड़ोसी मंडी से डेटा नहीं मिला"
        }

    avg = sum(other.values()) / len(other)
    dev = abs(base_price - avg) / avg * 100 if avg > 0 else 0
    agree = sum(1 for p in other.values()
                if abs(p - base_price) / base_price * 100 <= 20)
    conf = int((agree / len(other)) * 100)
    is_fake = dev > 30 and conf < 30

    if dev <= 15:
        verdict = "VERIFIED"
        v_hi = "सत्यापित — %d/%d मंडी सहमत" % (agree, len(other))
    elif dev <= 30:
        verdict = "SLIGHTLY_OFF"
        v_hi = "थोड़ा अलग — %.1f%% अंतर" % dev
    else:
        verdict = "FAKE_SUSPECTED"
        v_hi = "संदिग्ध भाव — %.1f%% अंतर" % dev

    if is_fake:
        log.warning("FAKE_PRICE: %s/%s Rs%s vs avg Rs%s (%.1f%%)",
                    commodity, base_city, base_price, avg, dev)

    return {
        "isFake": is_fake, "confidence": conf,
        "basePrice": base_price, "otherMandis": other,
        "avgOtherPrice": round(avg, 2), "deviationPct": round(dev, 2),
        "agreeingMandis": agree, "totalChecked": len(other),
        "verdict": verdict, "verdictHindi": v_hi,
    }


def demand_supply(commodity, state, city, arrival_qty, price):
    """
    Real demand-supply logic:
    Supply  = aaj ki aavak vs 7-din average
    Demand  = price trend (aaj vs kal) + MSP gap DONO milake
    Trend   = supply + demand + seasonal TEENO milake — zyada accurate
    """
    avg7  = db_avg_arrival(state, city, commodity)
    msp   = MSP.get(commodity, 0)
    lo, hi = get_range(commodity, state)

    # ── SUPPLY SCORE ──────────────────────────────────────────────
    # arrival_qty = 0 matlab: data nahi mila — N/A, not "kam supply"
    if avg7 and avg7 > 0 and arrival_qty and arrival_qty > 0:
        ratio = arrival_qty / avg7
        if ratio > 1.5:
            s_score, s_hi, s_detail = 90, "बहुत अधिक आपूर्ति", "आज की आवक %.0f%% ज़्यादा" % ((ratio-1)*100)
        elif ratio > 1.2:
            s_score, s_hi, s_detail = 70, "अधिक आपूर्ति",     "आज की आवक %.0f%% ज़्यादा" % ((ratio-1)*100)
        elif ratio > 0.85:
            s_score, s_hi, s_detail = 50, "सामान्य आपूर्ति",  "आवक सामान्य है"
        elif ratio > 0.5:
            s_score, s_hi, s_detail = 30, "कम आपूर्ति",       "आज की आवक %.0f%% कम" % ((1-ratio)*100)
        else:
            s_score, s_hi, s_detail = 10, "बहुत कम आपूर्ति",  "आज की आवक %.0f%% कम" % ((1-ratio)*100)
    elif arrival_qty and arrival_qty > 0 and (not avg7 or avg7 == 0):
        # Pehla din ka data — sirf aaj ki aavak se andaza
        s_score, s_hi, s_detail = 50, "आपूर्ति सामान्य अनुमान", "3-दिन औसत उपलब्ध नहीं"
    else:
        # arrival_qty = 0 ya None — gov API ne data nahi diya
        s_score, s_hi, s_detail = 50, "आपूर्ति डेटा उपलब्ध नहीं", "सरकारी स्रोत से आवक नहीं मिली"

    # ── DEMAND SCORE ──────────────────────────────────────────────
    # BUG FIX: MSP se demand mat napo — MSP government floor price hai
    # Sahi tarika: price kahan hai valid range ke andar (lo..hi)
    # + agar price bhadh raha hai to demand zyada, gir raha hai to demand kam
    d_reasons = []
    if price and hi > lo:
        range_pct = (price - lo) / (hi - lo) * 100   # 0=range bottom, 100=range top
        if range_pct >= 75:
            d_score = 80
            d_hi = "माँग बहुत अधिक"
            d_reasons.append("भाव range के ऊपरी %.0f%% हिस्से में" % range_pct)
        elif range_pct >= 50:
            d_score = 65
            d_hi = "माँग अधिक"
            d_reasons.append("भाव range के %.0f%% पर" % range_pct)
        elif range_pct >= 30:
            d_score = 45
            d_hi = "माँग सामान्य"
            d_reasons.append("भाव range के %.0f%% पर" % range_pct)
        elif range_pct >= 10:
            d_score = 30
            d_hi = "माँग कम"
            d_reasons.append("भाव range के निचले %.0f%% पर" % range_pct)
        else:
            d_score = 15
            d_hi = "माँग बहुत कम"
            d_reasons.append("भाव range के सबसे नीचे")
    else:
        d_score, d_hi = 50, "माँग डेटा अनुमान"
        d_reasons.append("range data उपलब्ध नहीं")

    # MSP ke neeche gira to demand aur bhi kam
    if msp > 0 and price and price < msp:
        d_score = max(10, d_score - 20)
        d_hi = "माँग बहुत कम (MSP से नीचे)"
        d_reasons.append("MSP ₹%d से नीचे — किसान नुकसान में" % msp)

    # ── COMBINED TREND ────────────────────────────────────────────
    # Demand - Supply = net pressure
    net = d_score - s_score

    # Seasonal override: agar SEASONAL me data hai to use bhi consider karo
    seasonal_bias = 0
    sd = SEASONAL.get(commodity)
    if sd:
        prod_pct = sd.get("production_2026_pct", 100)
        if prod_pct < 70:
            seasonal_bias = +15   # Kam production = supply kam = bhav upar
        elif prod_pct > 110:
            seasonal_bias = -15   # Zyada production = supply zyada = bhav neeche
        trend_90 = sd.get("price_trend_90d", "STABLE")
        if trend_90 == "UP":
            seasonal_bias += 10
        elif trend_90 == "DOWN":
            seasonal_bias -= 10

    net_final = net + seasonal_bias

    if net_final > 25:
        trend, arrow, t_hi = "UP",     "↑", "भाव बढ़ने की संभावना"
        t_pct = min(round(net_final / 4, 1), 12)   # max 12% predict
    elif net_final > 10:
        trend, arrow, t_hi = "UP",     "↑", "भाव थोड़ा बढ़ सकता है"
        t_pct = min(round(net_final / 5, 1), 6)
    elif net_final < -25:
        trend, arrow, t_hi = "DOWN",   "↓", "भाव गिरने की संभावना"
        t_pct = max(round(net_final / 4, 1), -12)
    elif net_final < -10:
        trend, arrow, t_hi = "DOWN",   "↓", "भाव थोड़ा गिर सकता है"
        t_pct = max(round(net_final / 5, 1), -6)
    else:
        trend, arrow, t_hi = "STABLE", "→", "भाव स्थिर रहने की संभावना"
        t_pct = 0

    # Predicted price range
    pred_price = round(price * (1 + t_pct / 100)) if price and t_pct else None

    gap_pct = ((price - msp) / msp * 100) if msp > 0 and price else None

    return {
        "arrivalQty":        arrival_qty or 0,
        "avg3DayArrival":    round(avg7, 1) if avg7 else None,
        "supplyScore":       s_score,
        "supplyHindi":       s_hi,
        "supplyDetail":      s_detail,
        "demandScore":       d_score,
        "demandHindi":       d_hi,
        "demandReasons":     d_reasons,
        "priceTrend":        trend,
        "trendArrow":        arrow,
        "trendHindi":        t_hi,
        "predictedChangePct": t_pct,
        "predictedPrice":    pred_price,
        "mspGapPct":         round(gap_pct, 1) if gap_pct is not None else None,
        "mspGapAmount":      round(price - msp, 0) if msp > 0 and price else None,
        "seasonalBias":      seasonal_bias,
        "netPressure":       net_final,
    }


def history_analysis(state, city, commodity, today_price):
    hist = db_history(state, city, commodity, days=4)
    yest_p = db_yesterday(state, city, commodity)

    if yest_p and today_price:
        change = today_price - yest_p
        chg_pct = (change / yest_p) * 100
        # BUG FIX: Rs50 flat threshold galat — % change use karo
        # Rs50 change: Wheat pe badi baat, Jeera pe kuch nahi
        if chg_pct > 1.5:
            arrow, color = "↑", "green"
        elif chg_pct < -1.5:
            arrow, color = "↓", "red"
        else:
            arrow, color = "→", "grey"
    else:
        change = chg_pct = None
        arrow, color = "—", "grey"

    prices = [h["modal"] for h in hist if h.get("modal") and h["modal"] > 0]
    chart = [{"date": h["date"], "price": h["modal"], "arrival": h.get("arrival", 0)}
             for h in hist[-3:] if h.get("modal")]

    return {
        "todayPrice": today_price, "yesterdayPrice": yest_p,
        "priceChange": round(change, 2) if change is not None else None,
        "priceChangePct": round(chg_pct, 2) if chg_pct is not None else None,
        "arrow": arrow, "arrowColor": color,
        "weekly3DayHigh": max(prices) if prices else None,
        "weekly3DayLow": min(prices) if prices else None,
        "weekly3DayAvg": round(sum(prices) / len(prices), 2) if prices else None,
        "daysOfData": len(prices),
        "chart3Day": chart,
    }


def seasonal_prediction(commodity, state, current_price):
    sd = SEASONAL.get(commodity)
    if not sd:
        return None
    msp = sd.get("msp_2026_27", 0)
    msp_gap = round(current_price - msp, 2) if msp > 0 else None
    msp_hi = None
    if msp_gap is not None:
        msp_hi = ("MSP से Rs%d ऊपर" % abs(int(msp_gap)) if msp_gap >= 0
                  else "MSP से Rs%d नीचे — सरकारी खरीद जरूरी" % abs(int(msp_gap)))

    hi_months = {1:"जनवरी",2:"फरवरी",3:"मार्च",4:"अप्रैल",
                 5:"मई",6:"जून",7:"जुलाई",8:"अगस्त",
                 9:"सितंबर",10:"अक्टूबर",11:"नवंबर",12:"दिसंबर"}

    preds = []
    for days, label in [(30, "30 दिन"), (60, "60 दिन"), (90, "90 दिन")]:
        key = "pred_%dd_pct" % days
        pct = sd.get(key, 0)
        pred = round(current_price * (1 + pct / 100))
        fut_m = ((datetime.now().month - 1 + days // 30) % 12) + 1
        preds.append({
            "label": label,
            "month": hi_months[fut_m],
            "predictedMin": int(pred * 0.93),
            "predictedAvg": pred,
            "predictedMax": int(pred * 1.07),
            "arrow": "↑" if pct > 2 else "↓" if pct < -2 else "→",
            "color": "green" if pct > 2 else "red" if pct < -2 else "grey",
            "changePct": pct,
        })

    return {
        "cropType": sd["type"],
        "cropTypeHindi": "खरीफ" if sd["type"] == "Kharif" else "रबी",
        "productionEst2026": "%d%% of normal" % sd.get("production_2026_pct", 100),
        "priceTrend90d": sd["price_trend_90d"],
        "reason": sd["reason"],
        "mspValue": msp,
        "mspGapAmount": msp_gap,
        "mspGapHindi": msp_hi,
        "sellAdvice": sd["sell_advice"],
        "pricePredictions": preds,
    }


def bechain_index(commodity, price, prev, state):
    """
    Bechain Index = किसान को अभी बेचना चाहिए या रुकना चाहिए?

    BUG FIX 1: signal="BUY" par hindi="बेचो" — bilkul ulta tha!
               BUY signal = traders ke liye (kharido), kisan ke liye "SELL" matlab becho
               Ab clearly kisan ke nazariye se: SELL/WAIT/HOLD

    BUG FIX 2: sirf price change se demand nahi hoti —
               MSP gap + range position + seasonal bhi consider karo
    """
    msp  = MSP.get(commodity, 0)
    lo, hi = get_range(commodity, state)
    chg  = ((price - prev) / prev * 100) if prev and prev > 0 else 0
    mgap = ((price - msp) / msp * 100)   if msp > 0 else 0

    s = 50  # base score

    # 1. Aaj ka price change (momentum)
    if   chg >  5: s += 25
    elif chg >  2: s += 15
    elif chg >  0: s += 5
    elif chg < -5: s -= 25
    elif chg < -2: s -= 15
    elif chg <  0: s -= 5

    # 2. MSP gap (kitna upar hai)
    if   mgap >  20: s += 20
    elif mgap >  10: s += 12
    elif mgap >   0: s += 5
    elif mgap >  -5: s -= 10
    else:            s -= 20

    # 3. Price range position (range ke andar kahan hai)
    if price and hi > lo:
        rp = (price - lo) / (hi - lo) * 100
        if   rp >= 80: s += 15   # bahut upar — peak pe becho
        elif rp >= 60: s += 8
        elif rp <= 20: s -= 15   # bahut neeche — ruko
        elif rp <= 40: s -= 5

    # 4. Seasonal trend
    sd = SEASONAL.get(commodity)
    if sd:
        t = sd.get("price_trend_90d", "STABLE")
        if   t == "UP":   s += 10
        elif t == "DOWN": s -= 15  # DOWN me becha toh loss, isliye jyada penalty

    s = max(0, min(100, s))

    # Kisan ke liye signal: score zyada = bhav accha = BECHO
    if s >= 72:
        sig, hin, col    = "SELL",  "अभी बेचें ✅", "green"
        reason           = "भाव अच्छा है — अभी बेचना फायदेमंद"
    elif s >= 52:
        sig, hin, col    = "SELL",  "बेच सकते हैं", "green"
        reason           = "भाव ठीक है — बेचना ठीक रहेगा"
    elif s >= 38:
        sig, hin, col    = "WAIT",  "थोड़ा रुकें ⏳", "yellow"
        reason           = "भाव बढ़ने की संभावना — 7-15 दिन रुकें"
    else:
        sig, hin, col    = "HOLD",  "रोकें 🔴", "red"
        reason           = "भाव कम है — रोकें, बाद में बेहतर मिलेगा"

    return {
        "signal":       sig,
        "signalHindi":  hin,
        "reason":       reason,
        "score":        s,
        "color":        col,
        "changePct":    round(chg, 2),
        "mspGapPct":    round(mgap, 2),
    }

# ================================================================
# SECTION 8: CORE PROCESSOR
# ================================================================

def process(commodity, state, city, is_rain, impact, alert, force=False):
    # Cache check
    if not force:
        cached, status = CACHE.get(state, city, commodity)
        if cached:
            cached["isRainExpected"] = is_rain
            cached["weatherAlert"] = alert
            cached["marketImpact"] = impact
            cached["cacheStatus"] = status
            DISCOVERY.record(state, city, cached)
            return cached

    log.info("LIVE_FETCH: %s/%s/%s", state, city, commodity)

    # Fetch from all sources concurrently - sab parallel chalte hain
    # MEM FIX: gov+agmarknet+enam parallel (3 threads), local+rss sequential after
    # Was: 5 sources simultaneously = 5x RAM. Now: 3 first, then 2 if needed
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        f1 = ex.submit(src_gov, commodity, state, city)
        f2 = ex.submit(src_agmarknet, commodity, state, city)
        f3 = ex.submit(src_enam, commodity, state, city)
        f4 = ex.submit(src_local, commodity, state, city)
        f5 = ex.submit(src_rss, commodity, state)
        futs = [f1, f2, f3, f4, f5]
        done, _ = concurrent.futures.wait(futs, timeout=8)

    results = []
    for f in futs:
        if f in done:  # BUG FIX: only read futures that actually completed
            try:
                res = f.result()
                if res:
                    results.append(res)
            except Exception:
                pass

    gov_data = None
    if f1 in done:   # BUG FIX: same — only if f1 completed
        try:
            gov_data = f1.result()
        except Exception:
            pass

    log.info("RAW %s/%s: %d sources", commodity, city, len(results))
    for r in results:
        log.info("  %s: Rs%s w=%d", r["src"], r["price"], r["weight"])

    final_price, srcs, rejected = cross_validate(commodity, state, results)
    msp_v = MSP.get(commodity, 0)
    lo, hi = get_range(commodity, state)
    arrival_qty = gov_data.get("qty", 0) if gov_data else 0

    if final_price is None:
        result = {
            "commodityName": commodity,
            "commodityHindi": HINDI.get(commodity, commodity),
            "currentPrice": None, "maxPrice": None, "minPrice": None,
            "yesterdayPrice": None, "priceChange": None,
            "arrivalQuantity": 0, "updateDate": "N/A",
            "mspValue": msp_v, "priceVsMsp": None,
            "validRangeMin": lo, "validRangeMax": hi,
            "priceStatus": "UNVERIFIED",
            "sourcesUsed": [], "sourcesRejected": rejected,
            "bechainIndex": {"signal": "NONE", "signalHindi": "डेटा नहीं", "score": 0},
            "isRainExpected": is_rain, "weatherAlert": alert, "marketImpact": impact,
            "dataSourceCount": 0, "cacheStatus": "LIVE",
            "lastFetchedAt": datetime.now().isoformat(),
            "history3Day": None, "demandSupply": None,
            "crossCheck": None, "seasonalAnalysis": None,
        }
        CACHE.set(state, city, commodity, result)
        DISCOVERY.record(state, city, result)
        return result

    # Save to DB
    db_save(state, city, commodity, final_price,
            gov_data.get("max", final_price) if gov_data else final_price,
            gov_data.get("min", final_price) if gov_data else final_price,
            arrival_qty, srcs,
            "VERIFIED" if len(set(srcs)) >= 2 else "SINGLE_SOURCE")

    # Cross-check (multi-mandi fake detection)
    cross = multi_mandi_check(commodity, state, city, final_price)
    verified_price = final_price
    if cross["isFake"] and cross.get("avgOtherPrice"):
        verified_price = cross["avgOtherPrice"]

    # History analysis
    hist = history_analysis(state, city, commodity, verified_price)

    # Demand-Supply
    ds = demand_supply(commodity, state, city, arrival_qty, verified_price)

    # Seasonal prediction
    seasonal = seasonal_prediction(commodity, state, verified_price)

    prev = hist.get("yesterdayPrice") or (gov_data.get("prev") if gov_data else None)
    n_srcs = len(set(srcs))

    result = {
        "commodityName":    commodity,
        "commodityHindi":   HINDI.get(commodity, commodity),
        "currentPrice":     verified_price,
        "rawFetchedPrice":  final_price,
        "maxPrice":  gov_data.get("max", verified_price) if gov_data else verified_price,
        "minPrice":  gov_data.get("min", verified_price) if gov_data else verified_price,
        "yesterdayPrice":   prev,
        "priceChange":      round(verified_price - prev, 2) if prev else None,
        "arrivalQuantity":  arrival_qty,
        "updateDate":       gov_data.get("date", "N/A") if gov_data else "N/A",
        "mspValue":         msp_v,
        "priceVsMsp":       round(verified_price - msp_v, 2) if msp_v else None,
        "validRangeMin":    lo,
        "validRangeMax":    hi,
        "priceStatus":      ("VERIFIED" if n_srcs >= 2
                             else "SINGLE_SOURCE" if n_srcs == 1
                             else "UNVERIFIED"),
        "sourcesUsed":      list(set(srcs)),
        "sourcesRejected":  rejected,
        "bechainIndex":     bechain_index(commodity, verified_price, prev, state),
        "isRainExpected":   is_rain,
        "weatherAlert":     alert,
        "marketImpact":     impact,
        "dataSourceCount":  n_srcs,
        "cacheStatus":      "LIVE",
        "lastFetchedAt":    datetime.now().isoformat(),
        "history3Day":      hist,
        "demandSupply":     ds,
        "crossCheck":       cross,
        "seasonalAnalysis": seasonal,
    }

    CACHE.set(state, city, commodity, result)
    DISCOVERY.record(state, city, result)

    # ADD: Check price alerts for this commodity
    if verified_price:
        triggered = db_alerts_check(commodity, state, city, verified_price)
        if triggered:
            log.info("ALERT TRIGGERED: %s/%s Rs%s — %d alerts", commodity, city, verified_price, len(triggered))
            result["alertsTriggered"] = triggered

    return result

# ================================================================
# SECTION 9: BACKGROUND TASKS
# ================================================================

PRELOAD = [
    ("Rajasthan", "Nagaur"),   # Primary mandi - startup me load hoga
    ("Rajasthan", "Jaipur"),   # Secondary
    ("Madhya Pradesh", "Indore"),
    ("Maharashtra", "Nashik"),
    ("Gujarat", "Unjha"),
    ("Punjab", "Ludhiana"),
    ("Haryana", "Karnal"),
]


def scan_mandi(state, city):
    crops = STATE_DATA.get(state, {}).get("crops", [])
    log.info("SCAN START %s/%s: %d crops", state, city, len(crops))
    is_rain, impact, alert = get_weather(city)
    loaded = 0
    for com in crops:
        try:
            r = process(com, state, city, is_rain, impact, alert)
            if r.get("currentPrice"):
                loaded += 1
            time.sleep(1.0)  # Sources pe load kam - rate limit se bachega
        except Exception as e:
            log.warning("SCAN ERR %s: %s", com, e)
    log.info("SCAN DONE %s/%s: %d/%d", state, city, loaded, len(crops))
    return loaded


def bg_refresh():
    log.info("BG_REFRESH started")
    time.sleep(300)  # 5 min wait - startup complete hone do
    while True:
        log.info("BG_REFRESH cycle")
        for state, city in PRELOAD:
            try:
                scan_mandi(state, city)
                time.sleep(30)  # Sources pe kam load - 10s se 30s
            except Exception as e:
                log.error("BG_REFRESH err %s: %s", state, e)
        log.info("BG_REFRESH sleeping 6h")
        time.sleep(6 * 3600)  # 4h se 6h - mandi data utna jaldi nahi badlta


def startup():
    log.info("STARTUP: DB init + light preload (5 crops only)")
    db_init()
    # MEM FIX: sirf top 5 Rajasthan crops preload - full scan bg_refresh me hoga
    # Full scan = 20 crops x 5 sources = 100 HTTP calls = RAM spike at startup
    TOP5 = ["Mustard", "Wheat", "Chana", "Moong", "Guar"]
    is_rain, impact, alert = get_weather("Nagaur")
    for com in TOP5:
        try:
            process(com, "Rajasthan", "Nagaur", is_rain, impact, alert)
            time.sleep(0.5)
        except Exception as e:
            log.error("STARTUP err %s: %s", com, e)
    log.info("STARTUP done. Discovery: %s", DISCOVERY.count())


@app.on_event("startup")
async def on_startup():
    log.info("=" * 55)
    log.info("MandiPulse v9.0 STARTING")
    log.info("=" * 55)
    threading.Thread(target=startup, daemon=True).start()
    threading.Thread(target=bg_refresh, daemon=True).start()

# ================================================================
# SECTION 10: API ENDPOINTS
# ================================================================

@app.get("/")
def root():
    dc = DISCOVERY.count()
    cs = CACHE.status()
    return {
        "status": "MandiPulse v9.0 Online",
        "version": "9.0",
        "discovery": dc,
        "cache": cs,
        "states": len(STATE_DATA),
        "features": [
            "20 states x local crops (22 crops with seasonal data)",
            "6 data sources (GOV+Agmarknet+eNAM+Local+RSS+SOPA)",
            "Fake price detection (multi-mandi cross-check)",
            "3-day SQLite price history",
            "Demand-Supply analysis with seasonal bias",
            "Auto-discovery live list",
            "Smart 6hr cache + 6hr auto-refresh",
            "Seasonal prediction 22 crops (30/60/90 days)",
            "Quantity Calculator (grade + transport cost)",
            "Multi-mandi price compare",
            "Buy/Sell listings board",
            "Price alerts (ABOVE/BELOW target)",
            "User preferences (default mandi + fav crops)",
            "Mandi open/close timing (IST)",
        ],
        "endpoints": [
            "GET  /api/bulk-predictions?state=X&city=Y",
            "GET  /api/mandi-predictions?commodity=X&state=Y&city=Z",
            "GET  /api/compare?commodity=X&state=Y&cities=A,B,C",
            "GET  /api/live-list?state=X&city=Y",
            "GET  /api/live-list/poll?state=X&city=Y&since=ISO",
            "POST /api/scan-mandi?state=X&city=Y",
            "GET  /api/seasonal-analysis?commodity=X&state=Y&city=Z",
            "GET  /api/market-outlook?state=X&city=Y",
            "GET  /api/price-history?commodity=X&state=Y&city=Z",
            "GET  /api/mandi-timing",
            "GET  /api/state-crops?state=X",
            "GET  /api/all-states",
            "GET  /api/markets?commodity=X&state=Y",
            "GET  /api/mandipulse/calculate?commodity=X&state=Y&city=Z&qty_quintal=50&transport_per_q=40&grade=FAQ",
            "GET  /api/mandipulse/dashboard?commodity=X&state=Y&city=Z",
            "GET  /api/listings/all?commodity=X&state=Y&ltype=SELL",
            "POST /api/listings/add",
            "DEL  /api/listings/{id}",
            "POST /api/alerts/add",
            "GET  /api/alerts/all?commodity=X",
            "GET  /api/alerts/check?commodity=X&state=Y&city=Z",
            "GET  /api/user/prefs/{user_id}",
            "POST /api/user/prefs/{user_id}",
            "GET  /api/user/dashboard/{user_id}",
        ]
    }


@app.get("/health")
def health():
    import os, resource
    dc = DISCOVERY.count()
    cs = CACHE.status()
    # RAM usage check
    try:
        mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        mem_mb = -1
    return {
        "status": "ok", "version": "9.0",
        "cache_items": cs["total"],
        "cache_max": 300,
        "discovery_items": dc["total"],
        "discovery_max": 500,
        "discovered_with_price": dc["with_price"],
        "ram_mb": round(mem_mb, 1),
        "active_requests": _active_requests,
        "max_concurrent": MAX_CONCURRENT,
        "rate_tracked_ips": len(_rate_data),
        "db": DB_PATH,
        "time": datetime.now().isoformat()
    }


@app.get("/api/bulk-predictions")
def bulk(
    request: Request,
    state: str = Query(default="Rajasthan"),
    city:  str = Query(default="Nagaur"),
    commodities: str = Query(default="AUTO"),
    force_refresh: bool = Query(default=False)
):
    # SCALE FIX: Rate limit - max 5 bulk calls per minute per IP
    ip = request.client.host if request.client else "unknown"
    if not rate_check(ip, max_req=5, window=60):
        raise HTTPException(status_code=429, detail={
            "error": "Rate limit", "message": "1 minute me 5 se zyada bulk request allowed nahi", "retry_after": 60
        })

    if commodities == "AUTO" or not commodities:
        comm_list = STATE_DATA.get(state, {}).get("crops",
                    list(GLOBAL_RANGES.keys())[:15])
    else:
        comm_list = [c.strip() for c in commodities.split(",") if c.strip()]

    # SCALE FIX: max 15 crops per bulk request
    comm_list = comm_list[:15]

    log.info("BULK %s/%s: %d crops ip=%s force=%s", state, city, len(comm_list), ip, force_refresh)
    is_rain, impact, alert = get_weather(city)
    results = []

    # SCALE FIX: use global pool - no new pool creation per request
    futs = {
        _GLOBAL_POOL.submit(process, c, state, city, is_rain, impact, alert, force_refresh): c
        for c in comm_list
    }
    for f in concurrent.futures.as_completed(futs, timeout=15):
        try:
            results.append(f.result())
        except Exception as e:
            log.error("BULK thread err: %s", e)

    order = {"VERIFIED": 0, "SINGLE_SOURCE": 1, "UNVERIFIED": 2}
    results.sort(key=lambda x: order.get(x.get("priceStatus", "UNVERIFIED"), 3))
    verified = sum(1 for r in results if r.get("priceStatus") == "VERIFIED")
    single   = sum(1 for r in results if r.get("priceStatus") == "SINGLE_SOURCE")

    return {
        "state": state, "market": city,
        "fetchedAt": datetime.now().isoformat(),
        "serverVersion": "9.0",
        "totalCount": len(results),
        "verifiedCount": verified,
        "singleSourceCount": single,
        "weather": {"isRain": is_rain, "alert": alert, "impact": impact},
        "commodities": results,
    }


@app.get("/api/mandi-predictions")
def single(
    commodity: str = Query(...),
    state:     str = Query(...),
    city:      str = Query(...),
    force_refresh: bool = Query(default=False)
):
    is_rain, impact, alert = get_weather(city)
    r = process(commodity, state, city, is_rain, impact, alert, force_refresh)
    r["locationName"] = city
    r["predictionNote"] = (
        "बहु-स्रोत सत्यापित भाव" if r.get("priceStatus") != "UNVERIFIED"
        else "डेटा उपलब्ध नहीं"
    )
    return r


@app.get("/api/live-list")
def live_list(
    state: str = Query(default="Rajasthan"),
    city:  str = Query(default="Nagaur"),
    only_verified: bool = Query(default=False)
):
    items = DISCOVERY.all(state=state, city=city)
    if only_verified:
        items = [i for i in items
                 if i.get("priceStatus") in ("VERIFIED", "SINGLE_SOURCE")]
    return {
        "state": state, "city": city,
        "fetchedAt": datetime.now().isoformat(),
        "totalItems": len(items),
        "serverVersion": "9.0",
        "items": items,
    }


@app.get("/api/live-list/poll")
def live_list_poll(
    state: str = Query(default="Rajasthan"),
    city:  str = Query(default="Nagaur"),
    since: str = Query(default="")
):
    checked_at = datetime.now().isoformat()
    if not since:
        items = DISCOVERY.all(state=state, city=city)
        return {
            "state": state, "city": city,
            "checkedAt": checked_at, "isFirstLoad": True,
            "newItems": [], "updatedItems": [], "allItems": items,
            "totalCount": len(items),
        }
    updates = DISCOVERY.since(since, state=state, city=city)
    new_items     = [i for i in updates if i.get("isNew")]
    updated_items = [i for i in updates
                     if i.get("priceChanged") and not i.get("isNew")]
    return {
        "state": state, "city": city,
        "checkedAt": checked_at, "since": since,
        "isFirstLoad": False,
        "newItems": new_items,
        "updatedItems": updated_items,
        "totalChanges": len(updates),
        "hasChanges": len(updates) > 0,
    }


@app.post("/api/scan-mandi")
def scan_mandi_ep(
    state: str = Query(default="Rajasthan"),
    city:  str = Query(default="Nagaur"),
    background_tasks: BackgroundTasks = None,
    async_mode: bool = Query(default=True)   # BUG FIX: explicit param
):
    # BUG FIX: background_tasks object always exists in FastAPI — check async_mode instead
    if async_mode:
        background_tasks.add_task(scan_mandi, state, city)
        return {
            "message": "Scanning %s/%s in background" % (state, city),
            "pollAt": "/api/live-list/poll?state=%s&city=%s" % (state, city),
        }
    loaded = scan_mandi(state, city)
    return {"message": "Scan complete", "loaded": loaded}


@app.get("/api/seasonal-analysis")
def seasonal_ep(
    commodity: str = Query(...),
    state:     str = Query(default="Rajasthan"),
    city:      str = Query(default="Nagaur")
):
    is_rain, impact, alert = get_weather(city)
    item = process(commodity, state, city, is_rain, impact, alert)
    price = item.get("currentPrice")
    if price is None:
        return {
            "commodity": commodity, "currentPrice": None,
            "message": "भाव उपलब्ध नहीं",
            "history3Day": item.get("history3Day"),
        }
    seasonal = seasonal_prediction(commodity, state, price)
    hist = item.get("history3Day", {})
    ds   = item.get("demandSupply", {})
    cc   = item.get("crossCheck", {})
    return {
        "commodity": commodity,
        "commodityHindi": HINDI.get(commodity, commodity),
        "state": state, "city": city,
        "currentPrice": price,
        "fetchedAt": datetime.now().isoformat(),
        "serverVersion": "9.0",
        "todayVsYesterday": {
            "yesterdayPrice": hist.get("yesterdayPrice"),
            "priceChange": hist.get("priceChange"),
            "changePct": hist.get("priceChangePct"),
            "arrow": hist.get("arrow", "—"),
            "color": hist.get("arrowColor", "grey"),
        },
        "history3Day": {
            "chart": hist.get("chart3Day", []),
            "high": hist.get("weekly3DayHigh"),
            "low": hist.get("weekly3DayLow"),
            "avg": hist.get("weekly3DayAvg"),
            "daysOfData": hist.get("daysOfData", 0),
        },
        "demandSupply": {
            "arrivalToday": ds.get("arrivalQty", 0),
            "avg3Day": ds.get("avg3DayArrival"),
            "supplyHindi": ds.get("supplyHindi", "N/A"),
            "demandHindi": ds.get("demandHindi", "N/A"),
            "trendHindi": ds.get("trendHindi", "N/A"),
            "trendArrow": ds.get("trendArrow", "→"),
            "predictedChange": ds.get("predictedChangePct", 0),
        },
        "multiMandiCheck": {
            "isFake": cc.get("isFake", False),
            "confidence": cc.get("confidence", 0),
            "verdict": cc.get("verdictHindi", "N/A"),
            "otherMandis": cc.get("otherMandis", {}),
            "avgOtherPrice": cc.get("avgOtherPrice"),
        },
        "seasonalAnalysis": seasonal,
        "bechainIndex": item.get("bechainIndex", {}),
        "sourcesUsed": item.get("sourcesUsed", []),
        "priceStatus": item.get("priceStatus", ""),
    }


@app.get("/api/market-outlook")
def market_outlook(
    state: str = Query(default="Rajasthan"),
    city:  str = Query(default="Nagaur")
):
    crops = STATE_DATA.get(state, {}).get("crops", [])[:12]
    is_rain, impact, alert = get_weather(city)
    outlook = []
    # BUG FIX: parallel processing — serial loop bahut slow tha
    def _outlook_one(com):
        item = process(com, state, city, is_rain, impact, alert)
        price = item.get("currentPrice")
        if price is None:
            return None
        sd = SEASONAL.get(com, {})
        pred_pct = sd.get("pred_30d_pct")   # BUG FIX: None vs 0 distinguish
        return {
            "commodity": com,
            "commodityHindi": HINDI.get(com, com),
            "currentPrice": price,
            "priceStatus": item.get("priceStatus", ""),
            "priceTrend": sd.get("price_trend_90d", "STABLE"),
            "sellAdvice": sd.get("sell_advice", "नजर रखें"),
            "reason": sd.get("reason", ""),
            "pred30dPct": pred_pct,
            "pred30dPrice": round(price * (1 + pred_pct / 100)) if pred_pct is not None else None,
            "bechainIndex": item.get("bechainIndex", {}),
        }
    # SCALE FIX: use global pool
    futs_out = [_GLOBAL_POOL.submit(_outlook_one, c) for c in crops]
    for res in concurrent.futures.as_completed(futs_out, timeout=15):
        try:
            r = res.result()
            if r:
                outlook.append(r)
        except Exception:
            pass
    outlook.sort(key=lambda x: (
        0 if x["priceTrend"] == "UP" else
        1 if x["priceTrend"] == "STABLE" else 2
    ))
    return {
        "state": state, "city": city,
        "fetchedAt": datetime.now().isoformat(),
        "season": "Kharif 2026-27",
        "weather": {"isRain": is_rain, "alert": alert},
        "totalCrops": len(outlook),
        "outlook": outlook,
    }


@app.get("/api/price-history")
def price_history(
    commodity: str = Query(...),
    state:     str = Query(...),
    city:      str = Query(...),
    days:      int = Query(default=7, ge=1, le=90)  # BUG FIX: clamp 1-90 days
):
    hist = db_history(state, city, commodity, days=days)
    prices = [h["modal"] for h in hist if h.get("modal")]
    return {
        "commodity": commodity,
        "commodityHindi": HINDI.get(commodity, commodity),
        "state": state, "city": city, "days": days,
        "history": hist,
        "weeklyHigh": max(prices) if prices else None,
        "weeklyLow":  min(prices) if prices else None,
        "weeklyAvg":  round(sum(prices) / len(prices), 2) if prices else None,
    }


@app.get("/api/state-crops")
def state_crops(state: str = Query(...)):
    sd = STATE_DATA.get(state)
    if not sd:
        return {"state": state, "crops": list(GLOBAL_RANGES.keys()), "mandis": []}
    return {"state": state, "crops": sd["crops"],
            "mandis": sd["mandis"], "count": len(sd["crops"])}


@app.get("/api/all-states")
def all_states():
    return {
        "states": [
            {"name": s, "topCrops": d["crops"][:5],
             "totalCrops": len(d["crops"]), "majorMandis": d["mandis"][:4]}
            for s, d in STATE_DATA.items()
        ],
        "totalStates": len(STATE_DATA)
    }


@app.get("/api/markets")
def markets(commodity: str = Query(...), state: str = Query(...)):
    return {"markets": STATE_DATA.get(state, {}).get("mandis", [])}


@app.get("/api/mandipulse/calculate")
def calculate(
    commodity:       str   = Query(...),
    state:           str   = Query(...),
    city:            str   = Query(...),
    qty_quintal:     float = Query(default=1.0, ge=0.1, le=10000),
    transport_per_q: float = Query(default=0.0, ge=0.0, le=5000),
    grade:           str   = Query(default="Average")
):
    is_rain, impact, alert_w = get_weather(city)
    r = process(commodity, state, city, is_rain, impact, alert_w)
    price = r.get("currentPrice")
    msp_v = MSP.get(commodity, 0)
    if price is None:
        return {"commodityHindi": HINDI.get(commodity, commodity),
                "location": city, "currentPrice": None, "dataAvailable": False}
    grade_adj       = {"FAQ": 300, "Average": 0, "Below Average": -250}.get(grade, 0)
    effective_price = price + grade_adj
    gross_value     = round(effective_price * qty_quintal, 2)
    transport_total = round(transport_per_q * qty_quintal, 2)
    net_value       = round(gross_value - transport_total, 2)
    net_per_q       = round(effective_price - transport_per_q, 2)
    msp_total       = round(msp_v * qty_quintal, 2) if msp_v else None
    return {
        "commodityHindi":    HINDI.get(commodity, commodity),
        "commodity":         commodity,
        "location":          city,
        "currentPrice":      price,
        "dataAvailable":     True,
        "mandiTiming":       mandi_is_open(),
        "quantityCalc": {
            "qtyQuintal":      qty_quintal,
            "grade":           grade,
            "gradeAdjustment": grade_adj,
            "effectivePrice":  effective_price,
            "grossValue":      gross_value,
            "transportPerQ":   transport_per_q,
            "transportTotal":  transport_total,
            "netValue":        net_value,
            "netPerQuintal":   net_per_q,
            "mspTotalValue":   msp_total,
            "summary":         "%d quintal x Rs%d = Rs%.0f (net Rs%.0f)" % (
                                  int(qty_quintal), int(effective_price), gross_value, net_value),
        },
        "mspCalculator": {
            "mspValue":    msp_v, "currentPrice": price,
            "difference":  round(price - msp_v, 2),
            "hindi":       "MSP ke upar" if price > msp_v else "MSP se neeche",
            "color":       "green" if price > msp_v else "red",
            "pct":         round((price - msp_v) / msp_v * 100, 1) if msp_v else 0,
        },
        "bechainIndex":    r.get("bechainIndex", {}),
        "sourcesUsed":     r.get("sourcesUsed", []),
        "dataSourceCount": r.get("dataSourceCount", 0),
        "history3Day":     r.get("history3Day"),
        "demandSupply":    r.get("demandSupply"),
        "crossCheck":      r.get("crossCheck"),
        "seasonalAnalysis": r.get("seasonalAnalysis"),
        "weatherImpact":   {"isRainExpected": is_rain, "alert": alert_w},
    }


@app.get("/api/mandipulse/dashboard")
def dashboard(
    commodity: str = Query(...),
    state:     str = Query(...),
    city:      str = Query(...)
):
    is_rain, impact, alert = get_weather(city)
    r = process(commodity, state, city, is_rain, impact, alert)
    return {
        "appName": "MandiPulse", "serverVersion": "9.0",
        "commodity": commodity, "location": city,
        "currentPrice": r.get("currentPrice"),
        "maxPrice": r.get("maxPrice"),
        "minPrice": r.get("minPrice"),
        "yesterdayPrice": r.get("yesterdayPrice"),
        "arrivalQty": r.get("arrivalQuantity", 0),
        "updateDate": r.get("updateDate", "N/A"),
        "priceStatus": r.get("priceStatus", ""),
        "sourcesUsed": r.get("sourcesUsed", []),
        "bechainIndex": r.get("bechainIndex", {}),
        "history3Day": r.get("history3Day"),
        "demandSupply": r.get("demandSupply"),
        "crossCheck": r.get("crossCheck"),
        "seasonalAnalysis": r.get("seasonalAnalysis"),
        "weather": {"isRain": is_rain, "alert": alert, "impact": impact},
    }


@app.get("/api/cache-status")
def cache_status():
    return {**CACHE.status(), "discovery": DISCOVERY.count()}


@app.post("/api/cache-clear")
def cache_clear(state: str = None, city: str = None):
    CACHE.clear(state, city)
    return {"message": "Cache cleared %s/%s" % (state or "ALL", city or "ALL")}



# ================================================================
# LISTINGS — Real buy/sell board
# ================================================================

@app.get("/api/listings/all")
def listings(
    commodity: str = Query(default=None),
    state:     str = Query(default=None),
    ltype:     str = Query(default=None, description="SELL or BUY"),
    limit:     int = Query(default=30, ge=1, le=100)
):
    data = db_listings_get(commodity=commodity, state=state, ltype=ltype, limit=limit)
    return {
        "total": len(data),
        "listings": data,
        "fetchedAt": datetime.now().isoformat()
    }

@app.post("/api/listings/add")
def add_listing(data: dict):
    required = ["type", "commodity", "state", "city", "qty_quintal", "price_per_q"]
    for f in required:
        if f not in data:
            raise HTTPException(status_code=400, detail="Missing field: %s" % f)
    if data.get("type") not in ("SELL", "BUY"):
        raise HTTPException(status_code=400, detail="type must be SELL or BUY")
    lid = db_listing_add(
        ltype     = data["type"],
        commodity = data["commodity"],
        state     = data["state"],
        city      = data["city"],
        qty       = float(data["qty_quintal"]),
        price     = float(data["price_per_q"]),
        quality   = data.get("quality", "Average"),
        contact   = data.get("contact", ""),
        note      = data.get("note", ""),
        days      = int(data.get("valid_days", 7))
    )
    if not lid:
        raise HTTPException(status_code=500, detail="DB error")
    return {"status": "success", "listingId": lid,
            "message": "Listing %s saved" % lid}

@app.delete("/api/listings/{listing_id}")
def delete_listing(listing_id: str):
    ok = db_listing_delete(listing_id)
    return {"status": "deleted" if ok else "not_found", "listingId": listing_id}


# ================================================================
# PRICE ALERTS
# ================================================================

@app.post("/api/alerts/add")
def alert_add(data: dict):
    required = ["commodity", "state", "city", "target_price", "condition"]
    for f in required:
        if f not in data:
            raise HTTPException(status_code=400, detail="Missing: %s" % f)
    if data.get("condition") not in ("ABOVE", "BELOW"):
        raise HTTPException(status_code=400, detail="condition must be ABOVE or BELOW")
    aid = db_alert_add(
        commodity    = data["commodity"],
        state        = data["state"],
        city         = data["city"],
        target_price = float(data["target_price"]),
        condition    = data["condition"],
        contact      = data.get("contact", "")
    )
    if not aid:
        raise HTTPException(status_code=500, detail="DB error")
    cond_hi = "पहुँचने पर" if data["condition"] == "ABOVE" else "गिरने पर"
    return {
        "status": "success",
        "alertId": aid,
        "message": "%s भाव ₹%s %s alert set" % (
            data["commodity"], data["target_price"], cond_hi)
    }

@app.get("/api/alerts/all")
def alerts_get(
    commodity: str = Query(default=None),
    state:     str = Query(default=None)
):
    data = db_alerts_get(commodity=commodity, state=state)
    return {"total": len(data), "alerts": data}

@app.get("/api/alerts/check")
def alerts_check_ep(
    commodity: str = Query(...),
    state:     str = Query(...),
    city:      str = Query(...)
):
    """Manually check if any alerts triggered for a commodity right now."""
    is_rain, impact, alert_w = get_weather(city)
    r = process(commodity, state, city, is_rain, impact, alert_w)
    price = r.get("currentPrice")
    if not price:
        return {"triggered": [], "currentPrice": None}
    triggered = db_alerts_check(commodity, state, city, price)
    return {
        "currentPrice": price,
        "triggered": triggered,
        "triggeredCount": len(triggered)
    }


# ================================================================
# USER PREFERENCES
# ================================================================

@app.get("/api/user/prefs/{user_id}")
def prefs_get(user_id: str):
    p = db_prefs_get(user_id)
    if not p:
        raise HTTPException(status_code=404, detail="User not found")
    return p

@app.post("/api/user/prefs/{user_id}")
def prefs_save(user_id: str, data: dict):
    ok = db_prefs_save(
        user_id       = user_id,
        default_state = data.get("default_state", "Rajasthan"),
        default_city  = data.get("default_city", "Nagaur"),
        fav_crops     = data.get("fav_crops", [])
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Save failed")
    return {"status": "saved", "user_id": user_id}

@app.get("/api/user/dashboard/{user_id}")
def user_dashboard(user_id: str):
    """User ki saved mandi ka personalized dashboard."""
    p = db_prefs_get(user_id)
    state = p.get("default_state", "Rajasthan") if p else "Rajasthan"
    city  = p.get("default_city",  "Nagaur")    if p else "Nagaur"
    crops = p.get("fav_crops", [])               if p else []
    if not crops:
        crops = STATE_DATA.get(state, {}).get("crops", [])[:5]
    is_rain, impact, alert_w = get_weather(city)
    results = []
    futs = [_GLOBAL_POOL.submit(process, c, state, city, is_rain, impact, alert_w)
            for c in crops[:8]]
    for f in concurrent.futures.as_completed(futs, timeout=15):
        try:
            results.append(f.result())
        except Exception:
            pass
    results.sort(key=lambda x: 0 if x.get("currentPrice") else 1)
    return {
        "userId": user_id,
        "state": state, "city": city,
        "favCrops": crops,
        "mandiTiming": mandi_is_open(),
        "weather": {"isRain": is_rain, "alert": alert_w},
        "prices": results,
        "fetchedAt": datetime.now().isoformat()
    }


# ================================================================
# MULTI-MANDI COMPARE
# ================================================================

@app.get("/api/compare")
def compare_mandis(
    commodity: str = Query(...),
    state:     str = Query(...),
    cities:    str = Query(..., description="Comma-separated: Nagaur,Jodhpur,Merta City")
):
    """Ek commodity ka alag alag mandis mein bhav compare karo."""
    city_list = [c.strip() for c in cities.split(",") if c.strip()][:5]
    if len(city_list) < 2:
        raise HTTPException(status_code=400, detail="Kam se kam 2 cities do")
    is_rain, impact, alert_w = get_weather(city_list[0])
    results = {}
    futs = {
        _GLOBAL_POOL.submit(process, commodity, state, c, is_rain, impact, alert_w): c
        for c in city_list
    }
    for f in concurrent.futures.as_completed(futs, timeout=15):
        c = futs[f]
        try:
            r = f.result()
            results[c] = {
                "city":         c,
                "currentPrice": r.get("currentPrice"),
                "priceStatus":  r.get("priceStatus", ""),
                "maxPrice":     r.get("maxPrice"),
                "minPrice":     r.get("minPrice"),
                "sourcesUsed":  r.get("sourcesUsed", []),
                "bechainIndex": r.get("bechainIndex", {}),
            }
        except Exception:
            results[c] = {"city": c, "currentPrice": None, "error": True}

    prices_found = {c: v["currentPrice"] for c, v in results.items() if v.get("currentPrice")}
    best_city    = max(prices_found, key=prices_found.get) if prices_found else None
    worst_city   = min(prices_found, key=prices_found.get) if prices_found else None

    return {
        "commodity":      commodity,
        "commodityHindi": HINDI.get(commodity, commodity),
        "state":          state,
        "results":        list(results.values()),
        "bestCity":       best_city,
        "bestPrice":      prices_found.get(best_city),
        "worstCity":      worst_city,
        "worstPrice":     prices_found.get(worst_city),
        "diffAmount":     round(prices_found[best_city] - prices_found[worst_city], 2)
                          if best_city and worst_city and best_city != worst_city else 0,
        "advice":         "%s में ₹%d ज़्यादा मिलेगा" % (
                           best_city, prices_found[best_city] - prices_found[worst_city])
                          if best_city and worst_city and best_city != worst_city else "सभी मंडी समान",
        "fetchedAt":      datetime.now().isoformat()
    }


# ================================================================
# MANDI TIMING
# ================================================================

@app.get("/api/mandi-timing")
def mandi_timing():
    """Abhi mandi khuli hai ya band?"""
    return mandi_is_open()
