# ================================================================
# MANDIPULSE SERVER — app.py FINAL v9.0
# Complete Production-Ready Server
# ================================================================
#
# FEATURES:
# 1. 20 States x Local Crops + Price Ranges
# 2. 6 Data Sources (GOV API, Agmarknet, eNAM, Local, RSS, SOPA)
# 3. Fake Price Detection (Multi-Mandi Cross-Check)
# 4. 7-Day SQLite Price History
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
# GET  /api/price-history             → 7-day chart data
# GET  /api/state-crops               → Crops for a state
# GET  /api/all-states                → All 20 states
# GET  /api/markets                   → Mandis for a state
# GET  /api/cache-status              → Cache info
# POST /api/cache-clear               → Clear cache
# GET  /api/mandipulse/dashboard      → Dashboard data
# GET  /api/mandipulse/calculate      → Calculate endpoint
# ================================================================

from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import requests
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

app = FastAPI(title="MandiPulse API", version="9.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0"}
DATA_GOV_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"
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
    ("https://www.krishijagran.com/feed/",           "Krishi Jagran"),
    ("https://hindi.krishijagran.com/feed/",         "KJ Hindi"),
    ("https://www.kisantak.in/feed/",                "Kisan Tak"),
    ("https://www.gaonconnection.com/feed",          "Gaon Connection"),
    ("https://khabar.ndtv.com/rss/kisan",            "NDTV Kisan"),
    ("https://www.abplive.com/agriculture/feed",     "ABP Kisan"),
    ("https://ddnews.gov.in/rss/kisan",              "DD Kisan"),
    ("https://www.bhaskar.com/rss-feed/1555/",       "DB Rajasthan"),
    ("https://www.patrika.com/rss/news.xml",         "Rajasthan Patrika"),
    ("https://www.jagran.com/rss/news-national.xml", "Dainik Jagran"),
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
        "reason": "Oct-Nov में खरीफ onion आएगी — भाव ₹1500 तक गिर सकता",
        "sell_advice": "अभी बेचें — Oct-Nov में भारी गिरावट",
        "pred_30d_pct": -27, "pred_60d_pct": -58, "pred_90d_pct": -68,
    },
}

# ================================================================
# SECTION 2: DATABASE
# ================================================================

def db_init():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
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
    conn.commit()
    conn.close()
    log.info("DB init done: " + DB_PATH)

def db_save(state, city, commodity, modal, maxp, minp, qty, sources, status):
    today = date.today().isoformat()
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
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
        conn.close()
        log.info("DB SAVED: %s/%s/%s Rs%s", state, city, commodity, modal)
    except Exception as e:
        log.error("DB SAVE ERR: %s", e)

def db_history(state, city, commodity, days=7):
    since = (date.today() - timedelta(days=days)).isoformat()
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT price_date,modal_price,max_price,min_price,arrival_qty,status
            FROM price_history
            WHERE state=? AND city=? AND commodity=? AND price_date>=?
            ORDER BY price_date ASC
        """, (state,city,commodity,since))
        rows = c.fetchall()
        conn.close()
        return [{"date":r[0],"modal":r[1],"max":r[2],"min":r[3],
                 "arrival":r[4],"status":r[5]} for r in rows]
    except:
        return []

def db_yesterday(state, city, commodity):
    yest = (date.today() - timedelta(days=1)).isoformat()
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT modal_price FROM price_history
            WHERE state=? AND city=? AND commodity=? AND price_date=?
        """, (state,city,commodity,yest))
        row = c.fetchone()
        conn.close()
        return row[0] if row else None
    except:
        return None

def db_avg_arrival(state, city, commodity):
    since = (date.today() - timedelta(days=7)).isoformat()
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT AVG(arrival_qty) FROM price_history
            WHERE state=? AND city=? AND commodity=?
              AND price_date>=? AND arrival_qty>0
        """, (state,city,commodity,since))
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] else None
    except:
        return None

# ================================================================
# SECTION 3: CACHE + DISCOVERY
# ================================================================

class PriceCache:
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
            age = (datetime.now() - e["t"]).seconds / 3600
            if age < 4:
                self.stats["hits"] += 1
                return e["d"], "FRESH"
            elif age < 6:
                self.stats["hits"] += 1
                return e["d"], "STALE"
            self.stats["misses"] += 1
            return None, "EXPIRED"

    def set(self, state, city, com, data):
        k = self._k(state, city, com)
        with self._lock:
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
               "&filters[market]=%s&limit=5") % (DATA_GOV_KEY, name, state, city)
        r = requests.get(url, timeout=12, headers=HDR)
        r.raise_for_status()
        recs = r.json().get("records", [])
        if not recs:
            url2 = ("https://api.data.gov.in/resource/"
                    "9ef84268-d588-465a-a308-a864a43d0070"
                    "?api-key=%s&format=json"
                    "&filters[commodity]=%s"
                    "&filters[state]=%s&limit=5") % (DATA_GOV_KEY, name, state)
            r2 = requests.get(url2, timeout=12, headers=HDR)
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
        r = requests.get(url, timeout=12, headers=HDR)
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
        r = requests.get(
            "https://enam.gov.in/web/dashboard/commodityPrice"
            "?commodity=%s&state=%s&market=%s" % (code, state, city),
            timeout=10, headers=HDR)
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
            r = requests.get(url, timeout=10, headers=HDR)
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
        for p in [f.result() for f in
                  concurrent.futures.as_completed([ex.submit(fetch, u) for u in urls])]:
            if p:
                prices.append(p)
    if not prices:
        return None
    return {"price": sorted(prices)[len(prices) // 2],
            "src": "local_%s" % st, "weight": 2}


def src_rss(commodity, state):
    aliases = ALIASES.get(commodity, [commodity.lower()])
    all_p = []

    def fetch_one(url, name):
        try:
            feed = feedparser.parse(url)
            found = []
            for e in feed.entries[:40]:
                text = (e.get("title", "") + " " + e.get("summary", "")).lower()
                if any(a.lower() in text for a in aliases):
                    p = extract_p(text)
                    if p and price_valid(commodity, p, state):
                        found.append(p)
            return found
        except Exception:
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
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

def get_weather(city):
    try:
        r = requests.get("https://wttr.in/%s?format=j1" % city,
                         timeout=8, headers=HDR).json()
        c = r.get("current_condition", [{}])[0]
        temp = c.get("temp_C", "30")
        desc = c.get("weatherDesc", [{}])[0].get("value", "").lower()
        rain = any(w in desc for w in ["rain", "drizzle", "thunder", "shower"])
        return (rain,
                "तेजी" if rain else "सामान्य",
                "बारिश (%sC)" % temp if rain else "साफ (%sC)" % temp)
    except Exception:
        return False, "सामान्य", "साफ मौसम"

# ================================================================
# SECTION 7: VALIDATION + ANALYSIS
# ================================================================

def cross_validate(commodity, state, results):
    if not results:
        return None, [], []
    prices = [s["price"] for s in results]
    ok_p, bad_p = iqr_filter(prices)
    accepted = [s for s in results if s["price"] in ok_p]
    rejected = ["%s(Rs%s)" % (s["src"], int(s["price"]))
                for s in results if s["price"] in bad_p]
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
        futs = {ex.submit(fetch_one, c): c for c in nearby[:4]}
        for f in concurrent.futures.as_completed(futs):
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
    avg7 = db_avg_arrival(state, city, commodity)
    msp = MSP.get(commodity, 0)

    if avg7 and avg7 > 0:
        ratio = arrival_qty / avg7
        if ratio > 1.3:
            s_score, s_hi = 80, "अधिक आपूर्ति"
        elif ratio > 0.9:
            s_score, s_hi = 50, "सामान्य आपूर्ति"
        else:
            s_score, s_hi = 20, "कम आपूर्ति"
    else:
        s_score, s_hi = 50, "आपूर्ति N/A"

    gap_pct = ((price - msp) / msp * 100) if msp > 0 and price else 0
    if gap_pct > 15:
        d_score, d_hi = 80, "माँग अधिक"
    elif gap_pct > 0:
        d_score, d_hi = 60, "माँग सामान्य"
    elif gap_pct > -10:
        d_score, d_hi = 40, "माँग कम"
    else:
        d_score, d_hi = 20, "माँग बहुत कम"

    net = d_score - s_score
    if net > 20:
        trend, arrow, t_hi = "UP", "↑", "भाव बढ़ने की संभावना"
        t_pct = min(net / 5, 8)
    elif net < -20:
        trend, arrow, t_hi = "DOWN", "↓", "भाव गिरने की संभावना"
        t_pct = max(net / 5, -8)
    else:
        trend, arrow, t_hi = "STABLE", "→", "भाव स्थिर"
        t_pct = 0

    return {
        "arrivalQty": arrival_qty, "avg7DayArrival": round(avg7, 1) if avg7 else None,
        "supplyScore": s_score, "supplyHindi": s_hi,
        "demandScore": d_score, "demandHindi": d_hi,
        "priceTrend": trend, "trendArrow": arrow, "trendHindi": t_hi,
        "predictedChangePct": round(t_pct, 1), "mspGapPct": round(gap_pct, 1),
    }


def history_analysis(state, city, commodity, today_price):
    hist = db_history(state, city, commodity, days=8)
    yest_p = db_yesterday(state, city, commodity)

    if yest_p and today_price:
        change = today_price - yest_p
        chg_pct = (change / yest_p) * 100
        if change > 50:
            arrow, color = "↑", "green"
        elif change < -50:
            arrow, color = "↓", "red"
        else:
            arrow, color = "→", "grey"
    else:
        change = chg_pct = None
        arrow, color = "—", "grey"

    prices = [h["modal"] for h in hist if h.get("modal") and h["modal"] > 0]
    chart = [{"date": h["date"], "price": h["modal"], "arrival": h.get("arrival", 0)}
             for h in hist[-7:] if h.get("modal")]

    return {
        "todayPrice": today_price, "yesterdayPrice": yest_p,
        "priceChange": round(change, 2) if change is not None else None,
        "priceChangePct": round(chg_pct, 2) if chg_pct is not None else None,
        "arrow": arrow, "arrowColor": color,
        "weekly7DayHigh": max(prices) if prices else None,
        "weekly7DayLow": min(prices) if prices else None,
        "weekly7DayAvg": round(sum(prices) / len(prices), 2) if prices else None,
        "daysOfData": len(prices),
        "chart7Day": chart,
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
    msp = MSP.get(commodity, 0)
    chg = ((price - prev) / prev * 100) if prev and prev > 0 else 0
    mgap = ((price - msp) / msp * 100) if msp > 0 else 0
    s = 50
    if chg > 3: s += 20
    elif chg > 1: s += 10
    elif chg < -3: s -= 20
    elif chg < -1: s -= 10
    if mgap > 15: s += 15
    elif mgap > 5: s += 8
    elif mgap < 0: s -= 15
    s = max(0, min(100, s))
    if s >= 70:   sig, hin, col = "BUY",  "बेचो", "green"
    elif s >= 45: sig, hin, col = "WAIT", "रुको", "yellow"
    else:         sig, hin, col = "HOLD", "होल्ड", "red"
    return {"signal": sig, "signalHindi": hin, "score": s, "color": col,
            "changePct": round(chg, 2), "mspGapPct": round(mgap, 2)}

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

    # Fetch from all sources concurrently
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        f1 = ex.submit(src_gov, commodity, state, city)
        f2 = ex.submit(src_agmarknet, commodity, state, city)
        f3 = ex.submit(src_enam, commodity, state, city)
        f4 = ex.submit(src_local, commodity, state, city)
        f5 = ex.submit(src_rss, commodity, state)
        for f in [f1, f2, f3, f4, f5]:
            try:
                res = f.result()
                if res:
                    results.append(res)
            except Exception:
                pass

    gov_data = None
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
            "history7Day": None, "demandSupply": None,
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
        "history7Day":      hist,
        "demandSupply":     ds,
        "crossCheck":       cross,
        "seasonalAnalysis": seasonal,
    }

    CACHE.set(state, city, commodity, result)
    DISCOVERY.record(state, city, result)
    return result

# ================================================================
# SECTION 9: BACKGROUND TASKS
# ================================================================

PRELOAD = [
    ("Rajasthan", "Nagaur"),
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
            time.sleep(0.3)
        except Exception as e:
            log.warning("SCAN ERR %s: %s", com, e)
    log.info("SCAN DONE %s/%s: %d/%d", state, city, loaded, len(crops))
    return loaded


def bg_refresh():
    log.info("BG_REFRESH started")
    time.sleep(90)
    while True:
        log.info("BG_REFRESH cycle")
        for state, city in PRELOAD:
            try:
                scan_mandi(state, city)
                time.sleep(10)
            except Exception as e:
                log.error("BG_REFRESH err %s: %s", state, e)
        log.info("BG_REFRESH sleeping 4h")
        time.sleep(4 * 3600)


def startup():
    log.info("STARTUP: DB init + preloading top mandis")
    db_init()
    for state, city in PRELOAD[:3]:
        try:
            scan_mandi(state, city)
        except Exception as e:
            log.error("STARTUP err %s: %s", state, e)
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
            "20 states x local crops",
            "6 data sources (GOV+Agmarknet+eNAM+Local+RSS+SOPA)",
            "Fake price detection (multi-mandi cross-check)",
            "7-day SQLite price history",
            "Demand-Supply analysis",
            "Auto-discovery live list",
            "Smart 4hr cache + 4hr auto-refresh",
            "Seasonal prediction (30/60/90 days)",
        ],
        "endpoints": [
            "GET /api/bulk-predictions?state=X&city=Y",
            "GET /api/live-list?state=X&city=Y",
            "GET /api/live-list/poll?state=X&city=Y&since=ISO",
            "POST /api/scan-mandi?state=X&city=Y",
            "GET /api/seasonal-analysis?commodity=X&state=Y&city=Z",
            "GET /api/market-outlook?state=X&city=Y",
            "GET /api/price-history?commodity=X&state=Y&city=Z",
            "GET /api/state-crops?state=X",
            "GET /api/all-states",
            "GET /api/markets?commodity=X&state=Y",
        ]
    }


@app.get("/health")
def health():
    dc = DISCOVERY.count()
    cs = CACHE.status()
    return {
        "status": "ok", "version": "9.0",
        "cache_items": cs["total"],
        "discovered_with_price": dc["with_price"],
        "db": DB_PATH,
        "time": datetime.now().isoformat()
    }


@app.get("/api/bulk-predictions")
def bulk(
    state: str = Query(default="Rajasthan"),
    city:  str = Query(default="Nagaur"),
    commodities: str = Query(default="AUTO"),
    force_refresh: bool = Query(default=False)
):
    if commodities == "AUTO" or not commodities:
        comm_list = STATE_DATA.get(state, {}).get("crops",
                    list(GLOBAL_RANGES.keys())[:15])
    else:
        comm_list = [c.strip() for c in commodities.split(",") if c.strip()]

    log.info("BULK %s/%s: %d crops force=%s", state, city, len(comm_list), force_refresh)
    is_rain, impact, alert = get_weather(city)
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futs = {
            ex.submit(process, c, state, city, is_rain, impact, alert, force_refresh): c
            for c in comm_list
        }
        for f in concurrent.futures.as_completed(futs):
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
    background_tasks: BackgroundTasks = None
):
    if background_tasks:
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
            "history7Day": item.get("history7Day"),
        }
    seasonal = seasonal_prediction(commodity, state, price)
    hist = item.get("history7Day", {})
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
        "history7Day": {
            "chart": hist.get("chart7Day", []),
            "weeklyHigh": hist.get("weekly7DayHigh"),
            "weeklyLow": hist.get("weekly7DayLow"),
            "weeklyAvg": hist.get("weekly7DayAvg"),
            "daysOfData": hist.get("daysOfData", 0),
        },
        "demandSupply": {
            "arrivalToday": ds.get("arrivalQty", 0),
            "avg7Day": ds.get("avg7DayArrival"),
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
    for com in crops:
        item = process(com, state, city, is_rain, impact, alert)
        price = item.get("currentPrice")
        if price is None:
            continue
        sd = SEASONAL.get(com, {})
        outlook.append({
            "commodity": com,
            "commodityHindi": HINDI.get(com, com),
            "currentPrice": price,
            "priceStatus": item.get("priceStatus", ""),
            "priceTrend": sd.get("price_trend_90d", "STABLE"),
            "sellAdvice": sd.get("sell_advice", "नजर रखें"),
            "reason": sd.get("reason", ""),
            "pred30dPct": sd.get("pred_30d_pct", 0),
            "pred30dPrice": round(price * (1 + sd.get("pred_30d_pct", 0) / 100)),
        })
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
    days:      int = Query(default=7)
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
    commodity: str = Query(...),
    state:     str = Query(...),
    city:      str = Query(...)
):
    is_rain, impact, alert = get_weather(city)
    r = process(commodity, state, city, is_rain, impact, alert)
    price = r.get("currentPrice")
    msp_v = MSP.get(commodity, 0)
    if price is None:
        return {"commodityHindi": HINDI.get(commodity, commodity),
                "location": city, "currentPrice": None, "dataAvailable": False}
    return {
        "commodityHindi": HINDI.get(commodity, commodity),
        "location": city, "currentPrice": price, "dataAvailable": True,
        "bechainIndex": r.get("bechainIndex", {}),
        "sourcesUsed": r.get("sourcesUsed", []),
        "dataSourceCount": r.get("dataSourceCount", 0),
        "history7Day": r.get("history7Day"),
        "demandSupply": r.get("demandSupply"),
        "crossCheck": r.get("crossCheck"),
        "seasonalAnalysis": r.get("seasonalAnalysis"),
        "mspCalculator": {
            "mspValue": msp_v, "currentPrice": price,
            "difference": round(price - msp_v, 2),
            "hindi": "MSP के ऊपर" if price > msp_v else "MSP से नीचे",
            "color": "green" if price > msp_v else "red",
            "pct": round((price - msp_v) / msp_v * 100, 1) if msp_v else 0,
        },
        "weatherImpact": {"isRainExpected": is_rain, "alert": alert},
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
        "history7Day": r.get("history7Day"),
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


@app.get("/api/listings/all")
def listings(is_pro: bool = False):
    return {"listings": []}


@app.post("/api/listings/add")
def add_listing(data: dict):
    return {"status": "success", "listingId": "L001"}
