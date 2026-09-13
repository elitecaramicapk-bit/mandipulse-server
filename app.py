# ============================================================
# MANDI BHAV + WEATHER PREDICTION SYSTEM
# FastAPI Server — app.py (NULL SAFETY FIX v2.20.0)
# ============================================================

from fastapi import FastAPI, Query, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
import requests
from typing import Optional
from datetime import datetime
import random
import feedparser

app = FastAPI(
    title="Mandi Pulse API",
    description="Full suite of Mandi Bhav features with default values",
    version="2.20.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# DATA & KEYS
# ============================================================
WEATHER_API_KEY = "caec7b9eba2a2b70be4c1783b8803882"
DATA_GOV_API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"

HINDI_COMMODITY = {
    "Wheat":"गेहूँ","Rice":"चावल","Onion":"प्याज",
    "Potato":"आलू","Tomato":"टमाटर","Garlic":"लहसुन",
    "Mustard":"सरसों","Soybean":"सोयाबीन","Cotton":"कपास",
    "Maize":"मक्का","Chana":"चना","Moong":"मूँग",
    "Urad":"उड़द","Jeera":"जीरा","Coriander":"धनिया",
    "Fennel":"सौंफ","Methi":"मेथी","Groundnut":"मूँगफली",
    "Barley":"जौ","Jowar":"ज्वार","Bajra":"बाजरा",
    "Sugarcane":"गन्ना","Guar":"ग्वार","Castor":"अरंडी"
}

MSP_2025_26 = {
    "Wheat": 2425, "Rice": 2300, "Maize": 2090,
    "Jowar": 3371, "Bajra": 2625, "Barley": 1735,
    "Chana": 5440, "Moong": 8682, "Urad": 7400,
    "Mustard": 5950, "Soybean": 4892, "Cotton": 7121
}

# ============================================================
# HELPERS
# ============================================================
def safe_float(val, default=0.0):
    try: return float(val) if val and val != "None" else default
    except: return default

def get_weather_risk(city: str):
    try:
        url = f"https://wttr.in/{city}?format=j1"
        res = requests.get(url, timeout=10).json()
        current = res.get('current_condition', [{}])[0]
        temp = current.get('temp_C', '30')
        desc = current.get('weatherDesc', [{}])[0].get('value', '').lower()
        is_rain = "rain" in desc or "drizzle" in desc
        return is_rain, "तेजी" if is_rain else "सामान्य", f"🌧️ बारिश" if is_rain else f"☀️ साफ ({temp}°C)"
    except: return False, "सामान्य", "☀️ मौसम साफ"

def get_mandi_data(commodity: str, state: str, city: str):
    try:
        url = f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?api-key={DATA_GOV_API_KEY}&format=json&filters[commodity]={commodity}&filters[state]={state}&filters[market]={city}&limit=5"
        res = requests.get(url, timeout=10).json()
        records = res.get('records', [])
        if records:
            r = records[0]
            prev = records[1] if len(records) > 1 else r
            return {
                "avg": float(r.get('modal_price', 5000)),
                "max": float(r.get('max_price', 5200)),
                "min": float(r.get('min_price', 4800)),
                "prev_avg": float(prev.get('modal_price', 5000)),
                "arrival": int(safe_float(r.get('arrivals_in_qtl', 100))),
                "date": r.get('arrival_date', 'N/A'),
                "prev_date": prev.get('arrival_date', 'N/A')
            }
        return None
    except: return None

# ============================================================
# CORE ENDPOINTS
# ============================================================

@app.get("/api/mandi-predictions")
def get_predictions(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    data = get_mandi_data(commodity, state, city)
    is_rain, impact, alert = get_weather_risk(city)

    if not data:
        return {
            "locationName": city,
            "commodityName": commodity,
            "currentPrice": 0.0,
            "maxPrice": 0.0,
            "minPrice": 0.0,
            "yesterdayPrice": 0.0,
            "priceChange": 0.0,
            "arrivalQuantity": 0,
            "updateDate": "N/A",
            "yesterdayDate": "N/A",
            "isRainExpected": is_rain,
            "weatherAlert": alert,
            "marketImpact": impact,
            "cropImpactIndex": "डेटा नहीं मिला",
            "predictedMinPrice": 0.0,
            "predictedMaxPrice": 0.0,
            "predictionNote": "बाजार में अभी डेटा उपलब्ध नहीं है।"
        }

    return {
        "locationName": city, "commodityName": commodity, "currentPrice": data['avg'],
        "maxPrice": data['max'], "minPrice": data['min'], "yesterdayPrice": data['prev_avg'],
        "priceChange": data['avg'] - data['prev_avg'], "arrivalQuantity": data['arrival'],
        "updateDate": data['date'], "yesterdayDate": data['prev_date'],
        "isRainExpected": is_rain, "weatherAlert": alert, "marketImpact": impact,
        "cropImpactIndex": "स्थिर", "predictedMinPrice": data['avg']*0.95, "predictedMaxPrice": data['avg']*1.10,
        "predictionNote": "बाजार भाव आधारित अनुमान"
    }

@app.get("/api/mandipulse/dashboard")
def get_dashboard(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    data = get_mandi_data(commodity, state, city)
    is_rain, impact, alert = get_weather_risk(city)

    empty_bechain = {"signal": "NONE", "signalHindi": "डेटा नहीं ⚪", "score": 0, "advice": "सरकारी डेटा अपडेट नहीं हुआ है।"}
    empty_calendar = {"bestMonth": "N/A", "bestPrice": 0.0, "worstMonth": "N/A", "sellAdvice": "Hold"}
    empty_heatmap = {"hottestMandi": city, "top3": []}

    if not data:
        return {
            "appName": "MandiPulse 💓", "tagline": "Live accurate data", "commodity": commodity, "location": city,
            "currentPrice": 0.0, "maxPrice": 0.0, "minPrice": 0.0, "yesterdayPrice": 0.0, "arrivalQty": 0,
            "updateDate": "N/A", "yesterdayDate": "N/A", "bechainIndex": empty_bechain,
            "fasalCalendar": empty_calendar, "mandiHeatMap": empty_heatmap, "weather": {"isRain": is_rain, "alert": alert, "impact": impact}
        }

    return {
        "appName": "MandiPulse 💓", "commodity": commodity, "location": city,
        "currentPrice": data['avg'], "maxPrice": data['max'], "minPrice": data['min'],
        "yesterdayPrice": data['prev_avg'], "arrivalQty": data['arrival'],
        "updateDate": data['date'], "yesterdayDate": data['prev_date'],
        "bechainIndex": {"signal": "WAIT", "signalHindi": "रुको 🟡", "score": 70, "advice": "बाजार स्थिर है"},
        "fasalCalendar": {"bestMonth": "मई", "bestPrice": data['avg']*1.15, "worstMonth": "जनवरी", "sellAdvice": "Hold"},
        "mandiHeatMap": {"hottestMandi": city, "top3": []},
        "weather": {"isRain": is_rain, "alert": alert, "impact": impact}
    }

@app.get("/api/mandipulse/calculate")
def calculate_mandipulse(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    data = get_mandi_data(commodity, state, city)
    is_rain, impact, alert = get_weather_risk(city)
    avg = data['avg'] if data else 5000.0
    return {
        "commodityHindi": HINDI_COMMODITY.get(commodity, commodity), "location": city, "currentPrice": avg,
        "masterScore": 75, "masterSignal": "HOLD", "masterHindi": "मजबूत भाव", "masterColor": "green", "masterAdvice": "रोको — लाभ होगा",
        "demandSupply": {"supplyLevel": "सामान्य", "supplyHindi": "आवक स्थिर", "supplyScore": 50, "demandScore": 70, "netHindi": "माँग > आपूर्ति", "netColor": "green", "chartValue": 60},
        "pricePrediction": {"months": [{"monthName": "अक्टूबर", "predictedAvg": int(avg*1.05), "direction": "↑", "dirColor": "green", "predictedMin": int(avg*0.95), "predictedMax": int(avg*1.15), "reason": "मौसमी", "changeFromNow": "5%"}]},
        "bechainIndex": {"score": 65, "signalHindi": "रुको 🟡", "reason": "तेजी संभव", "signalColor": "yellow", "advice": "इंतजार करें"},
        "mspCalculator": {"mspValue": MSP_2025_26.get(commodity, 0), "currentPrice": avg, "hindi": "MSP के करीब", "advice": "रुको", "color": "yellow", "action": "इंतजार"},
        "weatherImpact": {"isRain": is_rain, "impactHindi": alert, "advice": "सावधानी रखें", "impactScore": 50, "impactColor": "yellow", "priceImpact": "5%", "timeline": "7 दिन", "alert": alert, "isRain": is_rain, "sensitivityHindi": "मध्यम"}
    }

# ============================================================
# LISTINGS & MARKETS
# ============================================================

@app.get("/api/markets")
def get_markets(commodity: str, state: str):
    return {"markets": ["Nagaur", "Jodhpur", "Jaipur", "Merta City", "Unjha"]}

@app.get("/api/listings/all")
def get_all_listings(is_pro: bool = False):
    return {"listings": []}

@app.post("/api/listings/add")
def add_listing(data: dict):
    return {"status": "success", "listingId": "L001"}

@app.get("/")
def root(): return {"status": "MandiPulse v2.20.0 Online"}
