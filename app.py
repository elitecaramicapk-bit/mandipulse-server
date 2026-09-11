# ============================================================
# MANDI BHAV + WEATHER PREDICTION SYSTEM
# FastAPI Server — app.py (PRO FEATURES ADDED)
# ============================================================

from fastapi import FastAPI, Query, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
import requests
from typing import Optional
from datetime import datetime
import random
import feedparser

app = FastAPI(
    title="Mandi Bhav Prediction API",
    description="Mandi price + weather risk + PRO features",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# API KEYS
# ============================================================
WEATHER_API_KEY = "caec7b9eba2a2b70be4c1783b8803882"
DATA_GOV_API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"

# ── Hindi Translations ──
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

# ── MSP 2025-26 ──
MSP_2025_26 = {
    "Wheat": 2425, "Rice": 2300, "Maize": 2090,
    "Jowar": 3371, "Bajra": 2625, "Barley": 1735,
    "Chana": 5440, "Moong": 8682, "Urad": 7400,
    "Masoor": 6425, "Groundnut": 6783, "Mustard": 5950,
    "Soybean": 4892, "Cotton": 7121, "Castor": 6760,
    "Gwar": 5765, "Til": 8635, "Sunflower": 7280,
    "Sugarcane": 340, "Jowar(Hybrid)": 3421,
}

# ── Seasonal Price Multipliers ──
SEASONAL_FACTORS = {
    "Jeera": {1: +8, 2: +5, 3: 0, 4: -5, 5: -8, 6: -3, 7: +2, 8: +5, 9: +8, 10: +12, 11: +15, 12: +10},
    "Mustard": {1: +5, 2: +3, 3: -5, 4: -10, 5: -8, 6: -3, 7: +2, 8: +5, 9: +8, 10: +5, 11: +8, 12: +10},
    "Moong": {1: +5, 2: +8, 3: +10, 4: +5, 5: 0, 6: -3, 7: -5, 8: -10, 9: -15, 10: -8, 11: -3, 12: +3},
    "Gwar": {1: +5, 2: +8, 3: +5, 4: 0, 5: -5, 6: -8, 7: -5, 8: -10, 9: -8, 10: -5, 11: +3, 12: +8},
    "Wheat": {1: +5, 2: +3, 3: -3, 4: -8, 5: -10, 6: -5, 7: +3, 8: +5, 9: +8, 10: +8, 11: +10, 12: +8},
}

# ── Crop Weather Sensitivity ──
WEATHER_SENSITIVITY = {
    "HIGH": ["Onion", "Tomato", "Potato", "Methi", "Coriander"],
    "MEDIUM": ["Jeera", "Mustard", "Moong", "Bajra", "Jowar", "Gwar", "Til", "Soanf", "Isabgol", "Taramira"],
    "LOW": ["Wheat", "Rice", "Chana", "Soybean", "Cotton", "Groundnut", "Masoor", "Urad"],
}

# ── Export Peak Months ──
EXPORT_PEAK = {
    "Jeera": [3, 4, 5, 6], "Soanf": [3, 4, 5], "Methi": [2, 3, 4], "Isabgol": [3, 4, 5],
}

MONTH_NAMES_HI = {1:"जनवरी", 2:"फरवरी", 3:"मार्च", 4:"अप्रैल", 5:"मई", 6:"जून", 7:"जुलाई", 8:"अगस्त", 9:"सितंबर", 10:"अक्टूबर", 11:"नवंबर", 12:"दिसंबर"}

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
        desc = current.get('weatherDesc', [{}])[0].get('value', '').lower()
        rain_expected = any("rain" in h.get('chanceofrain', "0") for d in res.get('weather', []) for h in d.get('hourly', []))
        if "rain" in desc or rain_expected:
            return True, "तेजी की संभावना", "⚠️ बारिश की संभावना"
        return False, "सामान्य", "☀️ मौसम साफ"
    except: return False, "सामान्य", "मौसम डेटा नहीं"

def get_mandi_data(commodity: str, state: str, city: str):
    try:
        url = f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?api-key={DATA_GOV_API_KEY}&format=json&filters[commodity]={commodity}&filters[state]={state}&filters[market]={city}&limit=1"
        res = requests.get(url, timeout=10).json()
        records = res.get('records', [])
        if records:
            r = records[0]
            return float(r.get('modal_price', 5000)), int(safe_float(r.get('arrivals_in_qtl', 100)))
        return 5000.0, 100
    except: return 5000.0, 100

def get_weather_sensitivity(commodity: str) -> str:
    for level, crops in WEATHER_SENSITIVITY.items():
        if commodity in crops: return level
    return "MEDIUM"

def is_export_peak(commodity: str, month: int) -> bool:
    return month in EXPORT_PEAK.get(commodity, [])

def get_seasonal_factor(commodity: str, month: int) -> int:
    return SEASONAL_FACTORS.get(commodity, {}).get(month, 0)

# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/api/mandipulse/calculate")
def calculate_mandipulse(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    now = datetime.now()
    current_month = now.month
    commodity_hindi = HINDI_COMMODITY.get(commodity, commodity)

    base_price, arrival = get_mandi_data(commodity, state, city)
    is_rain, market_impact, alert_text = get_weather_risk(city)
    msp = MSP_2025_26.get(commodity, 0)

    # Supply-Demand
    supply_score = 80 if arrival > 500 else (50 if arrival > 200 else 20)
    demand_score = 50 + (25 if is_export_peak(commodity, current_month) else 0) + (30 if msp > 0 and base_price < msp else 0)
    demand_score = max(0, min(100, demand_score))
    net_sd = demand_score - supply_score

    demand_supply = {
        "supplyLevel": "अधिक" if arrival > 500 else "सामान्य",
        "supplyHindi": f"आवक: {arrival} क्विंटल",
        "supplyScore": supply_score,
        "demandScore": demand_score,
        "netHindi": "माँग > आपूर्ति" if net_sd > 20 else "संतुलित",
        "netColor": "green" if net_sd > 20 else "yellow",
        "chartValue": min(100, 50 + net_sd)
    }

    # 3-Month Prediction
    predictions = []
    for i in range(1, 4):
        pm = ((current_month - 1 + i) % 12) + 1
        adj = get_seasonal_factor(commodity, pm) + (10 if i == 1 and is_rain else 0)
        p_avg = base_price * (1 + adj / 100)
        predictions.append({
            "monthName": MONTH_NAMES_HI[pm],
            "predictedMin": int(p_avg * 0.92),
            "predictedMax": int(p_avg * 1.08),
            "predictedAvg": int(p_avg),
            "direction": "↑ तेजी" if adj > 3 else ("↓ मंदी" if adj < -3 else "→ स्थिर"),
            "dirColor": "green" if adj > 3 else ("red" if adj < -3 else "gray"),
            "reason": "मौसमी मांग" if adj > 0 else "सामान्य",
            "changeFromNow": f"{adj}%"
        })

    # Bechain Index
    b_score = 50 + (20 if is_rain else 0) + (20 if arrival < 100 else -20 if arrival > 500 else 0)
    b_score = max(0, min(100, b_score))
    bechain = {
        "score": b_score,
        "signalHindi": "रुको 🟡" if b_score >= 65 else ("बेचो 🟢" if b_score >= 40 else "जल्दी बेचो 🔴"),
        "signalColor": "yellow" if b_score >= 65 else ("green" if b_score >= 40 else "red"),
        "reason": "भाव बढ़ने की उम्मीद" if b_score >= 65 else "सही समय",
        "advice": "अगले महीने तक रुकें" if b_score >= 65 else "अभी बेचें"
    }

    # MSP
    msp_calc = {"mspValue": msp, "currentPrice": base_price, "hindi": f"MSP: ₹{msp}" if msp > 0 else "MSP नहीं", "color": "green" if base_price > msp else "red", "advice": "सरकारी केंद्र पर बेचें" if base_price < msp else "बाजार में बेचें", "action": "FCI केंद्र" if base_price < msp else "प्राइवेट ट्रेडर"}

    # Weather
    weather_impact = {"isRain": is_rain, "sensitivityHindi": "मध्यम", "impactScore": 60 if is_rain else 20, "impactHindi": "बारिश से तेजी" if is_rain else "मौसम साफ", "priceImpact": "10%" if is_rain else "0%", "impactColor": "yellow" if is_rain else "green", "advice": "माल ढक कर रखें" if is_rain else "कोई चिंता नहीं", "timeline": "7 दिन", "alert": alert_text}

    return {
        "commodityHindi": commodity_hindi, "location": f"{city}, {state}", "currentPrice": base_price,
        "masterScore": int((b_score + (100 if base_price > msp else 50)) / 2),
        "masterHindi": "मजबूत भाव", "masterColor": "green", "masterAdvice": "रोको — लाभ होगा",
        "demandSupply": demand_supply,
        "pricePrediction": {"months": predictions},
        "bechainIndex": bechain,
        "mspCalculator": msp_calc,
        "weatherImpact": weather_impact
    }

@app.get("/")
def root(): return {"status": "MandiPulse v2.18 Online"}
