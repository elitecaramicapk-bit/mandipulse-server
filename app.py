# ============================================================
# MANDI BHAV + WEATHER PREDICTION SYSTEM
# FastAPI Server — app.py (STATE-LEVEL FALLBACK VERSION 2.8.0)
# ============================================================

from fastapi import FastAPI, Query, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
import requests
from typing import Optional
from datetime import datetime
import random

app = FastAPI(
    title="Mandi Bhav Prediction API",
    description="Intelligent fallback: City -> State -> Error",
    version="2.8.0"
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

# ============================================================
# HELPERS
# ============================================================
def safe_float(val, default=0.0):
    try: return float(val) if val and val != "None" else default
    except: return default

def get_mandi_data(commodity: str, state: str, city: str):
    commodity_map = {
        "Wheat": "Wheat", "Moong": "Moong(Whole)", "Gram": "Gram Raw(Chana)", "Chana": "Gram Raw(Chana)",
        "Mustard": "Mustard", "Soybean": "Soyabean", "Onion": "Onion", "Garlic": "Garlic",
        "Potato": "Potato", "Tomato": "Tomato", "Cotton": "Cotton", "Bajra": "Bajra(Pearl Millet/Cumbu)",
        "Cumin": "Cummin,Cumin(Jeera),Peepal", "Jeera": "Cummin,Cumin(Jeera),Peepal"
    }
    mapped_commodity = commodity_map.get(commodity, commodity)

    source = "मंडी"

    try:
        # STEP 1: Search specific City/Market
        url = (
            f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
            f"?api-key={DATA_GOV_API_KEY}&format=json"
            f"&filters[commodity]={mapped_commodity}&filters[state]={state}&filters[market]={city}"
            f"&limit=10"
        )
        response = requests.get(url, timeout=12)
        records = response.json().get('records', [])

        # STEP 2: Fallback to State if no City data
        if not records:
            source = f"{state} (औसत)"
            url_fb = (
                f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
                f"?api-key={DATA_GOV_API_KEY}&format=json"
                f"&filters[commodity]={mapped_commodity}&filters[state]={state}"
                f"&limit=50"
            )
            records = requests.get(url_fb, timeout=12).json().get('records', [])

        if not records: return None, None, None, "", ""

        # Processing records
        for r in records:
            try: r['dt_obj'] = datetime.strptime(r.get('arrival_date', '01/01/2000'), '%d/%m/%Y')
            except: r['dt_obj'] = datetime(2000, 1, 1)

        sorted_recs = sorted(records, key=lambda x: x['dt_obj'], reverse=True)
        unique_days = []
        for r in sorted_recs:
            price = safe_float(r.get('modal_price'))
            if price > 0:
                dt = r['dt_obj'].date()
                if not unique_days or dt != unique_days[-1]['date']:
                    unique_days.append({
                        'date': dt,
                        'price': price,
                        'arrival': int(safe_float(r.get('arrivals_in_qtl'))),
                        'date_str': r.get('arrival_date')
                    })
            if len(unique_days) >= 2: break

        if unique_days:
            curr = unique_days[0]['price']
            prev = unique_days[1]['price'] if len(unique_days) >= 2 else curr
            return curr, prev, unique_days[0]['arrival'], unique_days[0]['date_str'], source

        return None, None, None, "", ""
    except:
        return None, None, None, "", ""

def get_weather_risk(city: str):
    try:
        geo_url = f"http://api.openweathermap.org/geo/1.0/direct?q={city},IN&limit=1&appid={WEATHER_API_KEY}"
        geo_res = requests.get(geo_url, timeout=5).json()
        if not geo_res: return False, "सामान्य", "लोकेशन नहीं मिली"
        lat, lon = geo_res[0]['lat'], geo_res[0]['lon']
        weather_url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}&units=metric"
        weather_res = requests.get(weather_url, timeout=5).json()
        rain = any('Rain' in w.get('main', '') for item in weather_res.get('list', []) for w in item.get('weather', []))
        return (True, "तेजी", "⚠️ बारिश संभव") if rain else (False, "सामान्य", "☀️ मौसम साफ")
    except:
        return False, "सामान्य", "मौसम डेटा नहीं"

# ============================================================
# API ENDPOINTS
# ============================================================
@app.get("/api/mandipulse/dashboard")
def get_dashboard(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    cur, prev, arr, date, source = get_mandi_data(commodity, state, city)

    if cur is None:
        raise HTTPException(status_code=404, detail=f"क्षमा करें, {commodity} का डेटा अभी उपलब्ध नहीं है।")

    is_rain, impact, alert = get_weather_risk(city)

    # Smart signal
    signal = "WAIT" if cur > prev else "BUY" if cur < prev else "HOLD"
    sig_hi = "रुको 🟡" if signal == "WAIT" else "खरीदो 🟢" if signal == "BUY" else "स्थिर ⚪"

    return {
        "appName": "MandiPulse 💓",
        "commodity": commodity,
        "location": f"{city} ({source})",
        "currentPrice": cur,
        "arrivalQty": arr,
        "bechainIndex": {
            "signal": signal,
            "signalHindi": sig_hi,
            "score": 75 if cur > prev else 45,
            "advice": f"भाव {source} के अनुसार ₹{cur} है।"
        },
        "fasalCalendar": {"bestMonth": "मई", "bestPrice": cur*1.15, "worstMonth": "दिसंबर", "sellAdvice": "सही समय पर बेचें"},
        "mandiHeatMap": {"hottestMandi": city, "top3": []},
        "weather": {"isRain": is_rain, "alert": alert, "impact": impact}
    }

@app.get("/")
def root(): return {"status": "✅ MandiPulse Server Active", "version": "2.8.0"}

@app.get("/api/mandi-predictions")
def get_predictions(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    cur, prev, arr, date, source = get_mandi_data(commodity, state, city)
    if cur is None: raise HTTPException(status_code=404, detail="डेटा नहीं मिला")
    is_rain, impact, alert = get_weather_risk(city)
    return {
        "locationName": f"{city} ({source})", "commodityName": commodity,
        "currentPrice": cur, "yesterdayPrice": prev, "priceChange": cur - prev,
        "arrivalQuantity": arr, "updateDate": date, "isRainExpected": is_rain,
        "weatherAlert": alert, "marketImpact": impact, "cropImpactIndex": "स्थिर",
        "predictedMinPrice": round(cur * 0.95, 0), "predictedMaxPrice": round(cur * 1.15, 0),
        "predictionNote": f"{source} डेटा आधारित"
    }
