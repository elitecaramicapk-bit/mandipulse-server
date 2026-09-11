# ============================================================
# MANDI BHAV + WEATHER PREDICTION SYSTEM
# FastAPI Server — app.py (DUAL DATE VERSION 2.11.0)
# ============================================================

from fastapi import FastAPI, Query, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import requests
from typing import Optional
from datetime import datetime

app = FastAPI(
    title="Mandi Bhav Prediction API",
    description="Precise matching with exact government commodity names and dual dates",
    version="2.11.0"
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
        "Wheat": "Wheat",
        "Moong": "Green Gram(Moong)(Whole)",
        "Gram": "Bengal Gram(Gram)(Whole)",
        "Chana": "Bengal Gram(Gram)(Whole)",
        "Mustard": "Mustard",
        "Soybean": "Soyabean",
        "Onion": "Onion",
        "Garlic": "Garlic",
        "Potato": "Potato",
        "Tomato": "Tomato",
        "Cotton": "Cotton",
        "Bajra": "Bajra(Pearl Millet/Cumbu)",
        "Jowar": "Jowar(Sorghum)",
        "Jeera": "Cummin Seed(Jeera)",
        "Cumin": "Cummin Seed(Jeera)",
        "Urad": "Black Gram (Urad)(Whole)",
        "Masoor": "Lentil (Masur)(Whole)",
        "Arhar": "Arhar (Tur/Red Gram)",
        "Tur": "Arhar (Tur/Red Gram)",
        "Groundnut": "Groundnut",
        "Chilli": "Chilli Red",
        "Coriander": "Coriander(Seed)",
        "Fennel": "Fennel(Saunf)",
        "Rice": "Rice",
        "Sugarcane": "Sugarcane"
    }

    mapped_commodity = commodity_map.get(commodity, commodity)

    search_queries = [
        {"filters[commodity]": mapped_commodity, "filters[state]": state, "filters[market]": city},
        {"filters[commodity]": mapped_commodity, "filters[state]": state},
        {"filters[state]": state, "filters[market]": city}
    ]

    final_source = "मंडी"

    for i, q in enumerate(search_queries):
        try:
            url = f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?api-key={DATA_GOV_API_KEY}&format=json&limit=200"
            for k, v in q.items():
                url += f"&{k}={v}"

            res = requests.get(url, timeout=12).json()
            records = res.get('records', [])

            if records:
                if i == 2:
                    records = [r for r in records if commodity.lower() in r.get('commodity', '').lower()]
                    if not records: continue

                if i == 1: final_source = f"{state} औसत"

                for r in records:
                    try: r['dt'] = datetime.strptime(r.get('arrival_date', '01/01/2000'), '%d/%m/%Y')
                    except: r['dt'] = datetime(2000, 1, 1)

                sorted_recs = sorted(records, key=lambda x: x['dt'], reverse=True)

                days = []
                for r in sorted_recs:
                    p = safe_float(r.get('modal_price'))
                    if p > 0:
                        d = r['dt'].date()
                        if not days or d != days[-1]['date']:
                            days.append({
                                'price': p,
                                'date': d,
                                'arrival': int(safe_float(r.get('arrivals_in_qtl'))),
                                'date_str': r.get('arrival_date')
                            })
                    if len(days) >= 2: break

                if days:
                    curr_p = days[0]['price']
                    prev_p = days[1]['price'] if len(days) >= 2 else curr_p
                    curr_d = days[0]['date_str']
                    prev_d = days[1]['date_str'] if len(days) >= 2 else curr_d
                    arr = days[0]['arrival']
                    return curr_p, prev_p, arr, curr_d, prev_d, final_source
        except Exception as e:
            print(f"Strategy {i} failed: {e}")
            continue

    return None, None, None, "", "", ""

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
@app.get("/api/mandi-predictions")
def get_predictions(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    cur, prev, arr, date, prev_date, source = get_mandi_data(commodity, state, city)

    if cur is None:
        return {
            "locationName": f"{city}, {state}", "commodityName": commodity,
            "currentPrice": 0.0, "yesterdayPrice": 0.0, "priceChange": 0.0,
            "arrivalQuantity": 0, "updateDate": "N/A", "yesterdayDate": "N/A", "isRainExpected": False,
            "weatherAlert": "डेटा नहीं मिला", "marketImpact": "N/A", "cropImpactIndex": "मंडी बंद या नाम गलत",
            "predictedMinPrice": 0.0, "predictedMaxPrice": 0.0, "predictionNote": f"सरकारी पोर्टल पर {commodity} का रिकॉर्ड नहीं मिला।"
        }

    is_rain, impact, alert = get_weather_risk(city)
    return {
        "locationName": f"{city} ({source})", "commodityName": commodity,
        "currentPrice": cur, "yesterdayPrice": prev, "priceChange": cur - prev,
        "arrivalQuantity": arr, "updateDate": date, "yesterdayDate": prev_date, "isRainExpected": is_rain,
        "weatherAlert": alert, "marketImpact": impact, "cropImpactIndex": "स्थिर",
        "predictedMinPrice": round(cur * 0.95, 0), "predictedMaxPrice": round(cur * 1.15, 0),
        "predictionNote": f"{source} डेटा आधारित"
    }

@app.get("/")
def root(): return {"status": "✅ MandiPulse Server Active", "version": "2.11.0"}

@app.get("/api/mandipulse/dashboard")
def get_dashboard(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    cur, prev, arr, date, prev_date, source = get_mandi_data(commodity, state, city)
    if cur is None:
        return {
            "appName": "MandiPulse 💓", "commodity": commodity, "location": f"{city}, {state}",
            "currentPrice": 0.0, "yesterdayPrice": 0.0, "arrivalQty": 0, "updateDate": "N/A", "yesterdayDate": "N/A",
            "bechainIndex": {"signal": "NONE", "signalHindi": "डेटा नहीं ⚪", "score": 0, "advice": "सरकारी डेटा अपडेट नहीं हुआ है।"},
            "fasalCalendar": {"bestMonth": "N/A", "bestPrice": 0.0, "worstMonth": "N/A", "sellAdvice": "बाद में चेक करें"},
            "mandiHeatMap": {"hottestMandi": city, "top3": []},
            "weather": {"isRain": False, "alert": "N/A", "impact": "N/A"}
        }

    is_rain, impact, alert = get_weather_risk(city)
    return {
        "appName": "MandiPulse 💓", "commodity": commodity, "location": f"{city} ({source})",
        "currentPrice": cur, "yesterdayPrice": prev, "arrivalQty": arr, "updateDate": date, "yesterdayDate": prev_date,
        "bechainIndex": {"signal": "WAIT" if cur >= prev else "BUY", "signalHindi": "रुको 🟡" if cur >= prev else "खरीदो 🟢", "score": 70, "advice": f"अंतिम अपडेट: {date}"},
        "fasalCalendar": {"bestMonth": "मई", "bestPrice": cur*1.12, "worstMonth": "जनवरी", "sellAdvice": "Hold for better price"},
        "mandiHeatMap": {"hottestMandi": city, "top3": []},
        "weather": {"isRain": is_rain, "alert": alert, "impact": impact}
    }
