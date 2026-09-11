# ============================================================
# MANDI BHAV + WEATHER PREDICTION SYSTEM
# FastAPI Server — app.py (MARKET OUTLOOK VERSION 2.17.0)
# ============================================================

from fastapi import FastAPI, Query, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import requests
from typing import Optional
from datetime import datetime
import random

app = FastAPI(
    title="Mandi Pulse API",
    description="Dynamic Markets + Demand-Supply Forecast",
    version="2.17.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# DATA SOURCES
# ============================================================
DATA_GOV_API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"

COMMODITY_MAP = {
    "Wheat": "Wheat", "Moong": "Green Gram(Moong)(Whole)", "Gram": "Bengal Gram(Gram)(Whole)",
    "Chana": "Bengal Gram(Gram)(Whole)", "Mustard": "Mustard", "Soybean": "Soyabean",
    "Onion": "Onion", "Garlic": "Garlic", "Potato": "Potato", "Tomato": "Tomato",
    "Cotton": "Cotton", "Bajra": "Bajra(Pearl Millet/Cumbu)", "Jowar": "Jowar(Sorghum)",
    "Jeera": "Cummin Seed(Jeera)", "Cumin": "Cummin Seed(Jeera)", "Urad": "Black Gram (Urad)(Whole)",
    "Masoor": "Lentil (Masur)(Whole)", "Arhar": "Arhar (Tur/Red Gram)", "Rice": "Rice"
}

# ============================================================
# HELPERS
# ============================================================
def safe_float(val, default=0.0):
    try: return float(val) if val and val != "None" else default
    except: return default

def get_market_outlook(curr_p, prev_p, curr_arr, prev_arr):
    """Calculates outlook based on Demand-Supply rules"""
    if curr_p > prev_p and curr_arr < prev_arr:
        return "तेजी 📈 (मांग ज्यादा, आवक कम)", "बाजार में माल कम है, भाव और बढ़ सकते हैं।"
    elif curr_p < prev_p and curr_arr > prev_arr:
        return "मंदी 📉 (आवक ज्यादा, मांग कम)", "माल की आवक ज्यादा होने से भाव गिर सकते हैं।"
    elif curr_p > prev_p and curr_arr > prev_arr:
        return "मजबूत पकड़ 💹", "ज्यादा आवक के बावजूद मांग अच्छी है, भाव स्थिर रहेंगे।"
    else:
        return "स्थिर ⚖️", "बाजार अभी सामान्य स्थिति में है।"

def get_mandi_data(commodity: str, state: str, city: str):
    mapped_commodity = COMMODITY_MAP.get(commodity, commodity)

    try:
        url = f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?api-key={DATA_GOV_API_KEY}&format=json&limit=100"
        url += f"&filters[commodity]={mapped_commodity}&filters[state]={state}"

        res = requests.get(url, timeout=10).json()
        records = res.get('records', [])

        if records:
            market_recs = [r for r in records if city.lower() in r.get('market', '').lower()]
            active_recs = market_recs if market_recs else records

            for r in active_recs:
                try: r['dt'] = datetime.strptime(r.get('arrival_date', '01/01/2000'), '%d/%m/%Y')
                except: r['dt'] = datetime(2000, 1, 1)

            sorted_recs = sorted(active_recs, key=lambda x: x['dt'], reverse=True)

            days = []
            for r in sorted_recs:
                p_avg = safe_float(r.get('modal_price'))
                if p_avg > 0:
                    d = r['dt'].date()
                    if not days or d != days[-1]['date']:
                        days.append({
                            'avg': p_avg, 'min': safe_float(r.get('min_price')), 'max': safe_float(r.get('max_price')),
                            'date': d, 'arrival': int(safe_float(r.get('arrivals_in_qtl'))), 'date_str': r.get('arrival_date')
                        })
                if len(days) >= 2: break

            if days:
                source = "मंडी" if market_recs else f"{state} औसत"
                curr = days[0]
                prev = days[1] if len(days) >= 2 else curr

                outlook_title, outlook_desc = get_market_outlook(curr['avg'], prev['avg'], curr['arrival'], prev['arrival'])

                return {
                    "avg": curr['avg'], "max": curr['max'], "min": curr['min'],
                    "prev_avg": prev['avg'], "arrival": curr['arrival'], "prev_arrival": prev['arrival'],
                    "date": curr['date_str'], "prev_date": prev['date_str'], "source": source,
                    "outlook_title": outlook_title, "outlook_desc": outlook_desc
                }
    except: pass
    return None

def get_weather_risk(city: str):
    try:
        url = f"https://wttr.in/{city}?format=j1"
        res = requests.get(url, timeout=5).json()
        current = res.get('current_condition', [{}])[0]
        temp = current.get('temp_C', '30')
        desc = current.get('weatherDesc', [{}])[0].get('value', '').lower()
        is_rain = "rain" in desc or "drizzle" in desc
        status = "🌧️ बारिश" if is_rain else f"☀️ साफ ({temp}°C)"
        return is_rain, "तेजी" if is_rain else "सामान्य", status
    except: return False, "सामान्य", "☀️ मौसम साफ"

# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/api/markets")
def get_active_markets(commodity: str, state: str):
    """Returns list of markets that have data for this commodity"""
    mapped_commodity = COMMODITY_MAP.get(commodity, commodity)
    try:
        url = f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?api-key={DATA_GOV_API_KEY}&format=json&limit=200"
        url += f"&filters[commodity]={mapped_commodity}&filters[state]={state}"
        res = requests.get(url, timeout=10).json()
        markets = sorted(list(set(r.get('market') for r in res.get('records', []))))
        return {"markets": markets if markets else ["Nagaur", "Jodhpur", "Jaipur"]}
    except:
        return {"markets": ["Nagaur", "Jodhpur", "Jaipur"]}

@app.get("/api/mandi-predictions")
def get_predictions(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    data = get_mandi_data(commodity, state, city)
    is_rain, impact, alert = get_weather_risk(city)

    if not data:
        return {"locationName": city, "commodityName": commodity, "currentPrice": 0.0, "updateDate": "N/A", "weatherAlert": alert}

    return {
        "locationName": f"{city} ({data['source']})", "commodityName": commodity,
        "currentPrice": data['avg'], "maxPrice": data['max'], "minPrice": data['min'],
        "yesterdayPrice": data['prev_avg'], "priceChange": data['avg'] - data['prev_avg'],
        "arrivalQuantity": data['arrival'], "updateDate": data['date'], "yesterdayDate": data['prev_date'],
        "isRainExpected": is_rain, "weatherAlert": alert, "marketImpact": impact,
        "cropImpactIndex": data['outlook_title'],
        "predictionNote": data['outlook_desc'],
        "predictedMinPrice": round(data['avg'] * 0.95, 0), "predictedMaxPrice": round(data['avg'] * 1.15, 0)
    }

@app.get("/api/mandipulse/dashboard")
def get_dashboard(commodity: str = Query(...), state: str = Query(...), city: str = Query(...)):
    data = get_mandi_data(commodity, state, city)
    is_rain, impact, alert = get_weather_risk(city)

    if not data: return {"appName": "MandiPulse 💓", "commodity": commodity, "currentPrice": 0.0}

    return {
        "appName": "MandiPulse 💓", "commodity": commodity, "location": f"{city} ({data['source']})",
        "currentPrice": data['avg'], "maxPrice": data['max'], "minPrice": data['min'],
        "arrivalQty": data['arrival'], "updateDate": data['date'], "yesterdayDate": data['prev_date'],
        "bechainIndex": {"signal": "WAIT" if data['avg'] >= data['prev_avg'] else "BUY", "signalHindi": data['outlook_title'], "score": 70, "advice": data['outlook_desc']},
        "fasalCalendar": {"bestMonth": "मई", "bestPrice": data['avg']*1.12, "worstMonth": "जनवरी", "sellAdvice": "Hold for better price"},
        "mandiHeatMap": {"hottestMandi": city, "top3": []},
        "weather": {"isRain": is_rain, "alert": alert, "impact": impact}
    }

@app.get("/")
def root(): return {"status": "MandiPulse v2.17 Online"}
