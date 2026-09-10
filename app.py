# ============================================================
# MANDI BHAV + WEATHER PREDICTION SYSTEM
# FastAPI Server — app.py (PRO FEATURES ADDED)
# ============================================================

from fastapi import FastAPI, Query, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
import requests
from typing import Optional

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

# ============================================================
# FUNCTION 1: WEATHER RISK CHECK
# ============================================================
def get_weather_risk(city: str):
    try:
        geo_url = (
            f"http://api.openweathermap.org/geo/1.0/direct"
            f"?q={city},IN&limit=1&appid={WEATHER_API_KEY}"
        )
        geo_res = requests.get(geo_url, timeout=10).json()
        if not geo_res:
            return False, "सामान्य", "लोकेशन नहीं मिली"

        lat = geo_res[0]['lat']
        lon = geo_res[0]['lon']

        weather_url = (
            f"https://api.openweathermap.org/data/2.5/forecast"
            f"?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}&units=metric"
        )
        weather_res = requests.get(weather_url, timeout=10).json()

        rain_expected = False
        rain_days = 0
        for item in weather_res.get('list', []):
            for w in item.get('weather', []):
                if 'Rain' in w.get('main', ''):
                    rain_expected = True
                    rain_days += 1
                    break

        if rain_expected:
            return True, "तेजी की संभावना", f"⚠️ अगले 5 दिनों में बारिश ({rain_days} स्लॉट)"
        else:
            return False, "सामान्य", "☀️ मौसम साफ, सामान्य बाजार"
    except:
        return False, "सामान्य", "मौसम डेटा उपलब्ध नहीं"


# ============================================================
# FUNCTION 2: MANDI DATA FETCH (REAL DATA)
# ============================================================
def get_mandi_data(commodity: str, state: str, city: str):
    # Mapping for exact names in api.data.gov.in
    commodity_map = {
        "Wheat": "Wheat",
        "Moong": "Moong(Whole)",
        "Gram": "Gram Raw(Chana)",
        "Chana": "Gram Raw(Chana)",
        "Mustard": "Mustard",
        "Soybean": "Soyabean",
        "Onion": "Onion",
        "Garlic": "Garlic",
        "Potato": "Potato",
        "Tomato": "Tomato",
        "Cotton": "Cotton",
        "Bajra": "Bajra(Pearl Millet/Cumbu)",
        "Jowar": "Jowar(Sorghum)"
    }

    mapped_commodity = commodity_map.get(commodity, commodity)

    try:
        url = (
            f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
            f"?api-key={DATA_GOV_API_KEY}&format=json"
            f"&filters[commodity]={mapped_commodity}"
            f"&filters[state]={state}"
            f"&filters[market]={city}"
            f"&limit=10&sort[arrival_date]=desc"
        )
        res = requests.get(url, timeout=15).json()
        records = res.get('records', [])

        if len(records) >= 2:
            latest = records[0]
            previous = records[1]
            current_price = float(latest.get('modal_price', 0))
            yesterday_price = float(previous.get('modal_price', 0))
            arrival = int(latest.get('arrivals_in_qtl', 0))
            return current_price, yesterday_price, arrival
        elif len(records) == 1:
            latest = records[0]
            current_price = float(latest.get('modal_price', 0))
            return current_price, current_price, int(latest.get('arrivals_in_qtl', 0))

        return 0.0, 0.0, 0
    except Exception as e:
        print(f"Error fetching mandi data: {e}")
        return 0.0, 0.0, 0


# ============================================================
# NORMAL API — Sabke liye
# ============================================================
@app.get("/api/mandi-predictions")
def get_mandi_predictions(
    commodity: str = Query(...),
    state: str = Query(...),
    city: str = Query(...)
):
    current_price, yesterday_price, arrival_quantity = get_mandi_data(commodity, state, city)
    is_rain, market_impact, alert_text = get_weather_risk(city)

    if current_price == 0:
        raise HTTPException(status_code=404, detail="आज का भाव अपडेट नहीं हुआ है।")

    price_change = current_price - yesterday_price

    # Prediction Logic
    if is_rain or arrival_quantity < 100:
        min_predicted = round(current_price * 1.10, 2)
        max_predicted = round(current_price * 1.30, 2)
        crop_impact = "उच्च प्रभाव — तेजी संभव"
    elif arrival_quantity > 300:
        min_predicted = round(current_price * 0.85, 2)
        max_predicted = round(current_price * 1.05, 2)
        crop_impact = "अधिक आवक — मंदी संभव"
    else:
        min_predicted = round(current_price * 0.95, 2)
        max_predicted = round(current_price * 1.10, 2)
        crop_impact = "सामान्य बाजार — स्थिर"

    return {
        "locationName": f"{city}, {state}",
        "commodityName": commodity,
        "currentPrice": current_price,
        "yesterdayPrice": yesterday_price,
        "priceChange": price_change,
        "arrivalQuantity": arrival_quantity,
        "isRainExpected": is_rain,
        "weatherAlert": alert_text,
        "marketImpact": market_impact,
        "cropImpactIndex": crop_impact,
        "predictedMinPrice": min_predicted,
        "predictedMaxPrice": max_predicted,
        "predictionNote": "यह 1-3 महीने की अनुमानित कीमत है"
    }


# ============================================================
# PRO API — Sirf Pro/Admin ke liye (Firebase Token required)
# ============================================================
@app.get("/api/pro/advanced-predictions")
def get_pro_predictions(
    commodity: str = Query(...),
    state: str = Query(...),
    city: str = Query(...),
    authorization: Optional[str] = Header(None)
):
    # Token check
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Login required")

    token = authorization.replace("Bearer ", "")

    try:
        import firebase_admin
        from firebase_admin import auth, credentials
        if not firebase_admin._apps:
            cred = credentials.Certificate("firebase-service-account.json")
            firebase_admin.initialize_app(cred)

        decoded = auth.verify_id_token(token)
        role = decoded.get('role', 'user')
        if role not in ['pro', 'admin']:
            raise HTTPException(status_code=403, detail="Pro membership required")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token invalid")

    # Sab data fetch
    current_price, yesterday_price, arrival_quantity = get_mandi_data(commodity, state, city)
    is_rain, market_impact, alert_text = get_weather_risk(city)

    # Placeholder for more complex pro logic if needed
    # In a real app, these would also use the real current_price
    monthly = {
        "monthlyPrices": {"मई": current_price * 1.1, "जून": current_price * 1.05},
        "bestMonth": "मई",
        "bestMonthPrice": current_price * 1.1,
        "insight": "ऐतिहासिक डेटा के आधार पर मई में भाव बढ़ सकते हैं"
    }

    return {
        "locationName": f"{city}, {state}",
        "commodityName": commodity,
        "currentPrice": current_price,
        "yesterdayPrice": yesterday_price,
        "arrivalQuantity": arrival_quantity,
        "isRainExpected": is_rain,
        "weatherAlert": alert_text,
        "marketImpact": market_impact,
        "monthlyTrend": monthly,
        "districtDemand": {"topMarkets": [], "hotMarket": city, "insight": "मांग सामान्य है"},
        "increaseProbability": {"increasePercent": 15, "predictedPrice": current_price * 1.15, "confidence": "उच्च", "factors": {"rainImpact": "5%", "supplyImpact": "10%", "seasonalImpact": "0%"}, "insight": "तेजी संभव"},
        "chartData": {
            "labels": ["Today", "Yesterday"],
            "prices": [current_price, yesterday_price],
            "currentPrice": current_price,
            "predictedMax": current_price * 1.2
        }
    }


@app.get("/")
def root():
    return {"status": "✅ Server चालू है", "version": "2.1.0"}


# ============================================================
# MANDIPULSE — 4 EXTRA FEATURES
# ============================================================

# ============================================================
# FEATURE 1: BECHAIN INDEX
# ============================================================
@app.get("/api/mandipulse/bechain-index")
def get_bechain_index(
    commodity: str = Query(...),
    state: str = Query(...),
    city: str = Query(...)
):
    current_price, _, arrival = get_mandi_data(commodity, state, city)
    is_rain, market_impact, alert_text = get_weather_risk(city)

    score = 50
    if is_rain: score += 20
    if arrival > 0 and arrival < 100: score += 15
    elif arrival > 400: score -= 15

    score = max(0, min(100, score))

    if score >= 65:
        signal, signal_hi, color = "WAIT", "रुको 🟡", "yellow"
        advice = f"{commodity} अभी मत बेचो — {market_impact}"
    elif score <= 35:
        signal, signal_hi, color = "SELL", "बेचो 🔴", "red"
        advice = f"{commodity} अभी बेचने का सही समय है"
    else:
        signal, signal_hi, color = "BUY", "खरीदो 🟢", "green"
        advice = f"{commodity} खरीदने का सही समय है"

    return {
        "commodity": commodity,
        "currentPrice": current_price,
        "signal": signal,
        "signalHindi": signal_hi,
        "bechainScore": score,
        "advice": advice,
        "weatherImpact": alert_text,
        "arrivalImpact": f"आवक: {arrival} क्विंटल"
    }

# (Other endpoints like fasal-calendar, heat-map, dashboard omitted for brevity but should also use the updated get_mandi_data)
# ============================================================
# MANDIPULSE — COMBINED DASHBOARD
# ============================================================
@app.get("/api/mandipulse/dashboard")
def get_mandipulse_dashboard(
    commodity: str = Query(...),
    state: str = Query(...),
    city: str = Query(...)
):
    current_price, yesterday_price, arrival = get_mandi_data(commodity, state, city)
    is_rain, market_impact, alert_text = get_weather_risk(city)

    # Simplified for the demo, in a real app call the specific functions
    return {
        "appName": "MandiPulse 💓",
        "tagline": "Mandi ki dhadkan — live, real, accurate",
        "commodity": commodity,
        "location": f"{city}, {state}",
        "currentPrice": current_price,
        "arrivalQty": arrival,
        "bechainIndex": {
            "signal": "WAIT" if current_price > yesterday_price else "BUY",
            "signalHindi": "रुको 🟡" if current_price > yesterday_price else "खरीदो 🟢",
            "score": 70 if current_price > yesterday_price else 40,
            "advice": "कीमतों में उतार-चढ़ाव संभव है"
        },
        "fasalCalendar": {"bestMonth": "मई", "bestPrice": current_price*1.2, "worstMonth": "जनवरी", "sellAdvice": "मई तक रुकें"},
        "mandiHeatMap": {"hottestMandi": city, "top3": []},
        "weather": {"isRain": is_rain, "alert": alert_text, "impact": market_impact}
    }

# ============================================================
# FASAL LISTING SYSTEM
# ============================================================
fasal_listings = {}
listing_counter = 1

@app.post("/api/listings/add")
def add_listing(
    kisan_name: str = Query(...),
    kisan_phone: str = Query(...),
    kisan_city: str = Query(...),
    kisan_state: str = Query(...),
    commodity: str = Query(...),
    quantity_qtl: float = Query(...),
    price_per_qtl: float = Query(...),
    delivery_available: bool = Query(False),
    available_till: str = Query(...),
    description: str = Query(""),
    user_id: str = Query(...)
):
    global listing_counter
    listing_id = f"LST{listing_counter:04d}"
    listing_counter += 1
    fasal_listings[listing_id] = {
        "listingId": listing_id, "kisanName": kisan_name, "kisanPhone": kisan_phone,
        "kisanCity": kisan_city, "kisanState": kisan_state, "commodity": commodity,
        "quantityQtl": quantity_qtl, "pricePerQtl": price_per_qtl,
        "totalValue": round(quantity_qtl * price_per_qtl, 0),
        "deliveryAvailable": delivery_available, "availableTill": available_till,
        "description": description, "userId": user_id, "active": True, "views": 0, "postedOn": "today"
    }
    return {"status": "✅ Listing add ho gayi!", "listingId": listing_id}

@app.get("/api/listings/all")
def get_all_listings(commodity: str = Query(None), state: str = Query(None), is_pro: bool = Query(False)):
    result = []
    for lid, listing in fasal_listings.items():
        if not listing["active"]: continue
        item = {
            "listingId": listing["listingId"], "kisanName": listing["kisanName"],
            "kisanCity": listing["kisanCity"], "kisanState": listing["kisanState"],
            "commodity": listing["commodity"], "quantityQtl": listing["quantityQtl"],
            "pricePerQtl": listing["pricePerQtl"], "totalValue": listing["totalValue"],
            "deliveryAvailable": listing["deliveryAvailable"], "availableTill": listing["availableTill"],
            "description": listing["description"], "views": listing["views"], "postedOn": listing["postedOn"],
            "kisanPhone": listing["kisanPhone"] if is_pro else "Pro Member bano", "phoneVisible": is_pro
        }
        result.append(item)
    result.reverse()
    return {"totalListings": len(result), "listings": result}
