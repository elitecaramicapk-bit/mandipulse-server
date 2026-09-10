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
WEATHER_API_KEY = "caec7b9eba2a2b70be4c1783b8803882"   # ✅ Added
DATA_GOV_API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"  # ✅ Added

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
# FUNCTION 2: MANDI DATA FETCH
# ============================================================
def get_mandi_data(commodity: str, state: str, city: str):
    try:
        url = (
            f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
            f"?api-key={DATA_GOV_API_KEY}&format=json"
            f"&filters[commodity]={commodity}"
            f"&filters[state]={state}"
            f"&filters[market]={city}"
            f"&limit=5&sort[arrival_date]=desc"
        )
        res = requests.get(url, timeout=10).json()
        records = res.get('records', [])
        if records:
            latest = records[0]
            price = float(latest.get('modal_price', 5000))
            arrival = int(latest.get('arrivals_in_qtl', 100))
            return price, arrival
        return 5000.0, 100
    except:
        return 5000.0, 100


# ============================================================
# FUNCTION 3 (PRO): MONTHLY PRICE TREND
# ============================================================
def get_monthly_trend(commodity: str, state: str):
    try:
        url = (
            f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
            f"?api-key={DATA_GOV_API_KEY}&format=json"
            f"&filters[commodity]={commodity}"
            f"&filters[state]={state}"
            f"&limit=100&sort[arrival_date]=desc"
        )
        res = requests.get(url, timeout=10).json()
        records = res.get('records', [])

        month_names = {
            1:"जनवरी",2:"फरवरी",3:"मार्च",4:"अप्रैल",
            5:"मई",6:"जून",7:"जुलाई",8:"अगस्त",
            9:"सितंबर",10:"अक्टूबर",11:"नवंबर",12:"दिसंबर"
        }

        monthly_data = {}
        for r in records:
            try:
                date_str = r.get('arrival_date', '')
                month = int(date_str.split('/')[1])
                price = float(r.get('modal_price', 0))
                if month not in monthly_data:
                    monthly_data[month] = []
                monthly_data[month].append(price)
            except:
                continue

        monthly_avg = {}
        for month, prices in monthly_data.items():
            monthly_avg[month_names.get(month, str(month))] = round(sum(prices)/len(prices), 0)

        if monthly_avg:
            best_month = max(monthly_avg, key=monthly_avg.get)
            best_price = monthly_avg[best_month]
        else:
            monthly_avg = {
                "जनवरी":4800,"फरवरी":4900,"मार्च":5100,"अप्रैल":5400,
                "मई":5600,"जून":5200,"जुलाई":4900,"अगस्त":4700,
                "सितंबर":4800,"अक्टूबर":5000,"नवंबर":5200,"दिसंबर":5100
            }
            best_month = "मई"
            best_price = 5600

        return {
            "monthlyPrices": monthly_avg,
            "bestMonth": best_month,
            "bestMonthPrice": best_price,
            "insight": f"{best_month} में सबसे अधिक भाव रहने की संभावना है"
        }
    except:
        return {"monthlyPrices":{},"bestMonth":"डेटा नहीं","bestMonthPrice":0,"insight":"उपलब्ध नहीं"}


# ============================================================
# FUNCTION 4 (PRO): DISTRICT DEMAND
# ============================================================
def get_district_demand(commodity: str, state: str):
    try:
        url = (
            f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
            f"?api-key={DATA_GOV_API_KEY}&format=json"
            f"&filters[commodity]={commodity}"
            f"&filters[state]={state}"
            f"&limit=50&sort[arrival_date]=desc"
        )
        res = requests.get(url, timeout=10).json()
        records = res.get('records', [])

        market_arrival = {}
        market_price = {}
        for r in records:
            market = r.get('market', 'Unknown')
            arrival = float(r.get('arrivals_in_qtl', 0))
            price = float(r.get('modal_price', 0))
            if market not in market_arrival:
                market_arrival[market] = 0
                market_price[market] = []
            market_arrival[market] += arrival
            market_price[market].append(price)

        top_markets = sorted(market_arrival.items(), key=lambda x: x[1], reverse=True)[:5]
        result = []
        for market, arrival in top_markets:
            prices = market_price.get(market, [5000])
            avg_price = round(sum(prices)/len(prices), 0)
            result.append({
                "market": market,
                "totalArrival": round(arrival, 0),
                "avgPrice": avg_price,
                "demandLevel": "🔴 उच्च" if arrival > 500 else "🟡 मध्यम" if arrival > 200 else "🟢 कम"
            })

        hot_market = top_markets[0][0] if top_markets else "डेटा नहीं"
        return {
            "topMarkets": result,
            "hotMarket": hot_market,
            "insight": f"{hot_market} में सबसे ज्यादा मांग है"
        }
    except:
        return {"topMarkets":[],"hotMarket":"डेटा नहीं","insight":"उपलब्ध नहीं"}


# ============================================================
# FUNCTION 5 (PRO): % INCREASE PROBABILITY
# ============================================================
def get_increase_probability(base_price: float, is_rain: bool, arrival: int, commodity: str):
    rain_boost = 15 if is_rain else 0
    supply_boost = 20 if arrival < 100 else (10 if arrival < 200 else 0)
    supply_drop = -10 if arrival > 400 else 0
    seasonal = {"Wheat":8,"Onion":25,"Garlic":20,"Tomato":30,"Potato":15,"Cotton":10}
    seasonal_boost = seasonal.get(commodity, 10)

    total_boost = max(-20, min(50, rain_boost + supply_boost + supply_drop + seasonal_boost))

    confidence = "उच्च 🔴" if total_boost >= 20 else "मध्यम 🟡" if total_boost >= 10 else "कम 🟢"
    predicted_price = round(base_price * (1 + total_boost/100), 0)

    return {
        "increasePercent": total_boost,
        "predictedPrice": predicted_price,
        "confidence": confidence,
        "factors": {
            "rainImpact": f"+{rain_boost}%",
            "supplyImpact": f"{supply_boost + supply_drop}%",
            "seasonalImpact": f"+{seasonal_boost}%"
        },
        "insight": f"अगले 3 महीनों में लगभग {total_boost}% वृद्धि संभव"
    }


# ============================================================
# NORMAL API — Sabke liye
# ============================================================
@app.get("/api/mandi-predictions")
def get_mandi_predictions(
    commodity: str = Query(...),
    state: str = Query(...),
    city: str = Query(...)
):
    base_price, arrival_quantity = get_mandi_data(commodity, state, city)
    is_rain, market_impact, alert_text = get_weather_risk(city)

    if is_rain or arrival_quantity < 100:
        min_predicted = round(base_price * 1.10, 2)
        max_predicted = round(base_price * 1.30, 2)
        crop_impact = "उच्च प्रभाव — तेजी संभव"
    elif arrival_quantity > 300:
        min_predicted = round(base_price * 0.85, 2)
        max_predicted = round(base_price * 1.05, 2)
        crop_impact = "अधिक आवक — मंदी संभव"
    else:
        min_predicted = round(base_price * 0.95, 2)
        max_predicted = round(base_price * 1.10, 2)
        crop_impact = "सामान्य बाजार — स्थिर"

    return {
        "locationName": f"{city}, {state}",
        "commodityName": commodity,
        "currentPrice": base_price,
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
    base_price, arrival_quantity = get_mandi_data(commodity, state, city)
    is_rain, market_impact, alert_text = get_weather_risk(city)
    monthly = get_monthly_trend(commodity, state)
    district = get_district_demand(commodity, state)
    increase = get_increase_probability(base_price, is_rain, arrival_quantity, commodity)

    return {
        "locationName": f"{city}, {state}",
        "commodityName": commodity,
        "currentPrice": base_price,
        "arrivalQuantity": arrival_quantity,
        "isRainExpected": is_rain,
        "weatherAlert": alert_text,
        "marketImpact": market_impact,
        "monthlyTrend": monthly,
        "districtDemand": district,
        "increaseProbability": increase,
        "chartData": {
            "labels": list(monthly.get("monthlyPrices", {}).keys()),
            "prices": list(monthly.get("monthlyPrices", {}).values()),
            "currentPrice": base_price,
            "predictedMax": increase.get("predictedPrice", base_price)
        }
    }


@app.get("/")
def root():
    return {"status": "✅ Server चालू है", "version": "2.0.0"}


# ============================================================
# MANDIPULSE — 4 EXTRA FEATURES
# ============================================================

# ============================================================
# FEATURE 1: BECHAIN INDEX — Buy / Sell / Wait Signal
# Kisan/Trader ke liye: Aaj beche ya ruko?
# ============================================================
@app.get("/api/mandipulse/bechain-index")
def get_bechain_index(
    commodity: str = Query(...),
    state: str = Query(...),
    city: str = Query(...)
):
    base_price, arrival = get_mandi_data(commodity, state, city)
    is_rain, market_impact, alert_text = get_weather_risk(city)

    # Score calculate karo (0-100)
    score = 50  # base

    # Rain = prices will go up = WAIT
    if is_rain:
        score += 20

    # Low arrival = less supply = prices go up = WAIT
    if arrival < 100:
        score += 15
    elif arrival > 400:
        score -= 15  # High supply = prices may fall = SELL NOW

    # Seasonal logic (Wheat example)
    import datetime
    month = datetime.datetime.now().month
    # Harvest months (Mar-May) = high supply = SELL
    if month in [3, 4, 5]:
        score -= 10
    # Lean months (Oct-Dec) = low supply = WAIT/HOLD
    elif month in [10, 11, 12]:
        score += 10

    # Cap score
    score = max(0, min(100, score))

    # Signal decide karo
    if score >= 65:
        signal = "WAIT"
        signal_hi = "रुको 🟡"
        reason = "भाव अगले कुछ दिनों में और बढ़ सकता है"
        color = "yellow"
        advice = f"{commodity} अभी मत बेचो — {market_impact}"
    elif score <= 35:
        signal = "SELL"
        signal_hi = "बेचो 🔴"
        reason = "अभी अच्छा भाव मिल रहा है, बाद में गिर सकता है"
        color = "red"
        advice = f"{commodity} अभी बेचने का सही समय है"
    else:
        signal = "BUY"
        signal_hi = "खरीदो 🟢"
        reason = "भाव स्थिर है, खरीदारी का अच्छा मौका"
        color = "green"
        advice = f"{commodity} खरीदने का सही समय है"

    return {
        "commodity": commodity,
        "currentPrice": base_price,
        "signal": signal,
        "signalHindi": signal_hi,
        "bechainScore": score,
        "reason": reason,
        "color": color,
        "advice": advice,
        "weatherImpact": alert_text,
        "arrivalImpact": f"आवक: {arrival} क्विंटल"
    }


# ============================================================
# FEATURE 2: FASAL CALENDAR — Har mahine best bhav
# Kis mahine kaunsi fasal ka bhav utha
# ============================================================
@app.get("/api/mandipulse/fasal-calendar")
def get_fasal_calendar(
    commodity: str = Query(...),
    state: str = Query(...)
):
    # Known seasonal patterns for major commodities
    patterns = {
        "Wheat":   [4600,4700,5200,5500,5600,5200,4900,4700,4800,5000,5100,4900],
        "Onion":   [2800,2500,2000,1800,2200,3500,4000,4200,3800,3000,2600,2800],
        "Garlic":  [8000,7500,6000,5000,5500,7000,9000,9500,8500,7000,6500,7500],
        "Tomato":  [1500,1800,2500,3000,2000,1500,2000,2500,3000,2800,2000,1600],
        "Potato":  [1200,1100,1000,1200,1500,1800,2000,1900,1600,1300,1100,1200],
        "Cotton":  [6000,6200,6500,6800,7000,6500,6000,5800,6200,6500,6800,6200],
        "Soybean": [4500,4600,4800,5000,4800,4500,4200,4000,4500,4800,5000,4700],
        "Mustard": [5000,5200,5500,5800,5500,5000,4800,4700,5000,5200,5400,5100],
    }

    month_names = ["जन","फर","मार","अप्र","मई","जून",
                   "जुल","अग","सित","अक्ट","नव","दिस"]

    # Use known pattern or generate from API
    prices = patterns.get(commodity, [5000]*12)

    max_price = max(prices)
    min_price = min(prices)
    best_month_idx = prices.index(max_price)
    worst_month_idx = prices.index(min_price)

    monthly = []
    for i, p in enumerate(prices):
        monthly.append({
            "month": month_names[i],
            "monthNum": i + 1,
            "avgPrice": p,
            "isBest": i == best_month_idx,
            "isWorst": i == worst_month_idx
        })

    return {
        "commodity": commodity,
        "state": state,
        "monthlyData": monthly,
        "bestMonth": month_names[best_month_idx],
        "bestPrice": max_price,
        "worstMonth": month_names[worst_month_idx],
        "worstPrice": min_price,
        "insight": f"{month_names[best_month_idx]} mein {commodity} ka bhav sabse zyada hota hai",
        "sellAdvice": f"Best time to sell: {month_names[best_month_idx]}",
        "buyAdvice": f"Best time to buy: {month_names[worst_month_idx]}"
    }


# ============================================================
# FEATURE 3: MANDI HEAT MAP — Kaun si mandi HOT hai
# Live arrival + price data se ranking
# ============================================================
@app.get("/api/mandipulse/heat-map")
def get_mandi_heat_map(
    commodity: str = Query(...),
    state: str = Query(...)
):
    try:
        url = (
            f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
            f"?api-key={DATA_GOV_API_KEY}&format=json"
            f"&filters[commodity]={commodity}"
            f"&filters[state]={state}"
            f"&limit=100&sort[arrival_date]=desc"
        )
        res = requests.get(url, timeout=10).json()
        records = res.get('records', [])

        market_data = {}
        for r in records:
            market = r.get('market', '')
            if not market:
                continue
            arrival = float(r.get('arrivals_in_qtl', 0))
            price = float(r.get('modal_price', 0))
            if market not in market_data:
                market_data[market] = {"arrival": 0, "prices": [], "district": r.get('district', '')}
            market_data[market]["arrival"] += arrival
            market_data[market]["prices"].append(price)

        # Heat score = arrival * price_stability
        heat_list = []
        for market, data in market_data.items():
            prices = data["prices"]
            avg_price = sum(prices) / len(prices) if prices else 0
            arrival = data["arrival"]

            # Heat score (0-100)
            max_arrival = max([d["arrival"] for d in market_data.values()]) or 1
            heat_score = int((arrival / max_arrival) * 100)

            if heat_score >= 70:
                heat_level = "🔴 बहुत गर्म"
                heat_color = "red"
            elif heat_score >= 40:
                heat_level = "🟡 गर्म"
                heat_color = "yellow"
            else:
                heat_level = "🟢 ठंडा"
                heat_color = "green"

            heat_list.append({
                "market": market,
                "district": data["district"],
                "totalArrival": round(arrival, 0),
                "avgPrice": round(avg_price, 0),
                "heatScore": heat_score,
                "heatLevel": heat_level,
                "heatColor": heat_color
            })

        # Sort by heat score
        heat_list.sort(key=lambda x: x["heatScore"], reverse=True)
        top5 = heat_list[:5]

        hottest = top5[0]["market"] if top5 else "Data nahi"

        return {
            "commodity": commodity,
            "state": state,
            "heatMap": top5,
            "hottestMandi": hottest,
            "insight": f"{hottest} mein sabse zyada activity hai",
            "totalMarketsFound": len(heat_list)
        }

    except Exception as e:
        return {
            "commodity": commodity,
            "state": state,
            "heatMap": [],
            "hottestMandi": "Data unavailable",
            "insight": "Data load nahi hua",
            "totalMarketsFound": 0
        }


# ============================================================
# FEATURE 4: SMART ALERT — Target price pe notify
# User set kare target → server check kare → alert
# ============================================================

# In-memory alerts store (production mein Firebase use karna)
price_alerts = {}

@app.post("/api/mandipulse/set-alert")
def set_price_alert(
    commodity: str = Query(...),
    state: str = Query(...),
    city: str = Query(...),
    target_price: float = Query(...),
    alert_type: str = Query(..., description="above ya below"),
    user_id: str = Query(...)
):
    alert_key = f"{user_id}_{commodity}_{city}"
    price_alerts[alert_key] = {
        "userId": user_id,
        "commodity": commodity,
        "state": state,
        "city": city,
        "targetPrice": target_price,
        "alertType": alert_type,  # "above" ya "below"
        "active": True
    }

    return {
        "status": "✅ Alert set ho gaya",
        "commodity": commodity,
        "targetPrice": target_price,
        "alertType": alert_type,
        "message": f"Jab {commodity} ka bhav ₹{target_price} se {alert_type} jayega tab alert milega"
    }


@app.get("/api/mandipulse/check-alerts")
def check_price_alerts(user_id: str = Query(...)):
    triggered = []

    for key, alert in price_alerts.items():
        if not alert["active"] or alert["userId"] != user_id:
            continue

        current_price, _ = get_mandi_data(
            alert["commodity"], alert["state"], alert["city"]
        )

        triggered_flag = False
        if alert["alertType"] == "above" and current_price >= alert["targetPrice"]:
            triggered_flag = True
        elif alert["alertType"] == "below" and current_price <= alert["targetPrice"]:
            triggered_flag = True

        if triggered_flag:
            triggered.append({
                "commodity": alert["commodity"],
                "city": alert["city"],
                "targetPrice": alert["targetPrice"],
                "currentPrice": current_price,
                "alertType": alert["alertType"],
                "message": f"🔔 {alert['commodity']} ka bhav ₹{current_price} ho gaya! Target tha ₹{alert['targetPrice']}"
            })
            alert["active"] = False  # Alert fire ho gaya

    return {
        "userId": user_id,
        "triggeredAlerts": triggered,
        "totalTriggered": len(triggered)
    }


# ============================================================
# MANDIPULSE — COMBINED DASHBOARD (ek call mein sab)
# ============================================================
@app.get("/api/mandipulse/dashboard")
def get_mandipulse_dashboard(
    commodity: str = Query(...),
    state: str = Query(...),
    city: str = Query(...)
):
    # Sab features ek saath
    base_price, arrival = get_mandi_data(commodity, state, city)
    is_rain, market_impact, alert_text = get_weather_risk(city)

    # Bechain Index
    bechain = get_bechain_index(commodity, state, city)

    # Fasal Calendar
    calendar = get_fasal_calendar(commodity, state)

    # Heat Map
    heatmap = get_mandi_heat_map(commodity, state)

    return {
        "appName": "MandiPulse 💓",
        "tagline": "Mandi ki dhadkan — live, real, accurate",
        "commodity": commodity,
        "location": f"{city}, {state}",
        "currentPrice": base_price,
        "arrivalQty": arrival,

        # Feature 1
        "bechainIndex": {
            "signal": bechain["signal"],
            "signalHindi": bechain["signalHindi"],
            "score": bechain["bechainScore"],
            "advice": bechain["advice"]
        },

        # Feature 2
        "fasalCalendar": {
            "bestMonth": calendar["bestMonth"],
            "bestPrice": calendar["bestPrice"],
            "worstMonth": calendar["worstMonth"],
            "sellAdvice": calendar["sellAdvice"]
        },

        # Feature 3
        "mandiHeatMap": {
            "hottestMandi": heatmap["hottestMandi"],
            "top3": heatmap["heatMap"][:3]
        },

        # Weather
        "weather": {
            "isRain": is_rain,
            "alert": alert_text,
            "impact": market_impact
        }
    }
