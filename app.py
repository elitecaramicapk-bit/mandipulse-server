# ============================================================
# MANDI BHAV — MandiPulse Server
# app.py v4.0.0 — 10 DATA SOURCES
# ============================================================
#
# SOURCE LIST:
#  S1 : data.gov.in          (Government API - MOST TRUSTED)
#  S2 : Agmarknet scraper    (Govt agriculture market data)
#  S3 : eNAM API             (National Agriculture Market)
#  S4 : farmer.in scraper    (Agmarknet aggregator)
#  S5 : Krishi Jagran RSS    (Agriculture newspaper)
#  S6 : Hindi KJ RSS         (Hindi agriculture news)
#  S7 : Kisan Tak RSS        (Farmer video news portal)
#  S8 : Gaon Connection RSS  (Rural news portal)
#  S9 : ABP Live Kisan RSS   (ABP News agriculture feed)
#  S10: DD Kisan RSS         (Doordarshan Kisan channel)
#
# VALIDATION:
#  - Per-commodity min/max range check
#  - IQR outlier rejection
#  - Weighted cross-validation (Gov=3, Agmark=2, others=1)
#  - Returns null (never fake 5000) if all fail
# ============================================================

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import requests
import feedparser
import re
import logging
import concurrent.futures
from typing import Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("MandiPulse")

app = FastAPI(title="MandiPulse API", version="4.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ============================================================
# STATIC DATA
# ============================================================
DATA_GOV_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"

HINDI = {
    "Wheat":"गेहूँ","Rice":"चावल","Onion":"प्याज","Potato":"आलू",
    "Tomato":"टमाटर","Garlic":"लहसुन","Mustard":"सरसों",
    "Soybean":"सोयाबीन","Cotton":"कपास","Maize":"मक्का",
    "Chana":"चना","Moong":"मूँग","Urad":"उड़द","Jeera":"जीरा",
    "Coriander":"धनिया","Fennel":"सौंफ","Methi":"मेथी",
    "Groundnut":"मूँगफली","Barley":"जौ","Jowar":"ज्वार",
    "Bajra":"बाजरा","Sugarcane":"गन्ना","Guar":"ग्वार","Castor":"अरंडी"
}

MSP = {
    "Wheat":2425,"Rice":2300,"Maize":2090,"Jowar":3371,
    "Bajra":2625,"Barley":1735,"Chana":5440,"Moong":8682,
    "Urad":7400,"Mustard":5950,"Soybean":4892,"Cotton":7121,
    "Groundnut":6783,"Sugarcane":340,"Guar":5887
}

# Valid ₹/quintal ranges — FAKE PRICE GUARD
RANGES = {
    "Wheat":(1500,4500),"Rice":(1800,6000),"Onion":(200,12000),
    "Potato":(300,5000),"Tomato":(100,20000),"Garlic":(1000,50000),
    "Mustard":(4000,10000),"Soybean":(3000,8000),"Cotton":(5000,12000),
    "Maize":(1200,4000),"Chana":(3500,10000),"Moong":(5000,14000),
    "Urad":(4500,12000),"Barley":(1000,3500),"Bajra":(1500,4500),
    "Groundnut":(4000,10000),"Jowar":(2000,5000),"Jeera":(15000,80000),
    "Coriander":(5000,25000),"Guar":(3000,10000),"Castor":(5000,12000),
    "Fennel":(8000,30000),"Sugarcane":(250,450),"Methi":(3000,15000)
}

# Commodity aliases for news scraping (English + Hindi)
ALIASES = {
    "Wheat":    ["wheat","gehun","gehu","गेहूँ","गेहू"],
    "Rice":     ["rice","paddy","dhan","chawal","चावल","धान"],
    "Onion":    ["onion","pyaz","pyaaj","प्याज"],
    "Potato":   ["potato","aloo","आलू"],
    "Tomato":   ["tomato","tamatar","टमाटर"],
    "Garlic":   ["garlic","lahsun","लहसुन"],
    "Mustard":  ["mustard","sarson","सरसों","राई"],
    "Soybean":  ["soybean","soya","सोयाबीन"],
    "Cotton":   ["cotton","kapas","कपास","नरमा"],
    "Maize":    ["maize","corn","makka","मक्का"],
    "Chana":    ["chana","gram","chickpea","चना"],
    "Moong":    ["moong","mung","मूँग"],
    "Urad":     ["urad","उड़द","उड़द दाल"],
    "Jeera":    ["jeera","cumin","जीरा"],
    "Coriander":["coriander","dhaniya","धनिया"],
    "Groundnut":["groundnut","peanut","moongfali","मूँगफली"],
    "Barley":   ["barley","jau","जौ"],
    "Bajra":    ["bajra","pearl millet","बाजरा"],
    "Guar":     ["guar","ग्वार","गुआर"],
    "Castor":   ["castor","arandi","अरंडी"],
    "Fennel":   ["fennel","saunf","सौंफ"],
    "Jowar":    ["jowar","sorghum","ज्वार"]
}

# RSS feeds — S5 to S10
RSS_SOURCES = [
    ("https://www.krishijagran.com/feed/",            "Krishi Jagran EN"),
    ("https://hindi.krishijagran.com/feed/",          "Krishi Jagran HI"),
    ("https://www.kisantak.in/feed/",                 "Kisan Tak"),
    ("https://www.gaonconnection.com/feed",           "Gaon Connection"),
    ("https://khabar.ndtv.com/rss/kisan",             "NDTV Kisan"),
    ("https://www.abplive.com/agriculture/feed",      "ABP Kisan"),
    ("https://ddnews.gov.in/rss/kisan",               "DD Kisan"),
    ("https://www.jagran.com/rss/news-national.xml",  "Dainik Jagran"),
    ("https://www.bhaskar.com/rss-feed/1555/",        "Dainik Bhaskar"),
    ("https://www.patrika.com/rss/news.xml",          "Rajasthan Patrika"),
]

# ============================================================
# HELPERS
# ============================================================

def safe_f(v, default=None):
    if v is None or str(v).strip() in ("","None","N/A","-","null"):
        return default
    try:
        return float(str(v).replace(",","").strip())
    except:
        return default

def safe_i(v, default=0):
    f = safe_f(v)
    return int(f) if f is not None else default

def in_range(commodity, price):
    if price is None or price <= 0:
        return False
    lo, hi = RANGES.get(commodity, (100, 200000))
    ok = lo <= price <= hi
    if not ok:
        log.warning(f"RANGE_FAIL {commodity} ₹{price} not in [{lo},{hi}]")
    return ok

PRICE_RE = re.compile(
    r'(?:₹|rs\.?|rupay|rupees|bhav|rate|दर|भाव|price)'
    r'\s*[:\-]?\s*([1-9]\d{2,4}(?:[,\.]\d{1,3})?)',
    re.IGNORECASE)
NUM_RE = re.compile(r'\b([1-9]\d{3,4})\b')

def extract_price(text):
    m = PRICE_RE.search(text)
    if m:
        try:
            return float(m.group(1).replace(",",""))
        except:
            pass
    nums = []
    for m2 in NUM_RE.finditer(text):
        try:
            v = float(m2.group(1))
            if 500 <= v <= 100000:
                nums.append(v)
        except:
            pass
    return sorted(nums)[len(nums)//2] if nums else None

def weighted_median(price_weight_list):
    """[(price, weight), ...] → weighted median"""
    if not price_weight_list:
        return None
    expanded = []
    for price, w in price_weight_list:
        expanded.extend([price] * w)
    expanded.sort()
    return expanded[len(expanded)//2]

def iqr_filter(values):
    if len(values) < 3:
        return values, []
    s = sorted(values)
    n = len(s)
    q1 = s[n//4]
    q3 = s[(3*n)//4]
    iqr = q3 - q1
    lo = q1 - 1.5*iqr
    hi = q3 + 1.5*iqr
    ok  = [v for v in values if lo <= v <= hi]
    bad = [v for v in values if v < lo or v > hi]
    return ok, bad

# ============================================================
# S1: data.gov.in  (WEIGHT=3)
# ============================================================

def src_gov_api(commodity, state, city):
    try:
        url = (
            f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
            f"?api-key={DATA_GOV_KEY}&format=json"
            f"&filters[commodity]={commodity}"
            f"&filters[state]={state}"
            f"&filters[market]={city}&limit=5"
        )
        log.info(f"S1:GOV_API → {commodity}/{city}")
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        recs = r.json().get("records", [])
        if not recs:
            return None
        rec  = recs[0]
        prev = recs[1] if len(recs) > 1 else rec
        modal = safe_f(rec.get("modal_price"))
        if modal is None or not in_range(commodity, modal):
            return None
        return {
            "price": modal,
            "max":   safe_f(rec.get("max_price"), modal),
            "min":   safe_f(rec.get("min_price"), modal),
            "prev":  safe_f(prev.get("modal_price")),
            "qty":   safe_i(rec.get("arrivals_in_qtl")),
            "date":  rec.get("arrival_date","N/A"),
            "src":   "data.gov.in", "weight": 3
        }
    except Exception as e:
        log.warning(f"S1 error {commodity}: {e}")
        return None

# ============================================================
# S2: Agmarknet HTML scraper  (WEIGHT=3)
# ============================================================

AGMARK_NAMES = {
    "Wheat":"Wheat","Mustard":"Mustard(Sarson(Black))",
    "Chana":"Gram(Whole)","Soybean":"Soyabean",
    "Maize":"Maize","Barley":"Barley","Bajra":"Bajra",
    "Moong":"Moong(Whole)","Onion":"Onion","Garlic":"Garlic",
    "Rice":"Paddy(Desi)(Common)","Cotton":"Cotton",
    "Groundnut":"Groundnut","Jowar":"Jowar","Urad":"Urad (Whole)"
}

def src_agmarknet(commodity, state):
    try:
        name = AGMARK_NAMES.get(commodity, commodity)
        today = datetime.now().strftime("%d-%b-%Y")
        url = (
            f"https://agmarknet.gov.in/SearchCmmMkt.aspx"
            f"?Tx_Commodity={requests.utils.quote(name)}"
            f"&Tx_State={state}&Tx_District=0&Tx_Market=0"
            f"&DateFrom={today}&DateTo={today}"
            f"&Fr_Date={today}&To_Date={today}&Tx_Trend=0"
        )
        log.info(f"S2:AGMARKNET → {commodity}/{state}")
        r = requests.get(url, timeout=12,
                         headers={"User-Agent": "Mozilla/5.0"})
        prices = re.findall(r'<td[^>]*>(\d{3,5}(?:\.\d{1,2})?)</td>', r.text)
        valid  = [float(p) for p in prices if in_range(commodity, float(p))]
        if not valid:
            return None
        median = sorted(valid)[len(valid)//2]
        log.info(f"S2 {commodity} → ₹{median}")
        return {"price": median, "src": "agmarknet.gov.in", "weight": 3}
    except Exception as e:
        log.warning(f"S2 error {commodity}: {e}")
        return None

# ============================================================
# S3: eNAM (National Agriculture Market)  (WEIGHT=2)
# ============================================================

ENAM_CODES = {
    "Wheat":"102","Rice":"104","Mustard":"214","Chana":"301",
    "Moong":"306","Maize":"113","Soybean":"218","Groundnut":"206",
    "Onion":"412","Potato":"408","Garlic":"416","Barley":"107",
    "Bajra":"110","Cotton":"501","Urad":"305","Jowar":"111"
}

def src_enam(commodity, state, city):
    try:
        code = ENAM_CODES.get(commodity)
        if not code:
            return None
        url = (
            f"https://enam.gov.in/web/dashboard/commodityPrice"
            f"?commodity={code}&state={state}&market={city}"
        )
        log.info(f"S3:ENAM → {commodity}")
        r = requests.get(url, timeout=10,
                         headers={"Accept":"application/json"})
        data = r.json()
        if isinstance(data, list) and data:
            price = safe_f(data[0].get("modalPrice"))
        elif isinstance(data, dict):
            price = safe_f(data.get("modalPrice") or data.get("price"))
        else:
            return None
        if not in_range(commodity, price):
            return None
        log.info(f"S3 {commodity} → ₹{price}")
        return {"price": price, "src": "enam.gov.in", "weight": 2}
    except Exception as e:
        log.warning(f"S3 error {commodity}: {e}")
        return None

# ============================================================
# S4: farmer.in / gramidost.com scraper  (WEIGHT=2)
# ============================================================

def src_farmer_in(commodity, state):
    try:
        state_slug = state.lower().replace(" ","-")
        comm_slug  = commodity.lower()
        urls = [
            f"https://www.farmer.in/mandi-bhav/crop/{comm_slug}/state/{state_slug}/",
            f"https://gramidost.com/mandi-bhav.html",
        ]
        for url in urls:
            try:
                log.info(f"S4:FARMER.IN → {commodity}")
                r = requests.get(url, timeout=10,
                                 headers={"User-Agent":"Mozilla/5.0"})
                # Look for price pattern near commodity name
                aliases = ALIASES.get(commodity, [commodity.lower()])
                text = r.text.lower()
                for alias in aliases:
                    idx = text.find(alias.lower())
                    if idx >= 0:
                        chunk = r.text[max(0,idx-30):idx+150]
                        price = extract_price(chunk)
                        if price and in_range(commodity, price):
                            log.info(f"S4 {commodity} → ₹{price}")
                            return {"price":price,"src":"farmer.in","weight":2}
            except:
                continue
        return None
    except Exception as e:
        log.warning(f"S4 error {commodity}: {e}")
        return None

# ============================================================
# S5-S10: RSS News feeds  (WEIGHT=1 each)
# ============================================================

def src_rss_all(commodity):
    """Fetch ALL RSS feeds concurrently and extract prices."""
    aliases = ALIASES.get(commodity, [commodity.lower()])
    all_prices = []

    def fetch_feed(feed_url, feed_name):
        prices = []
        try:
            log.info(f"RSS:{feed_name} → {commodity}")
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:40]:
                text = (
                    entry.get("title","") + " " +
                    entry.get("summary","") + " " +
                    entry.get("description","")
                ).lower()
                if any(a.lower() in text for a in aliases):
                    p = extract_price(text)
                    if p and in_range(commodity, p):
                        prices.append(p)
        except Exception as e:
            log.debug(f"RSS {feed_name} error: {e}")
        return prices

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_feed, url, name): name
                   for url, name in RSS_SOURCES}
        for fut in concurrent.futures.as_completed(futures):
            try:
                all_prices.extend(fut.result())
            except:
                pass

    if not all_prices:
        return None
    median = sorted(all_prices)[len(all_prices)//2]
    log.info(f"RSS_ALL {commodity} → ₹{median} ({len(all_prices)} mentions)")
    return {"price": median, "src": "news_rss", "weight": 1}

# ============================================================
# WEATHER (called once per request)
# ============================================================

def get_weather(city):
    try:
        r = requests.get(f"https://wttr.in/{city}?format=j1", timeout=8).json()
        c    = r.get("current_condition",[{}])[0]
        temp = c.get("temp_C","30")
        desc = c.get("weatherDesc",[{}])[0].get("value","").lower()
        rain = any(w in desc for w in ["rain","drizzle","thunder","shower"])
        return (rain,
                "तेजी" if rain else "सामान्य",
                f"🌧️ बारिश ({temp}°C)" if rain else f"☀️ साफ ({temp}°C)")
    except:
        return False, "सामान्य", "☀️ मौसम साफ"

# ============================================================
# CROSS-VALIDATION ENGINE
# ============================================================

def cross_validate(commodity, source_results):
    """
    source_results = list of dicts: {price, src, weight}
    Steps:
      1. Range check already done per source
      2. IQR outlier removal
      3. Weighted median
    """
    if not source_results:
        return None, [], []

    prices = [s["price"] for s in source_results]
    ok_prices, bad_prices = iqr_filter(prices)

    accepted = [s for s in source_results if s["price"] in ok_prices]
    rejected = [s["src"] for s in source_results if s["price"] in bad_prices]

    if rejected:
        log.warning(f"IQR_REJECT {commodity}: {rejected}")

    if not accepted:
        # Fallback: use highest-weight source
        best = max(source_results, key=lambda x: x["weight"])
        accepted = [best]

    # Cross-check: if gov_api present, reject >25% deviators
    gov = next((s for s in accepted if s["src"]=="data.gov.in"), None)
    if gov:
        anchor = gov["price"]
        final_accepted = []
        for s in accepted:
            dev = abs(s["price"] - anchor) / anchor
            if dev > 0.25 and s["src"] != "data.gov.in":
                log.warning(f"CROSS_REJECT {commodity} {s['src']} "
                            f"₹{s['price']} vs anchor ₹{anchor} ({dev*100:.1f}%)")
                rejected.append(f"{s['src']}(₹{s['price']:.0f})")
            else:
                final_accepted.append(s)
        accepted = final_accepted if final_accepted else [gov]

    pw_list = [(s["price"], s["weight"]) for s in accepted]
    final   = weighted_median(pw_list)
    srcs    = list({s["src"] for s in accepted})

    log.info(f"VALIDATED {commodity} → ₹{final:.0f} "
             f"[{len(srcs)} sources: {srcs}]")
    return final, srcs, rejected

# ============================================================
# BECHAIN INDEX
# ============================================================

def bechain(commodity, price, prev):
    msp    = MSP.get(commodity, 0)
    chg    = ((price-prev)/prev*100) if prev and prev>0 else 0
    msp_gap= ((price-msp)/msp*100)   if msp > 0          else 0
    score  = 50
    if chg  >  3: score += 20
    elif chg >  1: score += 10
    elif chg < -3: score -= 20
    elif chg < -1: score -= 10
    if msp_gap > 15: score += 15
    elif msp_gap > 5: score += 8
    elif msp_gap <  0: score -= 15
    score = max(0, min(100, score))
    if score >= 70:
        sig, hin, col = "BUY",  "बेचो 🟢",  "green"
    elif score >= 45:
        sig, hin, col = "WAIT", "रुको 🟡",  "yellow"
    else:
        sig, hin, col = "HOLD", "होल्ड 🔴", "red"
    return {"signal":sig,"signalHindi":hin,"score":score,
            "color":col,"changePct":round(chg,2),
            "mspGapPct":round(msp_gap,2)}

# ============================================================
# MAIN COMMODITY PROCESSOR
# ============================================================

def process_commodity(commodity, state, city, is_rain, impact, alert):
    """Fetch from ALL 10 sources concurrently, validate, return result."""
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        f_gov    = ex.submit(src_gov_api,    commodity, state, city)
        f_agmark = ex.submit(src_agmarknet,  commodity, state)
        f_enam   = ex.submit(src_enam,       commodity, state, city)
        f_farmer = ex.submit(src_farmer_in,  commodity, state)
        f_rss    = ex.submit(src_rss_all,    commodity)

        for f in [f_gov, f_agmark, f_enam, f_farmer, f_rss]:
            try:
                res = f.result()
                if res:
                    results.append(res)
            except:
                pass

    gov_data = f_gov.result() if f_gov.done() else None

    final_price, srcs_used, srcs_rejected = cross_validate(commodity, results)

    if final_price is None:
        return {
            "commodityName":  commodity,
            "commodityHindi": HINDI.get(commodity, commodity),
            "currentPrice":   None,
            "maxPrice":       None, "minPrice": None,
            "yesterdayPrice": None, "priceChange": None,
            "arrivalQuantity":0, "updateDate":"N/A",
            "mspValue":       MSP.get(commodity,0),
            "priceVsMsp":     None,
            "priceStatus":    "UNVERIFIED",
            "sourcesUsed":    [],
            "sourcesRejected":srcs_rejected,
            "bechainIndex":   {"signal":"NONE","signalHindi":"डेटा नहीं ⚪","score":0},
            "isRainExpected": is_rain,
            "weatherAlert":   alert,
            "marketImpact":   impact,
            "dataSourceCount":0
        }

    prev  = gov_data.get("prev")  if gov_data else None
    date  = gov_data.get("date","N/A") if gov_data else "N/A"
    qty   = gov_data.get("qty",0)      if gov_data else 0
    p_max = gov_data.get("max",final_price) if gov_data else final_price
    p_min = gov_data.get("min",final_price) if gov_data else final_price
    msp_v = MSP.get(commodity,0)

    n_src = len(set(srcs_used))
    status = ("VERIFIED" if n_src >= 2
              else "SINGLE_SOURCE" if n_src == 1
              else "UNVERIFIED")

    return {
        "commodityName":   commodity,
        "commodityHindi":  HINDI.get(commodity, commodity),
        "currentPrice":    round(final_price, 2),
        "maxPrice":        p_max, "minPrice": p_min,
        "yesterdayPrice":  prev,
        "priceChange":     round(final_price - prev, 2) if prev else None,
        "arrivalQuantity": qty,
        "updateDate":      date,
        "mspValue":        msp_v,
        "priceVsMsp":      round(final_price - msp_v, 2) if msp_v else None,
        "priceStatus":     status,
        "sourcesUsed":     list(set(srcs_used)),
        "sourcesRejected": srcs_rejected,
        "bechainIndex":    bechain(commodity, final_price, prev),
        "isRainExpected":  is_rain,
        "weatherAlert":    alert,
        "marketImpact":    impact,
        "dataSourceCount": n_src
    }

# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/api/bulk-predictions")
def bulk_predictions(
    state:       str = Query(default="Rajasthan"),
    city:        str = Query(default="Nagaur"),
    commodities: str = Query(
        default="Wheat,Rice,Mustard,Chana,Moong,Maize,Soybean,"
                "Onion,Potato,Garlic,Groundnut,Barley,Bajra,Cotton,Urad"
    )
):
    comm_list = [c.strip() for c in commodities.split(",") if c.strip()]
    log.info(f"BULK {len(comm_list)} commodities | {state}/{city}")

    is_rain, impact, alert = get_weather(city)
    results = []

    # Process all commodities concurrently (max 5 at a time)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(process_commodity, c, state, city,
                      is_rain, impact, alert): c
            for c in comm_list
        }
        for fut in concurrent.futures.as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                log.error(f"BULK thread error: {e}")

    order = {"VERIFIED":0,"SINGLE_SOURCE":1,"UNVERIFIED":2}
    results.sort(key=lambda x: order.get(x["priceStatus"],3))

    verified = sum(1 for r in results if r["priceStatus"]=="VERIFIED")
    single   = sum(1 for r in results if r["priceStatus"]=="SINGLE_SOURCE")

    log.info(f"BULK DONE: {verified} verified, {single} single, "
             f"{len(results)-verified-single} unverified")

    return {
        "state":          state,
        "market":         city,
        "fetchedAt":      datetime.now().isoformat(),
        "serverVersion":  "4.0.0",
        "totalCount":     len(results),
        "verifiedCount":  verified,
        "singleSourceCount": single,
        "dataSources":    10,
        "weather":        {"isRain":is_rain,"alert":alert,"impact":impact},
        "commodities":    results
    }


@app.get("/api/mandi-predictions")
def single_prediction(
    commodity: str = Query(...),
    state:     str = Query(...),
    city:      str = Query(...)
):
    is_rain, impact, alert = get_weather(city)
    result = process_commodity(commodity, state, city, is_rain, impact, alert)
    # flatten for backward compat
    result.update({
        "locationName":    city,
        "commodityName":   commodity,
        "predictionNote":  "बहु-स्रोत सत्यापित भाव" if result["priceStatus"] != "UNVERIFIED"
                           else "सभी स्रोतों से सत्यापित भाव उपलब्ध नहीं।"
    })
    return result


@app.get("/api/mandipulse/calculate")
def calculate(
    commodity: str = Query(...),
    state:     str = Query(...),
    city:      str = Query(...)
):
    is_rain, impact, alert = get_weather(city)
    r = process_commodity(commodity, state, city, is_rain, impact, alert)
    price = r["currentPrice"]
    if price is None:
        return {"commodityHindi":HINDI.get(commodity,commodity),
                "location":city,"currentPrice":None,"dataAvailable":False,
                "message":"डेटा उपलब्ध नहीं।"}
    prev  = r["yesterdayPrice"]
    msp_v = MSP.get(commodity,0)
    return {
        "commodityHindi": HINDI.get(commodity,commodity),
        "location":       city,
        "currentPrice":   price,
        "dataAvailable":  True,
        "masterScore":    r["bechainIndex"]["score"],
        "masterSignal":   r["bechainIndex"]["signal"],
        "masterHindi":    r["bechainIndex"]["signalHindi"],
        "masterColor":    r["bechainIndex"]["color"],
        "sourcesUsed":    r["sourcesUsed"],
        "dataSourceCount":r["dataSourceCount"],
        "bechainIndex":   r["bechainIndex"],
        "mspCalculator":  {
            "mspValue":    msp_v,
            "currentPrice":price,
            "difference":  round(price-msp_v,2),
            "hindi":       "MSP के ऊपर" if price>msp_v else "MSP से नीचे",
            "color":       "green" if price>msp_v else "red"
        },
        "weatherImpact": {
            "isRainExpected": is_rain,
            "alert":          alert,
            "impactHindi":    impact
        }
    }


@app.get("/api/mandipulse/dashboard")
def dashboard(
    commodity: str = Query(...),
    state:     str = Query(...),
    city:      str = Query(...)
):
    is_rain, impact, alert = get_weather(city)
    r = process_commodity(commodity, state, city, is_rain, impact, alert)
    return {
        "appName":        "MandiPulse 💓",
        "serverVersion":  "4.0.0",
        "dataSources":    10,
        "commodity":      commodity,
        "location":       city,
        "currentPrice":   r["currentPrice"],
        "maxPrice":       r["maxPrice"],
        "minPrice":       r["minPrice"],
        "yesterdayPrice": r["yesterdayPrice"],
        "arrivalQty":     r["arrivalQuantity"],
        "updateDate":     r["updateDate"],
        "priceStatus":    r["priceStatus"],
        "sourcesUsed":    r["sourcesUsed"],
        "bechainIndex":   r["bechainIndex"],
        "fasalCalendar":  {
            "bestMonth":  "मई",
            "bestPrice":  round(r["currentPrice"]*1.15,2) if r["currentPrice"] else None,
            "worstMonth": "जनवरी",
            "sellAdvice": "Hold"
        },
        "weather":        {"isRain":is_rain,"alert":alert,"impact":impact}
    }


@app.get("/api/markets")
def markets(commodity: str = Query(...), state: str = Query(...)):
    m = {
        "Rajasthan":      ["Nagaur","Jodhpur","Jaipur","Merta City","Bikaner","Kota","Barmer","Sikar"],
        "Madhya Pradesh": ["Indore","Bhopal","Ujjain","Sehore","Dewas","Ratlam"],
        "Punjab":         ["Ludhiana","Amritsar","Patiala","Bathinda","Jalandhar"],
        "Haryana":        ["Karnal","Hisar","Rohtak","Sirsa","Ambala","Panipat"],
        "Uttar Pradesh":  ["Agra","Lucknow","Kanpur","Varanasi","Meerut","Hapur"],
        "Gujarat":        ["Unjha","Rajkot","Ahmedabad","Surat","Junagadh","Gondal"],
        "Maharashtra":    ["Pune","Nashik","Nagpur","Solapur","Latur","Lasalgaon"],
        "Karnataka":      ["Bangalore","Hubli","Belgaum","Davangere","Hassan"],
        "Andhra Pradesh": ["Kurnool","Guntur","Nellore","Tirupati","Kadapa"],
    }
    return {"markets": m.get(state, ["Market 1","Market 2","Market 3"])}


@app.get("/api/listings/all")
def listings(is_pro: bool = False):
    return {"listings": []}

@app.post("/api/listings/add")
def add_listing(data: dict):
    return {"status":"success","listingId":"L001"}

@app.get("/health")
def health():
    return {"status":"ok","version":"4.0.0","dataSources":10,
            "time":datetime.now().isoformat()}

@app.get("/")
def root():
    return {
        "status":  "MandiPulse v4.0.0 Online",
        "message": "10 data sources — fake-price-free",
        "sources": [
            "S1:  data.gov.in        (Govt API)     weight=3",
            "S2:  agmarknet.gov.in   (Govt scraper) weight=3",
            "S3:  enam.gov.in        (Nat'l market) weight=2",
            "S4:  farmer.in          (Aggregator)   weight=2",
            "S5:  Krishi Jagran EN   (News RSS)     weight=1",
            "S6:  Krishi Jagran HI   (News RSS)     weight=1",
            "S7:  Kisan Tak          (News RSS)     weight=1",
            "S8:  Gaon Connection    (News RSS)     weight=1",
            "S9:  ABP Kisan          (News RSS)     weight=1",
            "S10: DD Kisan           (Govt TV RSS)  weight=1",
        ]
    }
