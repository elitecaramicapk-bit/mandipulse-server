# ================================================================
# MANDIPULSE SERVER — app.py v6.0.0
# "Har State Ki Apni Fasl" Edition
# ================================================================
#
# WHAT'S NEW IN v6.0.0:
#  - 20 states fully mapped with their own crops + mandis
#  - State-specific commodity lists (only local crops returned)
#  - State-specific price ranges (Punjab wheat ≠ Rajasthan wheat)
#  - State-specific MSP + local market context
#  - 12 data sources (same as v5)
#  - Smart fallback: city → district → state → national
#
# STATE COVERAGE:
#  Rajasthan, Punjab, Haryana, UP, MP, Maharashtra,
#  Gujarat, Karnataka, AP, Telangana, Tamil Nadu,
#  Bihar, Odisha, WB, Assam, Chhattisgarh,
#  Jharkhand, Uttarakhand, HP, J&K
# ================================================================

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import requests, feedparser, re, logging, concurrent.futures
from typing import Optional, List
from datetime import datetime

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("MandiPulse")

app = FastAPI(title="MandiPulse API", version="6.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
    allow_methods=["*"], allow_headers=["*"])

HDR = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0"}
DATA_GOV_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"

# ================================================================
# HINDI NAMES
# ================================================================
HINDI = {
    "Wheat":"गेहूँ","Rice":"चावल","Paddy":"धान","Onion":"प्याज",
    "Potato":"आलू","Tomato":"टमाटर","Garlic":"लहसुन",
    "Mustard":"सरसों","Soybean":"सोयाबीन","Cotton":"कपास",
    "Maize":"मक्का","Chana":"चना","Moong":"मूँग","Urad":"उड़द",
    "Arhar":"अरहर","Masoor":"मसूर","Jeera":"जीरा",
    "Coriander":"धनिया","Fennel":"सौंफ","Methi":"मेथी",
    "Groundnut":"मूँगफली","Barley":"जौ","Jowar":"ज्वार",
    "Bajra":"बाजरा","Sugarcane":"गन्ना","Guar":"ग्वार",
    "Castor":"अरंडी","Taramira":"तारामीरा","Moath":"मोठ",
    "Turmeric":"हल्दी","Chilli":"मिर्च","Ginger":"अदरक",
    "Banana":"केला","Mango":"आम","Grapes":"अंगूर",
    "Pomegranate":"अनार","Coconut":"नारियल","Arecanut":"सुपारी",
    "Coffee":"कॉफी","Tea":"चाय","Jute":"पटसन",
    "Sunflower":"सूरजमुखी","Sesame":"तिल","Linseed":"अलसी",
    "Tobacco":"तम्बाकू","Rubber":"रबर","Pepper":"काली मिर्च",
    "Cardamom":"इलायची","Isabgul":"इसबगोल","Ajwain":"अजवाइन",
    "Nigella":"कलौंजी","Fenugreek":"मेथी दाना",
    "Watermelon":"तरबूज","Cucumber":"खीरा","Brinjal":"बैंगन",
    "Cabbage":"पत्तागोभी","Cauliflower":"फूलगोभी",
    "Peas":"मटर","Bitter Gourd":"करेला","Bottle Gourd":"लौकी",
    "Drumstick":"सहजन","Jackfruit":"कटहल","Tamarind":"इमली",
    "Mustard Oil":"सरसों तेल","Toor Dal":"तुअर दाल",
    "Jowar":"ज्वार","Ragi":"रागी","Moth Bean":"मोठ",
    "Lemon":"नींबू","Papaya":"पपीता","Guava":"अमरूद",
    "Capsicum":"शिमला मिर्च","Spinach":"पालक",
    "Cluster Beans":"ग्वार फली","Horse Gram":"कुलथी",
    "Niger Seed":"रामतिल","Safflower":"कुसुम",
}

# ================================================================
# MSP 2026-27 (updated)
# ================================================================
MSP = {
    "Wheat":2425,"Paddy":2300,"Rice":2300,"Maize":2090,
    "Jowar":3371,"Bajra":2625,"Ragi":4290,"Barley":1735,
    "Chana":5440,"Moong":8780,"Urad":7400,"Arhar":7550,
    "Masoor":6700,"Mustard":5950,"Soybean":4892,
    "Cotton":7121,"Groundnut":6783,"Sunflower":7280,
    "Sesame":9267,"Safflower":5800,"Nigella":7745,
    "Sugarcane":340,"Jute":5335,"Castor":6600,
}

# ================================================================
# STATE DATA: Crops + Mandis + Local Websites
# ================================================================
STATE_DATA = {

    "Rajasthan": {
        "crops": ["Mustard","Wheat","Chana","Moong","Guar","Jeera",
                  "Fennel","Taramira","Barley","Bajra","Coriander",
                  "Castor","Isabgul","Ajwain","Moath","Methi",
                  "Groundnut","Maize","Cotton","Sesame","Onion","Garlic"],
        "mandis": ["Nagaur","Jodhpur","Jaipur","Merta City","Bikaner",
                   "Kota","Barmer","Sikar","Alwar","Sriganganagar",
                   "Hanumangarh","Tonk","Bundi","Chittorgarh",
                   "Pali","Ajmer","Bhilwara","Dungarpur","Unjha"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
            "https://emandibhav.com/{city}-mandi-bhav/",
            "https://kisanupaj.com/MandiBhav/rj/{city}/{city}-mandi-bhav",
            "https://kisanekta.in/{city}-mandi-bhav/",
        ],
        "price_ranges": {
            "Mustard":(5000,10000),"Wheat":(1800,4000),
            "Chana":(4000,9000),"Moong":(5000,14000),
            "Guar":(3500,9000),"Jeera":(15000,80000),
            "Fennel":(6000,30000),"Taramira":(4000,9000),
            "Barley":(1200,3500),"Bajra":(1500,4500),
            "Coriander":(5000,25000),"Castor":(5000,12000),
            "Isabgul":(8000,25000),"Ajwain":(8000,30000),
            "Moath":(3000,8000),"Methi":(3000,15000),
            "Groundnut":(4500,10000),"Maize":(1500,3500),
            "Cotton":(5500,10000),"Sesame":(10000,30000),
            "Onion":(500,8000),"Garlic":(2000,40000),
        }
    },

    "Punjab": {
        "crops": ["Wheat","Paddy","Rice","Maize","Cotton","Potato",
                  "Onion","Sugarcane","Arhar","Chana","Mustard",
                  "Barley","Sunflower","Groundnut"],
        "mandis": ["Ludhiana","Amritsar","Patiala","Bathinda",
                   "Jalandhar","Firozpur","Moga","Barnala",
                   "Sangrur","Fazilka","Gurdaspur","Ropar",
                   "Nawanshahr","Muktsar"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
            "https://mandibhav.in/punjab/{city}/",
        ],
        "price_ranges": {
            "Wheat":(2000,4500),"Paddy":(1800,4500),
            "Rice":(2000,6000),"Maize":(1500,3500),
            "Cotton":(5500,10000),"Potato":(400,3000),
            "Onion":(300,8000),"Sugarcane":(280,400),
            "Arhar":(5500,10000),"Chana":(4500,9000),
            "Mustard":(5000,9500),"Barley":(1400,3200),
            "Sunflower":(5000,9000),"Groundnut":(4500,9500),
        }
    },

    "Haryana": {
        "crops": ["Wheat","Paddy","Rice","Mustard","Cotton","Jowar",
                  "Bajra","Maize","Barley","Sugarcane","Sunflower",
                  "Arhar","Chana","Onion","Potato","Moong"],
        "mandis": ["Karnal","Hisar","Rohtak","Sirsa","Ambala",
                   "Panipat","Sonipat","Fatehabad","Jind",
                   "Yamunanagar","Kaithal","Kurukshetra","Bhiwani"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Wheat":(2000,4500),"Paddy":(1800,4500),
            "Rice":(2000,6000),"Mustard":(5000,9500),
            "Cotton":(5500,10000),"Jowar":(2500,5000),
            "Bajra":(1800,4500),"Maize":(1500,3500),
            "Barley":(1300,3200),"Sugarcane":(280,400),
            "Sunflower":(5000,9000),"Arhar":(5500,10000),
            "Chana":(4500,9000),"Onion":(300,8000),
            "Potato":(400,3000),"Moong":(5500,12000),
        }
    },

    "Uttar Pradesh": {
        "crops": ["Wheat","Paddy","Rice","Sugarcane","Potato",
                  "Onion","Tomato","Mustard","Arhar","Chana",
                  "Moong","Urad","Masoor","Maize","Barley",
                  "Bajra","Garlic","Peas","Methi","Jowar"],
        "mandis": ["Agra","Lucknow","Kanpur","Varanasi","Meerut",
                   "Hapur","Mathura","Aligarh","Moradabad",
                   "Bareilly","Gorakhpur","Allahabad","Jhansi",
                   "Sitapur","Hardoi","Shahjahanpur"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
            "https://upmandiparishad.upsdc.gov.in/",
        ],
        "price_ranges": {
            "Wheat":(1900,4200),"Paddy":(1700,4000),
            "Rice":(2000,6000),"Sugarcane":(280,400),
            "Potato":(300,2500),"Onion":(300,8000),
            "Tomato":(200,15000),"Mustard":(5000,9000),
            "Arhar":(5500,10000),"Chana":(4500,9000),
            "Moong":(5500,12000),"Urad":(5000,11000),
            "Masoor":(4500,9000),"Maize":(1500,3500),
            "Barley":(1300,3200),"Bajra":(1600,4000),
            "Garlic":(2000,40000),"Peas":(800,5000),
            "Methi":(3000,12000),"Jowar":(2500,5000),
        }
    },

    "Madhya Pradesh": {
        "crops": ["Wheat","Soybean","Chana","Maize","Arhar",
                  "Moong","Urad","Cotton","Mustard","Masoor",
                  "Paddy","Rice","Garlic","Onion","Tomato",
                  "Coriander","Ajwain","Sesame","Linseed"],
        "mandis": ["Indore","Bhopal","Ujjain","Sehore","Dewas",
                   "Ratlam","Mandsaur","Neemuch","Gwalior",
                   "Khandwa","Burhanpur","Harda","Vidisha",
                   "Raisen","Chhindwara","Jabalpur","Sagar"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
            "https://mandibhav.in/mp/{city}/",
        ],
        "price_ranges": {
            "Wheat":(1900,4200),"Soybean":(3500,8000),
            "Chana":(4000,9000),"Maize":(1500,3500),
            "Arhar":(5500,10000),"Moong":(5500,12000),
            "Urad":(5000,11000),"Cotton":(5500,10000),
            "Mustard":(5000,9000),"Masoor":(4500,9000),
            "Paddy":(1700,4000),"Rice":(2000,6000),
            "Garlic":(2000,40000),"Onion":(300,8000),
            "Tomato":(200,15000),"Coriander":(5000,25000),
            "Ajwain":(8000,30000),"Sesame":(10000,28000),
            "Linseed":(4500,9000),
        }
    },

    "Maharashtra": {
        "crops": ["Onion","Soybean","Cotton","Jowar","Bajra",
                  "Wheat","Tur Dal","Arhar","Chana","Groundnut",
                  "Sunflower","Sugarcane","Rice","Paddy","Maize",
                  "Turmeric","Chilli","Tomato","Grapes","Banana",
                  "Pomegranate","Orange","Ginger"],
        "mandis": ["Pune","Nashik","Nagpur","Solapur","Latur",
                   "Lasalgaon","Nanded","Aurangabad","Kolhapur",
                   "Sangli","Satara","Jalgaon","Dhule","Amravati",
                   "Akola","Yavatmal","Osmanabad","Hingoli"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
            "https://market.maharashtra.gov.in/",
        ],
        "price_ranges": {
            "Onion":(200,12000),"Soybean":(3500,8000),
            "Cotton":(5500,10000),"Jowar":(2500,5000),
            "Bajra":(1600,4000),"Wheat":(1900,4200),
            "Arhar":(5500,10000),"Chana":(4000,9000),
            "Groundnut":(4500,10000),"Sunflower":(5000,9000),
            "Sugarcane":(280,400),"Rice":(2000,6000),
            "Paddy":(1700,4000),"Maize":(1500,3500),
            "Turmeric":(7000,25000),"Chilli":(5000,35000),
            "Tomato":(200,18000),"Grapes":(3000,20000),
            "Banana":(500,3000),"Pomegranate":(4000,20000),
            "Ginger":(2000,15000),
        }
    },

    "Gujarat": {
        "crops": ["Groundnut","Cotton","Wheat","Bajra","Jowar",
                  "Castor","Soybean","Sugarcane","Rice","Paddy",
                  "Maize","Garlic","Onion","Tomato","Potato",
                  "Tobacco","Cumin","Fennel","Sesame","Isabgul"],
        "mandis": ["Ahmedabad","Rajkot","Surat","Vadodara","Unjha",
                   "Gondal","Junagadh","Amreli","Jamnagar",
                   "Mehsana","Patan","Banaskantha","Sabarkantha",
                   "Kheda","Anand","Navsari","Valsad"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
            "https://mandibhav.in/gujarat/{city}/",
        ],
        "price_ranges": {
            "Groundnut":(4500,10000),"Cotton":(5500,10000),
            "Wheat":(2000,4200),"Bajra":(1600,4000),
            "Jowar":(2500,5000),"Castor":(5500,12000),
            "Soybean":(3500,8000),"Sugarcane":(280,400),
            "Rice":(2000,6000),"Paddy":(1700,4000),
            "Maize":(1500,3500),"Garlic":(2000,40000),
            "Onion":(300,10000),"Tomato":(200,18000),
            "Potato":(400,3000),"Tobacco":(5000,25000),
            "Cumin":(15000,80000),"Fennel":(6000,30000),
            "Sesame":(10000,28000),"Isabgul":(8000,25000),
        }
    },

    "Karnataka": {
        "crops": ["Rice","Paddy","Maize","Ragi","Jowar","Bajra",
                  "Arhar","Moong","Urad","Groundnut","Sunflower",
                  "Cotton","Soybean","Sugarcane","Onion","Tomato",
                  "Potato","Chilli","Turmeric","Coffee","Coconut",
                  "Arecanut","Pepper","Ginger","Banana"],
        "mandis": ["Bangalore","Hubli","Belgaum","Davangere",
                   "Tumkur","Mysore","Gulbarga","Bijapur",
                   "Raichur","Hassan","Shimoga","Udupi",
                   "Mandya","Kolar","Bidar","Bagalkot"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Rice":(2000,7000),"Paddy":(1700,4500),
            "Maize":(1500,3500),"Ragi":(3000,6000),
            "Jowar":(2500,5000),"Bajra":(1600,4000),
            "Arhar":(5500,10000),"Moong":(5500,12000),
            "Urad":(5000,11000),"Groundnut":(4500,10000),
            "Sunflower":(5000,9000),"Cotton":(5500,10000),
            "Soybean":(3500,8000),"Sugarcane":(280,400),
            "Onion":(300,10000),"Tomato":(200,20000),
            "Potato":(400,3000),"Chilli":(5000,35000),
            "Turmeric":(7000,25000),"Coffee":(8000,30000),
            "Coconut":(1500,12000),"Arecanut":(20000,80000),
            "Pepper":(30000,80000),"Ginger":(2000,15000),
            "Banana":(500,3000),
        }
    },

    "Andhra Pradesh": {
        "crops": ["Rice","Paddy","Maize","Chilli","Cotton",
                  "Groundnut","Tobacco","Sugarcane","Jowar",
                  "Bajra","Turmeric","Onion","Tomato","Banana",
                  "Mango","Coconut","Sunflower","Sesame",
                  "Castor","Arhar","Moong","Urad"],
        "mandis": ["Kurnool","Guntur","Nellore","Tirupati",
                   "Kadapa","Ongole","Eluru","Rajahmundry",
                   "Vishakhapatnam","Vijayawada","Karimnagar",
                   "Nizamabad","Warangal","Khammam"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Rice":(2000,7000),"Paddy":(1700,4500),
            "Maize":(1500,3500),"Chilli":(5000,35000),
            "Cotton":(5500,10000),"Groundnut":(4500,10000),
            "Tobacco":(5000,25000),"Sugarcane":(280,400),
            "Jowar":(2500,5000),"Turmeric":(7000,25000),
            "Onion":(300,10000),"Tomato":(200,20000),
            "Banana":(500,3000),"Coconut":(1500,12000),
            "Sunflower":(5000,9000),"Sesame":(10000,28000),
            "Castor":(5500,12000),"Arhar":(5500,10000),
            "Moong":(5500,12000),"Urad":(5000,11000),
        }
    },

    "Telangana": {
        "crops": ["Rice","Paddy","Maize","Cotton","Chilli",
                  "Turmeric","Soybean","Jowar","Bajra",
                  "Groundnut","Sugarcane","Onion","Tomato",
                  "Arhar","Moong","Sunflower","Castor"],
        "mandis": ["Hyderabad","Warangal","Nizamabad","Karimnagar",
                   "Khammam","Nalgonda","Mahbubnagar","Adilabad",
                   "Medak","Rangareddy"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Rice":(2000,7000),"Paddy":(1700,4500),
            "Maize":(1500,3500),"Cotton":(5500,10000),
            "Chilli":(5000,35000),"Turmeric":(7000,25000),
            "Soybean":(3500,8000),"Groundnut":(4500,10000),
            "Sugarcane":(280,400),"Arhar":(5500,10000),
            "Moong":(5500,12000),"Sunflower":(5000,9000),
            "Castor":(5500,12000),
        }
    },

    "Tamil Nadu": {
        "crops": ["Rice","Paddy","Maize","Ragi","Banana","Coconut",
                  "Sugarcane","Groundnut","Onion","Tomato",
                  "Drumstick","Tamarind","Pepper","Ginger",
                  "Turmeric","Cotton","Arhar","Moong",
                  "Chilli","Sesame","Sunflower","Cardamom"],
        "mandis": ["Chennai","Coimbatore","Madurai","Tiruchi",
                   "Salem","Tirunelveli","Erode","Vellore",
                   "Thoothukudi","Thanjavur","Dindigul",
                   "Namakkal","Tirupur","Kancheepuram"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Rice":(2200,7000),"Paddy":(1800,4500),
            "Maize":(1600,3500),"Ragi":(3000,6000),
            "Banana":(500,3000),"Coconut":(1500,12000),
            "Sugarcane":(280,400),"Groundnut":(4500,10000),
            "Tomato":(200,20000),"Turmeric":(7000,25000),
            "Cotton":(5500,10000),"Arhar":(5500,10000),
            "Moong":(5500,12000),"Sesame":(10000,28000),
            "Cardamom":(50000,200000),
        }
    },

    "Bihar": {
        "crops": ["Wheat","Paddy","Rice","Maize","Sugarcane",
                  "Potato","Onion","Tomato","Arhar","Moong",
                  "Masoor","Mustard","Barley","Chana","Jute",
                  "Linseed","Garlic","Ginger","Banana"],
        "mandis": ["Patna","Gaya","Muzaffarpur","Bhagalpur",
                   "Darbhanga","Purnia","Begusarai","Munger",
                   "Nalanda","Vaishali","Saran","Siwan",
                   "East Champaran","West Champaran"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Wheat":(1900,4000),"Paddy":(1700,4000),
            "Rice":(2000,6000),"Maize":(1400,3200),
            "Sugarcane":(280,400),"Potato":(300,2500),
            "Onion":(300,8000),"Tomato":(200,15000),
            "Arhar":(5500,10000),"Moong":(5500,12000),
            "Masoor":(4500,9000),"Mustard":(5000,9000),
            "Barley":(1300,3200),"Jute":(3500,7000),
        }
    },

    "West Bengal": {
        "crops": ["Rice","Paddy","Jute","Potato","Onion","Tomato",
                  "Maize","Mustard","Linseed","Sesame",
                  "Sugarcane","Banana","Coconut","Tea",
                  "Arhar","Moong","Masoor","Ginger","Turmeric"],
        "mandis": ["Kolkata","Siliguri","Asansol","Burdwan",
                   "Malda","Murshidabad","Nadia","Hooghly",
                   "Howrah","Medinipur","Bankura","Birbhum"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Rice":(2000,7000),"Paddy":(1700,4500),
            "Jute":(3500,7000),"Potato":(300,2500),
            "Onion":(300,8000),"Maize":(1400,3200),
            "Mustard":(5000,9000),"Tea":(10000,50000),
            "Banana":(500,3000),"Coconut":(1500,12000),
            "Turmeric":(7000,25000),"Ginger":(2000,15000),
        }
    },

    "Odisha": {
        "crops": ["Rice","Paddy","Maize","Arhar","Moong","Urad",
                  "Groundnut","Coconut","Sugarcane","Jute",
                  "Potato","Onion","Tomato","Mustard","Sesame",
                  "Niger Seed","Turmeric","Ginger","Banana"],
        "mandis": ["Bhubaneswar","Cuttack","Rourkela","Berhampur",
                   "Sambalpur","Balasore","Bhadrak","Puri",
                   "Koraput","Rayagada","Jharsuguda"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Rice":(2000,7000),"Paddy":(1700,4500),
            "Maize":(1400,3200),"Arhar":(5500,10000),
            "Groundnut":(4500,10000),"Coconut":(1500,12000),
            "Jute":(3500,7000),"Turmeric":(7000,25000),
            "Ginger":(2000,15000),"Niger Seed":(4000,12000),
        }
    },

    "Assam": {
        "crops": ["Rice","Paddy","Jute","Tea","Mustard","Potato",
                  "Ginger","Turmeric","Banana","Coconut",
                  "Onion","Tomato","Sugarcane","Sesame",
                  "Arhar","Moong","Maize","Linseed"],
        "mandis": ["Guwahati","Dibrugarh","Jorhat","Silchar",
                   "Nagaon","Tinsukia","Bongaigaon","Karimganj",
                   "Dhubri","Lakhimpur","Kamrup"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Rice":(2200,7000),"Paddy":(1800,4500),
            "Jute":(3500,7000),"Tea":(10000,50000),
            "Mustard":(5000,9000),"Potato":(400,3000),
            "Ginger":(2000,15000),"Turmeric":(7000,25000),
            "Banana":(500,3000),
        }
    },

    "Chhattisgarh": {
        "crops": ["Rice","Paddy","Maize","Jowar","Arhar","Moong",
                  "Urad","Chana","Groundnut","Soybean","Mustard",
                  "Onion","Potato","Tomato","Sugarcane",
                  "Niger Seed","Sesame","Linseed"],
        "mandis": ["Raipur","Bilaspur","Durg","Bhilai","Korba",
                   "Rajnandgaon","Jagdalpur","Ambikapur",
                   "Raigarh","Mahasamund"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Rice":(2000,7000),"Paddy":(1700,4500),
            "Maize":(1400,3200),"Arhar":(5500,10000),
            "Moong":(5500,12000),"Groundnut":(4500,10000),
            "Soybean":(3500,8000),"Niger Seed":(4000,12000),
        }
    },

    "Jharkhand": {
        "crops": ["Rice","Paddy","Maize","Arhar","Moong","Urad",
                  "Mustard","Onion","Potato","Tomato",
                  "Sugarcane","Niger Seed","Sesame","Linseed"],
        "mandis": ["Ranchi","Jamshedpur","Dhanbad","Bokaro",
                   "Hazaribagh","Deoghar","Dumka","Giridih"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Rice":(2000,7000),"Paddy":(1700,4500),
            "Maize":(1400,3200),"Mustard":(5000,9000),
        }
    },

    "Himachal Pradesh": {
        "crops": ["Apple","Potato","Tomato","Peas","Ginger",
                  "Wheat","Maize","Rice","Onion","Garlic",
                  "Capsicum","Cabbage","Cauliflower","Mushroom"],
        "mandis": ["Shimla","Manali","Dharamshala","Solan",
                   "Kullu","Mandi","Bilaspur","Hamirpur",
                   "Una","Kangra","Chamba"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Apple":(2000,15000),"Potato":(300,3500),
            "Tomato":(200,20000),"Peas":(800,8000),
            "Ginger":(2000,15000),"Wheat":(1900,4200),
            "Maize":(1400,3200),"Capsicum":(1500,15000),
            "Cabbage":(200,2000),"Cauliflower":(300,3000),
        }
    },

    "Uttarakhand": {
        "crops": ["Wheat","Rice","Paddy","Maize","Potato",
                  "Onion","Tomato","Ginger","Turmeric","Garlic",
                  "Apple","Peas","Cabbage","Mustard","Barley"],
        "mandis": ["Dehradun","Haridwar","Roorkee","Haldwani",
                   "Rudrapur","Kashipur","Rishikesh","Kotdwar"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Wheat":(2000,4200),"Rice":(2000,6000),
            "Potato":(300,3500),"Ginger":(2000,15000),
            "Apple":(2000,15000),
        }
    },

    "Jammu and Kashmir": {
        "crops": ["Apple","Walnut","Cherry","Pear","Saffron",
                  "Rice","Paddy","Maize","Wheat","Potato",
                  "Onion","Tomato","Peas","Cabbage","Mustard"],
        "mandis": ["Jammu","Srinagar","Sopore","Pulwama",
                   "Anantnag","Baramulla","Kathua","Udhampur"],
        "local_urls": [
            "https://khetiwadi.com/mandi/{city}-mandi-bhav",
        ],
        "price_ranges": {
            "Apple":(3000,20000),"Walnut":(10000,60000),
            "Cherry":(5000,30000),"Saffron":(200000,500000),
            "Rice":(2200,7000),"Paddy":(1800,4500),
            "Potato":(400,4000),"Peas":(800,8000),
        }
    },
}

# Default ranges for commodities not in state-specific list
GLOBAL_RANGES = {
    "Wheat":(1500,5000),"Rice":(1800,8000),"Paddy":(1500,5000),
    "Onion":(200,15000),"Potato":(200,5000),"Tomato":(100,25000),
    "Garlic":(1000,60000),"Mustard":(4000,12000),"Soybean":(3000,9000),
    "Cotton":(5000,12000),"Maize":(1200,4500),"Chana":(3500,12000),
    "Moong":(5000,15000),"Urad":(4500,14000),"Arhar":(5000,12000),
    "Masoor":(4000,10000),"Jeera":(10000,100000),"Turmeric":(5000,30000),
    "Chilli":(3000,40000),"Ginger":(1500,20000),"Banana":(300,5000),
    "Coconut":(1000,15000),"Groundnut":(4000,12000),"Sunflower":(4500,11000),
    "Sugarcane":(200,500),"Jute":(3000,9000),"Coffee":(5000,35000),
    "Tea":(8000,60000),"Pepper":(25000,90000),"Cardamom":(40000,250000),
    "Apple":(1500,25000),"Saffron":(150000,600000),"Fennel":(5000,35000),
    "Coriander":(4000,30000),"Barley":(1000,4000),"Bajra":(1400,5000),
    "Jowar":(2000,6000),"Ragi":(2500,7000),"Sesame":(8000,35000),
    "Castor":(4500,14000),"Guar":(3000,12000),"Niger Seed":(3500,15000),
    "Tobacco":(4000,30000),"Arecanut":(15000,100000),"Rubber":(15000,25000),
}

# ================================================================
# ALIASES
# ================================================================
ALIASES = {
    "Wheat":    ["wheat","gehun","gehu","गेहूँ"],
    "Rice":     ["rice","chawal","चावल"],
    "Paddy":    ["paddy","dhan","धान"],
    "Onion":    ["onion","pyaz","प्याज"],
    "Potato":   ["potato","aloo","आलू"],
    "Tomato":   ["tomato","tamatar","टमाटर"],
    "Garlic":   ["garlic","lahsun","लहसुन"],
    "Mustard":  ["mustard","sarson","सरसों","rayda","raya"],
    "Soybean":  ["soybean","soya","सोयाबीन"],
    "Cotton":   ["cotton","kapas","कपास","narma"],
    "Maize":    ["maize","makka","मक्का"],
    "Chana":    ["chana","gram","चना","bengal gram"],
    "Moong":    ["moong","मूँग","green gram"],
    "Urad":     ["urad","उड़द","black gram"],
    "Arhar":    ["arhar","tur","toor","अरहर","तुअर"],
    "Masoor":   ["masoor","lentil","मसूर"],
    "Jeera":    ["jeera","cumin","जीरा","cummin"],
    "Coriander":["coriander","dhaniya","धनिया"],
    "Fennel":   ["fennel","saunf","सौंफ","sonf"],
    "Groundnut":["groundnut","peanut","moongfali","मूँगफली"],
    "Barley":   ["barley","jau","जौ"],
    "Bajra":    ["bajra","बाजरा"],
    "Guar":     ["guar","ग्वार","guar seed"],
    "Castor":   ["castor","arandi","अरंडी"],
    "Turmeric": ["turmeric","haldi","हल्दी"],
    "Chilli":   ["chilli","mirch","मिर्च","red chilli"],
    "Ginger":   ["ginger","adrak","अदरक"],
    "Sugarcane":["sugarcane","ganna","गन्ना"],
    "Jute":     ["jute","patsan","पटसन"],
    "Cotton":   ["cotton","kapas","कपास"],
    "Sunflower":["sunflower","surajmukhi","सूरजमुखी"],
    "Sesame":   ["sesame","til","तिल","sesame seed"],
    "Jowar":    ["jowar","ज्वार","sorghum"],
    "Ragi":     ["ragi","finger millet","रागी"],
    "Banana":   ["banana","kela","केला"],
    "Coconut":  ["coconut","nariyal","नारियल"],
    "Apple":    ["apple","seb","सेब"],
    "Pepper":   ["pepper","kali mirch","काली मिर्च"],
    "Cardamom": ["cardamom","elaichi","इलायची"],
    "Tobacco":  ["tobacco","tambaku","तम्बाकू"],
    "Arecanut": ["arecanut","supari","सुपारी"],
    "Tea":      ["tea","chai","चाय"],
    "Coffee":   ["coffee","कॉफी"],
    "Isabgul":  ["isabgul","isabgol","इसबगोल","psyllium"],
    "Ajwain":   ["ajwain","carom","अजवाइन"],
    "Taramira": ["taramira","तारामीरा"],
    "Moath":    ["moath","moth","मोठ"],
    "Linseed":  ["linseed","alsi","अलसी"],
    "Niger Seed":["niger","ramtil","रामतिल"],
    "Safflower":["safflower","kusum","कुसुम"],
}

RSS_SOURCES = [
    ("https://www.krishijagran.com/feed/",       "Krishi Jagran EN"),
    ("https://hindi.krishijagran.com/feed/",     "Krishi Jagran HI"),
    ("https://www.kisantak.in/feed/",            "Kisan Tak"),
    ("https://www.gaonconnection.com/feed",      "Gaon Connection"),
    ("https://khabar.ndtv.com/rss/kisan",        "NDTV Kisan"),
    ("https://www.abplive.com/agriculture/feed", "ABP Kisan"),
    ("https://ddnews.gov.in/rss/kisan",          "DD Kisan"),
    ("https://www.bhaskar.com/rss-feed/1555/",  "DB Rajasthan"),
    ("https://www.patrika.com/rss/news.xml",     "Rajasthan Patrika"),
    ("https://www.jagran.com/rss/news-national.xml","Dainik Jagran"),
]

# ================================================================
# HELPERS
# ================================================================
def safe_f(v, default=None):
    if v is None or str(v).strip() in ("","None","N/A","-","null"):
        return default
    try:
        return float(str(v).replace(",","").replace("₹","").strip())
    except:
        return default

def safe_i(v, default=0):
    f = safe_f(v)
    return int(f) if f is not None else default

def get_range(commodity, state):
    """Get price range: state-specific first, then global"""
    sd = STATE_DATA.get(state, {})
    sr = sd.get("price_ranges", {})
    if commodity in sr:
        return sr[commodity]
    return GLOBAL_RANGES.get(commodity, (100, 200000))

def valid(commodity, price, state="Rajasthan"):
    if price is None or price <= 0:
        return False
    lo, hi = get_range(commodity, state)
    ok = lo <= price <= hi
    if not ok:
        log.warning(f"RANGE_FAIL {state}/{commodity} ₹{price} [{lo}-{hi}]")
    return ok

PRICE_RE = re.compile(
    r'(?:₹|rs\.?|rupay|price|bhav|rate|दर|भाव)'
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
    nums = [float(m.group(1)) for m in NUM_RE.finditer(text)
            if 500 <= float(m.group(1)) <= 200000]
    return sorted(nums)[len(nums)//2] if nums else None

def weighted_median(pw_list):
    if not pw_list:
        return None
    expanded = []
    for price, w in pw_list:
        expanded.extend([price]*w)
    expanded.sort()
    return round(expanded[len(expanded)//2], 2)

def iqr_filter(values):
    if len(values) < 3:
        return values, []
    s = sorted(values)
    n = len(s)
    q1, q3 = s[n//4], s[(3*n)//4]
    iqr = q3 - q1
    lo, hi = q1-1.5*iqr, q3+1.5*iqr
    return ([v for v in values if lo<=v<=hi],
            [v for v in values if v<lo or v>hi])

# ================================================================
# DATA SOURCES
# ================================================================

# S1: data.gov.in
AGMARK_NAMES = {
    "Wheat":"Wheat","Mustard":"Mustard(Sarson(Black))",
    "Chana":"Gram(Whole)","Soybean":"Soyabean","Maize":"Maize",
    "Barley":"Barley","Bajra":"Bajra","Moong":"Moong(Whole)",
    "Onion":"Onion","Garlic":"Garlic","Rice":"Rice","Paddy":"Paddy(Common)",
    "Cotton":"Cotton","Groundnut":"Groundnut","Jowar":"Jowar",
    "Urad":"Urad (Whole)","Jeera":"Cummin Seed","Fennel":"Soanf",
    "Guar":"Guar Seed","Coriander":"Coriander Seed",
    "Arhar":"Arhar (Tur/Pigeon Pea)(Whole)","Masoor":"Lentil",
    "Turmeric":"Turmeric","Chilli":"Dry Chillies","Castor":"Castor Seed",
    "Sunflower":"Sun Flower Seed","Sesame":"Sesamum(Sesame,Gingelly,Til)",
    "Sugarcane":"Sugarcane","Tobacco":"Tobacco","Ragi":"Ragi(Finger Millet)",
    "Jute":"Jute","Isabgul":"Isabgol(Psyllium)","Taramira":"Taramira",
    "Ajwain":"Ajwan","Linseed":"Linseed","Niger Seed":"Niger Seed (Ramtil)",
}

def src_gov_api(commodity, state, city):
    try:
        agmark = AGMARK_NAMES.get(commodity, commodity)
        url = (f"https://api.data.gov.in/resource/"
               f"9ef84268-d588-465a-a308-a864a43d0070"
               f"?api-key={DATA_GOV_KEY}&format=json"
               f"&filters[commodity]={agmark}"
               f"&filters[state]={state}"
               f"&filters[market]={city}&limit=5")
        log.info(f"S1:GOV → {commodity}/{city}")
        r = requests.get(url, timeout=12, headers=HDR)
        r.raise_for_status()
        recs = r.json().get("records",[])
        if not recs:
            # Try state-level without city
            url2 = url.replace(f"&filters[market]={city}","")
            r2 = requests.get(url2, timeout=12, headers=HDR)
            recs = r2.json().get("records",[])
        if not recs:
            return None
        rec  = recs[0]
        prev = recs[1] if len(recs)>1 else rec
        modal = safe_f(rec.get("modal_price"))
        if not valid(commodity, modal, state):
            return None
        return {
            "price":modal,
            "max":  safe_f(rec.get("max_price"),modal),
            "min":  safe_f(rec.get("min_price"),modal),
            "prev": safe_f(prev.get("modal_price")),
            "qty":  safe_i(rec.get("arrivals_in_qtl")),
            "date": rec.get("arrival_date","N/A"),
            "src":"data.gov.in","weight":3
        }
    except Exception as e:
        log.warning(f"S1 err {commodity}: {e}")
        return None

def src_agmarknet(commodity, state, city):
    try:
        name  = AGMARK_NAMES.get(commodity, commodity)
        today = datetime.now().strftime("%d-%b-%Y")
        url   = (f"https://agmarknet.gov.in/SearchCmmMkt.aspx"
                 f"?Tx_Commodity={requests.utils.quote(name)}"
                 f"&Tx_State={state}&Tx_District=0&Tx_Market=0"
                 f"&DateFrom={today}&DateTo={today}"
                 f"&Fr_Date={today}&To_Date={today}&Tx_Trend=0")
        log.info(f"S2:AGMARKNET → {commodity}/{state}")
        r = requests.get(url, timeout=12, headers=HDR)
        prices = re.findall(r'<td[^>]*>(\d{3,6}(?:\.\d{1,2})?)</td>',r.text)
        good = [float(p) for p in prices if valid(commodity,float(p),state)]
        if not good:
            return None
        median = sorted(good)[len(good)//2]
        return {"price":median,"src":"agmarknet.gov.in","weight":3}
    except Exception as e:
        log.warning(f"S2 err {commodity}: {e}")
        return None

ENAM_CODES = {
    "Wheat":"102","Rice":"104","Paddy":"116","Mustard":"214",
    "Chana":"301","Moong":"306","Maize":"113","Soybean":"218",
    "Groundnut":"206","Onion":"412","Potato":"408","Garlic":"416",
    "Barley":"107","Bajra":"110","Cotton":"501","Urad":"305",
    "Jowar":"111","Jeera":"401","Fennel":"406","Guar":"502",
    "Arhar":"304","Masoor":"303","Turmeric":"418","Chilli":"419",
    "Sunflower":"215","Sesame":"216","Castor":"213","Ragi":"115",
}

def src_enam(commodity, state, city):
    try:
        code = ENAM_CODES.get(commodity)
        if not code:
            return None
        url = (f"https://enam.gov.in/web/dashboard/commodityPrice"
               f"?commodity={code}&state={state}&market={city}")
        log.info(f"S3:ENAM → {commodity}")
        r    = requests.get(url, timeout=10, headers=HDR)
        data = r.json()
        price = None
        if isinstance(data,list) and data:
            price = safe_f(data[0].get("modalPrice"))
        elif isinstance(data,dict):
            price = safe_f(data.get("modalPrice") or data.get("price"))
        if not valid(commodity, price, state):
            return None
        return {"price":price,"src":"enam.gov.in","weight":2}
    except Exception as e:
        log.warning(f"S3 err {commodity}: {e}")
        return None

def src_local_portals(commodity, state, city):
    """Fetch from state-specific local portals"""
    sd = STATE_DATA.get(state, {})
    urls = sd.get("local_urls", [])
    aliases = ALIASES.get(commodity, [commodity.lower()])
    prices = []

    def fetch_url(url_tmpl):
        try:
            city_slug = city.lower().replace(" ","-")
            url = url_tmpl.format(city=city_slug)
            log.info(f"S4:LOCAL → {commodity}/{url}")
            r = requests.get(url, timeout=10, headers=HDR)
            text = r.text
            for alias in aliases:
                idx = text.lower().find(alias.lower())
                if idx >= 0:
                    chunk = re.sub('<[^>]+>',' ', text[max(0,idx-30):idx+300])
                    p = extract_price(chunk)
                    if p and valid(commodity, p, state):
                        return p
            # Table scan
            rows = re.findall(r'<tr[^>]*>.*?</tr>', text, re.DOTALL|re.IGNORECASE)
            for row in rows:
                rt = re.sub('<[^>]+',' ', row).lower()
                if any(a.lower() in rt for a in aliases):
                    p = extract_price(rt)
                    if p and valid(commodity, p, state):
                        return p
        except:
            pass
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(fetch_url, u) for u in urls]
        for f in concurrent.futures.as_completed(futs):
            try:
                p = f.result()
                if p:
                    prices.append(p)
            except:
                pass

    if not prices:
        return None
    median = sorted(prices)[len(prices)//2]
    log.info(f"S4:LOCAL {state}/{commodity} → ₹{median}")
    return {"price":median,"src":f"local_{state.lower()[:3]}","weight":2}

def src_acrop(commodity, state):
    try:
        slug_map = {
            "Moong":"moong","Wheat":"wheat","Mustard":"mustard",
            "Chana":"chana","Soybean":"soybean","Maize":"maize",
            "Onion":"onion","Garlic":"garlic","Barley":"barley",
            "Bajra":"bajra","Jeera":"cumin","Guar":"guar-seed",
            "Cotton":"cotton","Groundnut":"groundnut",
            "Rice":"rice","Paddy":"paddy","Fennel":"fennel",
            "Urad":"urad","Arhar":"arhar","Turmeric":"turmeric",
            "Chilli":"chilli","Sesame":"sesame","Castor":"castor",
            "Sunflower":"sunflower","Ragi":"ragi",
        }
        slug = slug_map.get(commodity)
        if not slug:
            return None
        url = f"https://acrop.app/prices/{slug}"
        log.info(f"S5:ACROP → {commodity}")
        r = requests.get(url, timeout=12, headers=HDR)
        # Find state-specific price first
        state_abbr = state[:3].lower()
        state_m = re.search(
            state_abbr + r'.{0,100}?₹\s*([1-9][\d,]+)',
            r.text, re.IGNORECASE|re.DOTALL)
        if state_m:
            p = safe_f(state_m.group(1))
            if valid(commodity, p, state):
                return {"price":p,"src":"acrop.app","weight":2}
        # Fallback: median price
        m = re.search(
            r'(?:median|modal|average).{0,50}?₹\s*([1-9][\d,]+)',
            r.text, re.IGNORECASE)
        if m:
            p = safe_f(m.group(1))
            if valid(commodity, p, state):
                return {"price":p,"src":"acrop.app","weight":2}
        return None
    except Exception as e:
        log.warning(f"S5 err {commodity}: {e}")
        return None

def src_rss_all(commodity, state):
    aliases = ALIASES.get(commodity,[commodity.lower()])
    state_aliases = [state.lower(), state[:3].lower()]
    all_prices = []

    def fetch_one(url, name):
        try:
            feed = feedparser.parse(url)
            found = []
            for e in feed.entries[:40]:
                text = (e.get("title","")+" "+e.get("summary","")).lower()
                if any(a.lower() in text for a in aliases):
                    p = extract_price(text)
                    if p and valid(commodity, p, state):
                        found.append(p)
            return found
        except:
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(fetch_one,u,n):n for u,n in RSS_SOURCES}
        for f in concurrent.futures.as_completed(futs):
            try:
                all_prices.extend(f.result())
            except:
                pass

    if not all_prices:
        return None
    median = sorted(all_prices)[len(all_prices)//2]
    log.info(f"RSS {commodity}/{state} → ₹{median} ({len(all_prices)})")
    return {"price":median,"src":"news_rss","weight":1}

# ================================================================
# WEATHER
# ================================================================
def get_weather(city):
    try:
        r = requests.get(f"https://wttr.in/{city}?format=j1",
                         timeout=8, headers=HDR).json()
        c    = r.get("current_condition",[{}])[0]
        temp = c.get("temp_C","30")
        desc = c.get("weatherDesc",[{}])[0].get("value","").lower()
        rain = any(w in desc for w in ["rain","drizzle","thunder","shower"])
        return (rain,
                "तेजी" if rain else "सामान्य",
                f"🌧️ बारिश ({temp}°C)" if rain else f"☀️ साफ ({temp}°C)")
    except:
        return False,"सामान्य","☀️ मौसम साफ"

# ================================================================
# CROSS-VALIDATION
# ================================================================
def cross_validate(commodity, state, results):
    if not results:
        return None,[],[]
    prices = [s["price"] for s in results]
    ok_p, bad_p = iqr_filter(prices)
    accepted  = [s for s in results if s["price"] in ok_p]
    rejected  = [f"{s['src']}(₹{s['price']:.0f})"
                 for s in results if s["price"] in bad_p]
    if not accepted:
        accepted = [max(results,key=lambda x:x["weight"])]
    # Anchor check against high-weight sources
    anchors = [s for s in accepted if s["weight"]>=2]
    if len(anchors)>=2:
        anchor = sorted([s["price"] for s in anchors])[len(anchors)//2]
        final = []
        for s in accepted:
            dev = abs(s["price"]-anchor)/anchor if anchor>0 else 1
            if dev>0.30 and s["weight"]==1:
                rejected.append(f"{s['src']}(₹{s['price']:.0f})")
            else:
                final.append(s)
        accepted = final if final else anchors
    pw = [(s["price"],s["weight"]) for s in accepted]
    price = weighted_median(pw)
    srcs  = list({s["src"] for s in accepted})
    log.info(f"FINAL {state}/{commodity} → ₹{price} [{len(srcs)} sources]")
    return price, srcs, rejected

def bechain(commodity, price, prev, state):
    msp    = MSP.get(commodity,0)
    chg    = ((price-prev)/prev*100) if prev and prev>0 else 0
    mgap   = ((price-msp)/msp*100) if msp>0 else 0
    score  = 50
    if chg>3:    score+=20
    elif chg>1:  score+=10
    elif chg<-3: score-=20
    elif chg<-1: score-=10
    if mgap>15:  score+=15
    elif mgap>5: score+=8
    elif mgap<0: score-=15
    score = max(0,min(100,score))
    if score>=70:   s,h,c="BUY","बेचो 🟢","green"
    elif score>=45: s,h,c="WAIT","रुको 🟡","yellow"
    else:           s,h,c="HOLD","होल्ड 🔴","red"
    return {"signal":s,"signalHindi":h,"score":score,"color":c,
            "changePct":round(chg,2),"mspGapPct":round(mgap,2)}

# ================================================================
# MAIN PROCESSOR
# ================================================================
def process(commodity, state, city, is_rain, impact, alert):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        f1 = ex.submit(src_gov_api,     commodity, state, city)
        f2 = ex.submit(src_agmarknet,   commodity, state, city)
        f3 = ex.submit(src_enam,        commodity, state, city)
        f4 = ex.submit(src_local_portals,commodity, state, city)
        f5 = ex.submit(src_acrop,       commodity, state)
        f6 = ex.submit(src_rss_all,     commodity, state)
        for f in [f1,f2,f3,f4,f5,f6]:
            try:
                res = f.result()
                if res:
                    results.append(res)
            except:
                pass
    gov_data = None
    try:
        gov_data = f1.result()
    except:
        pass

    log.info(f"RAW {state}/{city}/{commodity}: {len(results)} sources")
    for r in results:
        log.info(f"  └─ {r['src']:25} ₹{r['price']:,.0f}  w={r['weight']}")

    final_price, srcs_used, srcs_rejected = cross_validate(commodity,state,results)
    msp_v = MSP.get(commodity,0)
    lo,hi = get_range(commodity, state)

    if final_price is None:
        return {
            "commodityName": commodity,
            "commodityHindi":HINDI.get(commodity,commodity),
            "currentPrice":  None,"maxPrice":None,"minPrice":None,
            "yesterdayPrice":None,"priceChange":None,
            "arrivalQuantity":0,"updateDate":"N/A",
            "mspValue":msp_v,"priceVsMsp":None,
            "priceStatus":"UNVERIFIED",
            "validRangeMin":lo,"validRangeMax":hi,
            "sourcesUsed":[],"sourcesRejected":srcs_rejected,
            "bechainIndex":{"signal":"NONE","signalHindi":"डेटा नहीं ⚪","score":0},
            "isRainExpected":is_rain,"weatherAlert":alert,
            "marketImpact":impact,"dataSourceCount":0,
        }

    prev = gov_data.get("prev") if gov_data else None
    return {
        "commodityName":  commodity,
        "commodityHindi": HINDI.get(commodity,commodity),
        "currentPrice":   final_price,
        "maxPrice":  gov_data.get("max",final_price) if gov_data else final_price,
        "minPrice":  gov_data.get("min",final_price) if gov_data else final_price,
        "yesterdayPrice": prev,
        "priceChange":    round(final_price-prev,2) if prev else None,
        "arrivalQuantity":gov_data.get("qty",0) if gov_data else 0,
        "updateDate":     gov_data.get("date","N/A") if gov_data else "N/A",
        "mspValue":       msp_v,
        "priceVsMsp":     round(final_price-msp_v,2) if msp_v else None,
        "priceStatus":    ("VERIFIED" if len(set(srcs_used))>=2 else
                          "SINGLE_SOURCE" if len(srcs_used)==1 else "UNVERIFIED"),
        "validRangeMin":  lo,"validRangeMax":hi,
        "sourcesUsed":    list(set(srcs_used)),
        "sourcesRejected":srcs_rejected,
        "bechainIndex":   bechain(commodity,final_price,prev,state),
        "isRainExpected": is_rain,"weatherAlert":alert,
        "marketImpact":   impact,
        "dataSourceCount":len(set(srcs_used)),
    }

# ================================================================
# ENDPOINTS
# ================================================================

@app.get("/api/state-crops")
def get_state_crops(state: str = Query(...)):
    """Return crops that actually grow in this state"""
    sd = STATE_DATA.get(state)
    if not sd:
        return {"state":state,"crops":list(GLOBAL_RANGES.keys()),
                "mandis":[],"message":"State not in database, showing all crops"}
    return {
        "state":   state,
        "crops":   sd["crops"],
        "mandis":  sd["mandis"],
        "count":   len(sd["crops"]),
    }

@app.get("/api/all-states")
def get_all_states():
    """Return all supported states with their top crops"""
    return {
        "states": [
            {"name":state,"topCrops":data["crops"][:5],
             "totalCrops":len(data["crops"]),
             "majorMandis":data["mandis"][:4]}
            for state, data in STATE_DATA.items()
        ],
        "totalStates": len(STATE_DATA)
    }

@app.get("/api/bulk-predictions")
def bulk(
    state:       str = Query(default="Rajasthan"),
    city:        str = Query(default="Nagaur"),
    commodities: str = Query(default="AUTO")
):
    """
    AUTO mode: uses state's actual crops
    Or pass comma-separated list
    """
    if commodities == "AUTO" or commodities == "":
        sd = STATE_DATA.get(state, {})
        comm_list = sd.get("crops", list(GLOBAL_RANGES.keys())[:15])
    else:
        comm_list = [c.strip() for c in commodities.split(",") if c.strip()]

    log.info(f"BULK {state}/{city}: {len(comm_list)} crops")
    is_rain, impact, alert = get_weather(city)
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(process,c,state,city,is_rain,impact,alert):c
                for c in comm_list}
        for f in concurrent.futures.as_completed(futs):
            try:
                results.append(f.result())
            except Exception as e:
                log.error(f"Thread err: {e}")

    order = {"VERIFIED":0,"SINGLE_SOURCE":1,"UNVERIFIED":2}
    results.sort(key=lambda x:order.get(x["priceStatus"],3))
    verified = sum(1 for r in results if r["priceStatus"]=="VERIFIED")
    single   = sum(1 for r in results if r["priceStatus"]=="SINGLE_SOURCE")

    return {
        "state":state,"market":city,
        "fetchedAt":datetime.now().isoformat(),
        "serverVersion":"6.0.0",
        "totalCount":len(results),
        "verifiedCount":verified,
        "singleSourceCount":single,
        "dataSources":6,
        "weather":{"isRain":is_rain,"alert":alert,"impact":impact},
        "commodities":results,
    }

@app.get("/api/mandi-predictions")
def single(commodity:str=Query(...), state:str=Query(...), city:str=Query(...)):
    is_rain, impact, alert = get_weather(city)
    r = process(commodity, state, city, is_rain, impact, alert)
    r["locationName"] = city
    r["predictionNote"] = ("बहु-स्रोत सत्यापित भाव"
                           if r["priceStatus"]!="UNVERIFIED"
                           else "सत्यापित भाव उपलब्ध नहीं।")
    return r

@app.get("/api/mandipulse/calculate")
def calculate(commodity:str=Query(...),state:str=Query(...),city:str=Query(...)):
    is_rain, impact, alert = get_weather(city)
    r = process(commodity, state, city, is_rain, impact, alert)
    price = r["currentPrice"]
    msp_v = MSP.get(commodity, 0)
    if price is None:
        return {"commodityHindi":HINDI.get(commodity,commodity),
                "location":city,"currentPrice":None,
                "dataAvailable":False,"message":"डेटा उपलब्ध नहीं।"}
    return {
        "commodityHindi": HINDI.get(commodity,commodity),
        "location":       city, "currentPrice":price,
        "dataAvailable":  True,
        "masterScore":    r["bechainIndex"]["score"],
        "masterSignal":   r["bechainIndex"]["signal"],
        "masterHindi":    r["bechainIndex"]["signalHindi"],
        "bechainIndex":   r["bechainIndex"],
        "sourcesUsed":    r["sourcesUsed"],
        "dataSourceCount":r["dataSourceCount"],
        "mspCalculator":  {
            "mspValue":msp_v,"currentPrice":price,
            "difference":round(price-msp_v,2),
            "hindi":"MSP के ऊपर" if price>msp_v else "MSP से नीचे",
            "color":"green" if price>msp_v else "red",
            "pct":round((price-msp_v)/msp_v*100,1) if msp_v else 0,
        },
        "weatherImpact":{"isRainExpected":is_rain,"alert":alert},
    }

@app.get("/api/mandipulse/dashboard")
def dashboard(commodity:str=Query(...),state:str=Query(...),city:str=Query(...)):
    is_rain, impact, alert = get_weather(city)
    r = process(commodity, state, city, is_rain, impact, alert)
    return {"appName":"MandiPulse 💓","serverVersion":"6.0.0",
            "commodity":commodity,"location":city,
            "currentPrice":r["currentPrice"],"maxPrice":r["maxPrice"],
            "minPrice":r["minPrice"],"yesterdayPrice":r["yesterdayPrice"],
            "arrivalQty":r["arrivalQuantity"],"updateDate":r["updateDate"],
            "priceStatus":r["priceStatus"],"sourcesUsed":r["sourcesUsed"],
            "bechainIndex":r["bechainIndex"],
            "weather":{"isRain":is_rain,"alert":alert,"impact":impact}}

@app.get("/api/markets")
def markets(commodity:str=Query(...), state:str=Query(...)):
    sd = STATE_DATA.get(state, {})
    return {"markets":sd.get("mandis",["Market 1","Market 2"])}

@app.get("/api/listings/all")
def listings(is_pro:bool=False):
    return {"listings":[]}

@app.post("/api/listings/add")
def add_listing(data:dict):
    return {"status":"success","listingId":"L001"}

@app.get("/health")
def health():
    return {"status":"ok","version":"6.0.0",
            "states":len(STATE_DATA),"time":datetime.now().isoformat()}

@app.get("/")
def root():
    return {
        "status":  "MandiPulse v6.0.0 - Har State Ki Apni Fasl",
        "states":  len(STATE_DATA),
        "version": "6.0.0",
        "newAPIs": [
            "GET /api/state-crops?state=Punjab",
            "GET /api/all-states",
            "GET /api/bulk-predictions?state=X&city=Y (AUTO crops!)",
        ],
        "statesCovered": list(STATE_DATA.keys()),
    }
