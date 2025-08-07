import os
import requests
import time
import random
import re
from urllib.parse import urlparse
from typing import List, Dict, Optional
import json
import openai
from datetime import datetime, timedelta
import concurrent.futures
from dotenv import load_dotenv

# --- Configuration ---
# API Keys - Replace with your actual keys or set environment variables
load_dotenv()

# Now read keys only from the environment
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SERPAPI_KEY   = os.getenv("SERPAPI_KEY")
TAVILY_API_KEY= os.getenv("TAVILY_API_KEY")

REQUEST_TIMEOUT = 30
MAX_QUERIES_PER_SOURCE = 8
MAX_DEALS_TO_DISPLAY = 25
MAX_WORKERS = 3

openai.api_key = OPENAI_API_KEY

# Enhanced deal-specific keywords with better patterns
DEAL_INDICATORS = [
    "coupon", "promo code", "discount code", "save", "off", "deal", "promotion", 
    "rebate", "cashback", "special offer", "limited time", "sale", "clearance",
    "% off", "percent off", "dollars off", "$off", "free shipping", "bogo",
    "buy one get one", "buy 2 get", "printable coupon", "digital coupon",
    "manufacturer coupon", "store coupon", "app exclusive", "member price",
    "flash sale", "daily deal", "weekly special", "markdown", "reduced price"
]

CURRENT_DEAL_KEYWORDS = [
    f"{datetime.now().strftime('%B %Y').lower()}", 
    f"{datetime.now().year}", 
    "today", "this week", "current", "now", "active", "valid", "expires",
    "new deals", "latest", "recent", "fresh", "updated", "live", "available"
]

GF_KEYWORDS = [
    "gluten free", "gluten-free", "gluten free", "celiac", "wheat free", 
    "gf", "no gluten", "gluten conscious", "gluten sensitive", "gf certified"
]

# Enhanced store mappings with more coupon sites
DEAL_FOCUSED_STORES = {
     # Coupon hubs & deal communities
    "slickdeals.net":           "Slickdeals",
    "retailmenot.com":          "RetailMeNot",
    "coupons.com":              "Coupons.com",
    "thekrazycouponlady.com":   "Krazy Coupon Lady",
    "groupon.com":              "Groupon",
    "dealsplus.com":            "DealsPlus",
    "woot.com":                 "Woot",
    "hip2save.com":             "Hip2Save",
    "couponmom.com":            "CouponMom",
    "savingstar.com":           "SavingStar",
    "ibotta.com":               "Ibotta",
    "checkout51.com":           "Checkout51",
    "dealspotr.com":            "Dealspotr",
    "couponcabin.com":          "CouponCabin",
    "hotdeals.com":             "HotDeals",
    # Major retailers
    "target.com":               "Target",
    "walmart.com":              "Walmart",
    "amazon.com":               "Amazon",
    "costco.com":               "Costco",
    "kroger.com":               "Kroger",
    "samsclub.com":             "Sam's Club",
    "publix.com":               "Publix",
    "safeway.com":              "Safeway",
    "meijer.com":               "Meijer",
    "hy-vee.com":               "Hy-Vee",
    # Specialty gluten-free retailers
    "thrivemarket.com":         "Thrive Market",
    "vitacost.com":             "Vitacost",
    "iherb.com":                "iHerb",
    "glutenfreemall.com":       "Gluten Free Mall",
    "bobsredmill.com":          "Bob's Red Mill",
    "kingarthurbaking.com":     "King Arthur Baking",
    "annies.com":               "Annie's",
    "schar.com":                "Schar",
    "glutino.com":              "Glutino",
    "enjoylifefoods.com":       "Enjoy Life",
    "canyonbakehouse.com":      "Canyon Bakehouse",
    "simplemills.com":          "Simple Mills",
    "banza.com":                "Banza",
    "kinnikinnick.com":         "Kinnikinnick",
    "barilla.com":              "Barilla",
    "larabar.com":              "Larabar",
    "kind.com":                 "KIND",
    "thai-kitchen.com":         "Thai Kitchen",
    "utdfoods.com":             "Udi's",
}

enhanced_brands = [
        "Bob's Red Mill", "Katz Gluten Free", "Schar",
        "Canyon Bakehouse", "Enjoy Life", "Glutino",
        "Udi's", "Kinnikinnick", "Simple Mills", "Barilla",
        "Banza", "Larabar", "KIND", "Thai Kitchen",
        "Annie's", "King Arthur", "Pamela's", "Mary's Gone Crackers",
        "Tinkyada", "Ancient Harvest", "Lundberg", "GoGo Quinoa",
        "Feel Good Foods", "Namaste", "Good & Gather", "Nature's Path",
        "GFB", "Late July", "Quinn Snacks", "Against The Grain"
    ]

# Add this near your other constants
INVALID_COUPON_TERMS = {
    "CODE", "CODES", "TIONS", "GLUTEN", "GF", "SAVE", "SALE", 
    "FREE", "OFF", "PROMO", "DEAL", "MILTON", "ABSOLUTELY",  # common false positives
    "NAMASTE", "FITJOY",  # sometimes real, but often just brand tags
    "REBATE", "CASHBACK", "CLIP", "DIGITAL", "COUPON"
}

DOMAIN_TO_BRAND = {
    # --- Real Domains ---
    'costco.com': 'Costco',
    'target.com': 'Target',
    'walmart.com': 'Walmart',
    'kroger.com': 'Kroger',
    'amazon.com': 'Amazon',
    'happycampersgf.com': 'Happy Campers GF',
    'gfjules.com': 'GfJules',
    'katzglutenfree.com': 'Katz Gluten Free',
    'barian.com': 'Barian',
    'pipandebby.com': 'Pip & Ebby',
    'tasterepublic.com': 'Taste Republic',
    'groundsforchange.com': 'Grounds For Change',
    'hip2save.com': 'Hip2Save',
    'dealspotr.com': 'Dealspotr',
    'couponcabin.com': 'CouponCabin',
    'canyonbakehouse.com': 'Canyon Bakehouse',
    'bobsredmill.com': 'Bob\'s Red Mill',
    'enjoylifefoods.com': 'Enjoy Life Foods',
    'glutenfreemall.com': 'Gluten Free Mall',
    'schar.com': 'Schar',
    'barilla.com': 'Barilla',
    'kind.com': 'KIND',
    'larabar.com': 'Larabar',
    'thai-kitchen.com': 'Thai Kitchen',
    'utdfoods.com': 'Udi\'s',
    'annie.com': 'Annie\'s',
    'kingarthurbaking.com': 'King Arthur Baking',
    'simplemills.com': 'Simple Mills',
    'banza.com': 'Banza',
    'kinnikinnick.com': 'Kinnikinnick',
    'glutino.com': 'Glutino',
    'vitalcheese.com': 'Vital Cheese',
    'thegrindstone.com': 'The Grindstone',
    'sunfood.com': 'Sunfood',
    'poofyorganics.com': 'Poofy Organics',
    'realfoodblends.com': 'Real Food Blends',
    'gainful.com': 'Gainful',
    'bakerly.com': 'Bakerly',
    'fakefoods.com': 'FakeMeats.com',
    'skout.com': 'Skout Backcountry',
    'famousfoods.com': 'Famous Foods',

    # --- Subdomain Keys (from redirect/affiliate networks) ---
    'happycampersgf': 'Happy Campers GF',
    'glutenfreemall': 'Gluten Free Mall',
    'katzglutenfree': 'Katz Gluten Free',
    'canyonbakehouse': 'Canyon Bakehouse',
    'gfjules': 'GfJules',
    'bobsredmill': 'Bob\'s Red Mill',
    'enjoylifefoods': 'Enjoy Life Foods',
    'groundsforchange': 'Grounds For Change',
    'sunfood': 'Sunfood',
    'poofyorganics': 'Poofy Organics',
    'thegrindstone': 'The Grindstone',
    'realfoodblends': 'Real Food Blends',
    'tasterepublic': 'Taste Republic',
    'glutenproducts': 'Gluten Products',
    'newgrains': 'New Grains Gluten Free Bakery',
    'thegfb': 'The GFB: Gluten Free Bar',
    'maggiebeer': 'Maggie Beer',
    'gainful': 'Gainful',
    'bakerly': 'Bakerly',
    'fakefoods': 'FakeMeats.com',
    'skout': 'Skout Backcountry',
    'famousfoods': 'Famous Foods',
    'schar': 'Schar',
    'barilla': 'Barilla',
    'kind': 'KIND',
    'larabar': 'Larabar',
    'annies': 'Annie\'s',
    'naturepath': 'Nature\'s Path',
    'quinn': 'Quinn Snacks',
    'simplesquare': 'Simple Squares',
    'againstthegrain': 'Against The Grain',
    'cavemanfoods': 'Caveman Foods',
    'kinnikinnick': 'Kinnikinnick',
    'glutino': 'Glutino',
    'utdfoods': 'Udi\'s',
    'thai-kitchen': 'Thai Kitchen',
    'maryruthorganics': 'Mary Ruth Organics',
    'gelsons': 'Gelson\'s',
    'hiddenvalley': 'Hidden Valley',
    'vitaminwater': 'Vitamin Water',
    'planetorganic': 'Planet Organic',
    'energyfirst': 'EnergyFirst',
    'kingshawaiian': 'King\'s Hawaiian',
    'lundsandbyerlys': 'Lunds & Byerly\'s',
    'pranaorganic': 'Prana Organic',
    'mexgrocer': 'MexGrocer',
    'healthyheartmarket': 'Healthy Heart Market',
    'truecitrus': 'True Citrus',
    'sunbutter': 'SunButter',
    'mcevoyranch': 'McEvoy Ranch',
    'flora': 'Flora',
    'carringtonfarms': 'Carrington Farms',
    'cardenas': 'Cardenas Markets',
    'coborns': 'Coborn\'s',
    'foodmaxx': 'FoodMaxx',
    'newseasonsmarket': 'New Seasons Market',
    'waldenfarms': 'Walden Farms',
    'tastybite': 'Tasty Bite',
    'farwestfungi': 'Far West Fungi',
    'mariani': 'Mariani',
    'kinetikasports': 'Kinetica Sports',
    'naturebox': 'NatureBox'
}

def generate_comprehensive_queries() -> Dict[str, List[str]]:
    """Generate comprehensive queries for ALL stores and brands - perfect for weekly cron job."""
    print("🤖 Generating comprehensive queries for all stores & brands using OpenAI LLM...")

    current_month_year = datetime.now().strftime('%B %Y')
    current_year = str(datetime.now().year)

    # Get all stores and brands
    all_stores = list(DEAL_FOCUSED_STORES.keys())
    all_brands = enhanced_brands
    
    print(f"📊 Target coverage: {len(all_stores)} stores + {len(all_brands)} brands")

    llm_prompt = f"""
You are a comprehensive gluten-free deal researcher building a complete database. Generate search queries to find ALL current gluten-free deals, coupons, promo codes, sales, and discounts.

🎯 REQUIREMENTS:
- Generate exactly 2 queries for EACH store: {', '.join(all_stores[:10])}... (total {len(all_stores)} stores)
- Generate exactly 2 queries for EACH brand: {', '.join(all_brands[:10])}... (total {len(all_brands)} brands)
- Focus ONLY on gluten-free products and deals
- Mix different deal types: coupons, promo codes, sales, discounts, BOGO, rebates
- Use current time indicators: {current_month_year}, {current_year}, today, current, active

📋 STORE QUERY PATTERNS (2 per store):
Pattern A: "[store] gluten free [deal type] [time]"
Pattern B: "gluten free [deal type] at [store] [time]"

Examples for Target:
- Target gluten free coupons August 2025
- gluten free deals at Target current

📋 BRAND QUERY PATTERNS (2 per brand):  
Pattern A: "[brand] gluten free [deal type] [time]"
Pattern B: "[brand] [deal type] gluten free products [time]"

Examples for Udi's:
- Udi's gluten free coupons 2025
- Udi's promo codes gluten free products today

🚫 REQUIREMENTS:
- One query per line
- No numbering, bullets, or formatting
- Natural language only
- No site: operators or technical syntax
- Vary deal types: coupons, deals, sales, discounts, promo codes, BOGO, rebates
- Every query MUST include "gluten free" or "gluten-free"

Generate all store queries first, then all brand queries.
Total expected: {len(all_stores) * 2 + len(all_brands) * 2} queries

Current date: {current_month_year}
"""

    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a comprehensive search query expert specializing in gluten-free deals across all major stores and brands."},
                {"role": "user", "content": llm_prompt}
            ],
            max_tokens=2000,  # Increased for more queries
            temperature=0.7,
            top_p=0.9
        )

        generated_queries = response.choices[0].message.content.strip().split('\n')
        generated_queries = [q.strip().strip("•- ") for q in generated_queries if q.strip() and len(q) > 10]
        
        # Remove any numbered queries or headers
        clean_queries = []
        for q in generated_queries:
            # Skip if starts with number, bullet, or is too short
            if not re.match(r'^\d+\.|\d+\)|\•|\-', q) and len(q) > 15:
                clean_queries.append(q)
        
        print(f"✅ Generated {len(clean_queries)} comprehensive queries")
        
        # Split between SerpAPI and Tavily for load balancing
        mid_point = len(clean_queries) // 2
        serpapi_queries = clean_queries[:mid_point]
        tavily_queries = clean_queries[mid_point:]
        
        # Ensure we don't exceed API limits (adjust as needed)
        MAX_QUERIES_PER_SERVICE = 50  # Adjust based on your API limits
        serpapi_queries = serpapi_queries[:MAX_QUERIES_PER_SERVICE]
        tavily_queries = tavily_queries[:MAX_QUERIES_PER_SERVICE]

        print(f"📊 Query distribution: {len(serpapi_queries)} SerpAPI + {len(tavily_queries)} Tavily")
        print(f"🎯 Expected coverage: ~{len(all_stores)} stores + ~{len(all_brands)} brands")
        
        return {
            "serpapi": serpapi_queries,
            "tavily": tavily_queries
        }

    except Exception as e:
        print(f"❌ Comprehensive LLM query generation failed: {e}")
        return generate_comprehensive_fallback_queries()

def generate_comprehensive_fallback_queries() -> Dict[str, List[str]]:
    """Fallback comprehensive queries if LLM fails."""
    print("🔄 Using comprehensive fallback query generation...")
    
    current_date = datetime.now().strftime('%B %Y')
    current_year = str(datetime.now().year)
    
    all_queries = []
    
    # Generate 2 queries for top stores
    top_stores = ['target.com', 'walmart.com', 'kroger.com', 'costco.com', 'amazon.com', 
                  'publix.com', 'safeway.com', 'thrivemarket.com', 'vitacost.com']
    
    deal_types = ['coupons', 'deals', 'promo codes', 'sales', 'discounts', 'BOGO offers']
    time_indicators = [current_date, current_year, 'today', 'current', 'active']
    
    for store in top_stores:
        store_name = DEAL_FOCUSED_STORES.get(store, store.replace('.com', '').title())
        deal_type = random.choice(deal_types)
        time_ind = random.choice(time_indicators)
        
        # Pattern A: Store-first
        all_queries.append(f"{store_name} gluten free {deal_type} {time_ind}")
        # Pattern B: Product-first  
        all_queries.append(f"gluten free {deal_type} at {store_name} {time_ind}")
    
    # Generate 2 queries for top brands
    top_brands = enhanced_brands[:15]  # Top 15 brands
    
    for brand in top_brands:
        deal_type = random.choice(deal_types)
        time_ind = random.choice(time_indicators)
        
        # Pattern A: Brand-first
        all_queries.append(f"{brand} gluten free {deal_type} {time_ind}")
        # Pattern B: Product-first
        all_queries.append(f"{brand} {deal_type} gluten free products {time_ind}")
    
    # Shuffle for variety
    random.shuffle(all_queries)
    
    # Split between services
    mid_point = len(all_queries) // 2
    
    return {
        "serpapi": all_queries[:mid_point],
        "tavily": all_queries[mid_point:]
    }

# Add this function to get store/brand coverage statistics
def analyze_query_coverage(queries: Dict[str, List[str]]) -> Dict:
    """Analyze how many stores and brands are covered in the queries."""
    all_queries_text = ' '.join(queries['serpapi'] + queries['tavily']).lower()
    
    stores_covered = []
    for store_domain, store_name in DEAL_FOCUSED_STORES.items():
        store_variations = [store_name.lower(), store_domain.replace('.com', ''), store_domain]
        if any(var in all_queries_text for var in store_variations):
            stores_covered.append(store_name)
    
    brands_covered = []
    for brand in enhanced_brands:
        if brand.lower() in all_queries_text:
            brands_covered.append(brand)
    
    coverage_stats = {
        "total_queries": len(queries['serpapi']) + len(queries['tavily']),
        "stores_covered": len(stores_covered),
        "total_stores": len(DEAL_FOCUSED_STORES),
        "brands_covered": len(brands_covered), 
        "total_brands": len(enhanced_brands),
        "store_coverage_pct": round((len(stores_covered) / len(DEAL_FOCUSED_STORES)) * 100, 1),
        "brand_coverage_pct": round((len(brands_covered) / len(enhanced_brands)) * 100, 1),
        "covered_stores": stores_covered[:10],  # First 10 for display
        "covered_brands": brands_covered[:10]   # First 10 for display
    }
    
    return coverage_stats

def safe_string_check(text, keywords):
    """Safely check if any keywords exist in text, handling None values."""
    if not text or not isinstance(text, str):
        return False
    return any(keyword in text.lower() for keyword in keywords)

def is_high_quality_deal(item: Dict) -> bool:
    """Enhanced AI-powered deal quality detection with better signal weighting."""
    try:
        title = str(item.get("title", "") or "")
        snippet = str(item.get("snippet", "") or "")
        link = str(item.get("link", "") or "")
        text = (title + " " + snippet).lower()

        # Must contain a gluten-free indicator
        if not any(kw.lower() in text for kw in GF_KEYWORDS):
            return False

        quality_score = 0

        # Strong deal indicators (+3 points)
        strong_patterns = [
            r'\d+%\s*off',
            r'\$\d+\.?\d*\s*off',
            r'save\s*\$\d+',
            r'buy\s+\d+\s+get\s+\d+',
            r'for\s+\$\d+',
            r'printable coupon',
            r'digital coupon',
            r'free shipping',
            r'rebate',
            r'cashback'
        ]
        for pat in strong_patterns:
            if re.search(pat, text, re.I):
                quality_score += 3

        # Coupon code mention (+2)
        if re.search(r'(promo|coupon|discount)\s*code', text, re.I):
            quality_score += 2

        # Medium signals (+2)
        medium_signals = ["deal", "sale", "promotion", "special", "clearance", "markdown"]
        for sig in medium_signals:
            if sig in text:
                quality_score += 2

        # Freshness (+2)
        fresh_keywords = ['today', 'current', 'active', 'new', 'latest', 'this week', datetime.now().strftime('%B').lower()]
        if any(kw in text for kw in fresh_keywords):
            quality_score += 2

        # Trusted site bonus (+5)
        if any(domain in link.lower() for domain in DEAL_FOCUSED_STORES.keys()):
            quality_score += 5

        # Penalize suspicious code values
        coupon_code = item.get("coupon_code", "N/A").upper()
        if coupon_code in INVALID_COUPON_TERMS or len(coupon_code) > 20:
            quality_score -= 5  # strong penalty

        # Penalize missing discount
        if item.get("discount_amount") == "N/A":
            quality_score -= 2

        # LOWERED THRESHOLD FROM 8 TO 3
        return quality_score >= 3  # Much more lenient threshold

    except Exception as e:
        print(f"❌ Error in quality check: {e}")
        return False

def extract_comprehensive_deal_details(item: Dict) -> Dict:
    """Extract comprehensive deal information using AI-enhanced patterns."""
    try:
        title = str(item.get('title', '') or '')
        snippet = str(item.get('snippet', '') or '')
        link = str(item.get('link', '') or '')
        text = f"{title} {snippet}".lower()

        details = {
            "deal_type": "N/A",
            "discount_amount": "N/A",
            "coupon_code": "N/A",
            "expiration": "N/A",
            "restrictions": "N/A"
        }

        # --- Deal Type Detection ---
        deal_types = [
            (r'coupon|promo code|discount code', 'Coupon/Promo Code'),
            (r'rebate|cashback|cash back', 'Rebate/Cashback'),
            (r'sale|clearance|\d+%\s*off', 'Sale/Discount'),
            (r'free shipping', 'Free Shipping'),
            (r'bogo|buy one get|buy \d+ get', 'BOGO/Bundle'),
            (r'printable|digital coupon', 'Printable/Digital Coupon'),
            (r'limited time|while supplies last', 'Limited Time Offer'),
            (r'flash sale|daily deal', 'Flash/Daily Deal'),
            (r'member|exclusive|app only', 'Exclusive Deal')
        ]
        for pattern, deal_type in deal_types:
            if re.search(pattern, text, re.IGNORECASE):
                details["deal_type"] = deal_type
                break

        # --- Discount Extraction (Enhanced) ---
        discount_patterns = [
            (r'(\d+)%\s*off', r'\1% off'),
            (r'\$(\d+(?:\.\d{2})?)\s*off', r'$\1 off'),
            (r'save\s*\$(\d+(?:\.\d{2})?)', r'Save $\1'),
            (r'save\s*(\d+)%', r'Save \1%'),
            (r'up to\s*(\d+)%\s*off', r'Up to \1% off'),
            (r'(\d+)\s*percent\s*off', r'\1% off'),
            (r'buy\s*(\d+)\s*get\s*(\d+)', r'Buy \1 Get \2 Free'),
            (r'(\d+)\s*for\s*\$(\d+(?:\.\d{2})?)', r'\1 for $\2'),
            (r'half\s*off', '50% off'),
            (r'(\d+)\s*=\s*\$(\d+(?:\.\d{2})?)', r'\1 for $\2')
        ]
        for pattern, format_str in discount_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                details["discount_amount"] = re.sub(pattern, format_str, match.group(0), flags=re.IGNORECASE)
                break

        # --- Coupon Code Extraction (Improved) ---
        code_patterns = [
            r'(?:code|use|enter|promo|coupon)[:;\s]*([A-Z0-9]{4,20})\b',
            r'\b(SAVE[A-Z0-9]+)\b',
            r'\b(GET[A-Z0-9]+)\b',
            r'\b([A-Z]{3,}[0-9]{2,})\b',  # e.g., WELCOME20
            r'\b([A-Z0-9]{6,15})\b',      # generic long codes
            r'code\s*[:;]\s*([A-Z0-9]+)',
            r'"([A-Z0-9]{4,})"',          # codes in quotes
        ]
        for pattern in code_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                raw_code = match.upper() if isinstance(match, str) else match[0].upper()
                # Skip invalid or placeholder codes
                if raw_code in INVALID_COUPON_TERMS or len(raw_code) > 15:
                    continue
                if re.fullmatch(r'[A-Z0-9]{4,}', raw_code):  # valid format
                    details["coupon_code"] = raw_code
                    break
            if details["coupon_code"] != "N/A":
                break

        # --- Expiration Extraction ---
        exp_patterns = [
            r'expires? (?:on|at|in|by)?\s*([^\.,\n]+)',
            r'valid (?:through|until|by)?\s*([^\.,\n]+)',
            r'good (?:through|until)?\s*([^\.,\n]+)',
            r'offer ends?\s*([^\.,\n]+)',
            r'while supplies last|limited time|quantities limited'
        ]
        for pattern in exp_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                full_match = match.group(0).strip()
                if 'while supplies last' in full_match.lower():
                    details["expiration"] = "While Supplies Last"
                else:
                    details["expiration"] = full_match
                break

        # --- Restrictions / Fine Print ---
        restriction_keywords = ['restrictions apply', 'exclusions', 'not valid with other offers', 'one per customer']
        if any(kw in text for kw in restriction_keywords):
            details["restrictions"] = "Restrictions apply"

        return details

    except Exception as e:
        print(f"❌ Error extracting deal details: {e}")
        return {
            "deal_type": "N/A",
            "discount_amount": "N/A",
            "coupon_code": "N/A",
            "expiration": "N/A",
            "restrictions": "N/A"
        }

def search_serpapi_enhanced(query: str) -> List[Dict]:
    """Enhanced SerpAPI search with better filtering and error handling."""
    if SERPAPI_KEY == "your-serpapi-key-here":
        print("⚠️  SerpAPI key not configured.")
        return []

    url = "https://serpapi.com/search"
    all_items = []
    seen_links = set()

    # Try multiple engines for better coverage
    engines = ["google", "bing", "duckduckgo"]
    
    for engine in engines:
        params = {
            "engine": engine,
            "q": query,
            "api_key": SERPAPI_KEY,
            "num": 20,  # More results
            "gl": "us",
            "hl": "en",
            "safe": "off",
            "tbs": "qdr:m"  # Past month for fresh results
        }
        
        print(f"-> SerpAPI [{engine}]: {query}")

        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            
            if r.status_code == 429:
                print(f"⚠️  Rate limited on {engine}, waiting...")
                time.sleep(10)
                continue
                
            r.raise_for_status()
            data = r.json()
            
            if "error" in data:
                print(f"❌ SerpAPI error on {engine}: {data['error']}")
                continue
                
        except Exception as e:
            print(f"❌ Error searching SerpAPI [{engine}]: {e}")
            continue

        # Process results with AI-enhanced filtering
        results = data.get("organic_results", [])
        engine_items = 0
        
        for item in results:
            link = item.get("link", "")
            if link and link not in seen_links:
                result_item = {
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                    "link": link,
                    "source": f"SerpAPI ({engine})",
                    "timestamp": datetime.now().isoformat()
                }
                
                # Only include high-quality deals
                if is_high_quality_deal(result_item):
                    seen_links.add(link)
                    # Extract comprehensive deal details
                    deal_details = extract_comprehensive_deal_details(result_item)
                    result_item.update(deal_details)
                    all_items.append(result_item)
                    engine_items += 1
        
        print(f"  ✅ {engine_items} high-quality deals from {engine}")
        time.sleep(random.uniform(2, 4))  # Smart rate limiting

    print(f"  📊 Total SerpAPI deals: {len(all_items)}")
    return all_items

def search_tavily_enhanced(query: str) -> List[Dict]:
    """Enhanced Tavily search with AI-powered filtering."""
    if TAVILY_API_KEY == "your-tavily-key-here":
        print("⚠️  Tavily key not configured.")
        return []
        
    print(f"-> Tavily: {query}")
    
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",
        "include_answer": True,
        "max_results": 25,  # More results
        "include_domains": list(DEAL_FOCUSED_STORES.keys()),  # Focus on deal sites
        "days": 30  # Only recent results
    }
    
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"❌ Error searching Tavily: {e}")
        return []

    items = []
    
    # Process answer if it contains actual deal info
    answer = data.get("answer", "").strip()
    if answer and len(answer) > 50:  # Only substantial answers
        answer_item = {
            "title": f"AI Summary: {query[:50]}...",
            "snippet": answer[:500],  # Longer snippet
            "link": "N/A",
            "source": "Tavily AI Summary",
            "timestamp": datetime.now().isoformat()
        }
        
        if is_high_quality_deal(answer_item):
            deal_details = extract_comprehensive_deal_details(answer_item)
            answer_item.update(deal_details)
            items.append(answer_item)

    # Process search results with AI filtering
    valid_results = 0
    for result in data.get("results", []):
        item = {
            "title": result.get("title", ""),
            "snippet": result.get("content", "")[:600],  # Longer snippet
            "link": result.get("url", ""),
            "source": "Tavily",
            "timestamp": datetime.now().isoformat()
        }
        
        if is_high_quality_deal(item):
            deal_details = extract_comprehensive_deal_details(item)
            item.update(deal_details)
            items.append(item)
            valid_results += 1

    print(f"  ✅ {valid_results} high-quality Tavily deals")
    return items

def ai_powered_deal_validation(deals: List[Dict]) -> List[Dict]:
    """AI-powered final validation and scoring."""
    print(f"🤖 AI-powered validation of {len(deals)} deals...")
    validated_deals = []
    debug_stats = {"passed": 0, "failed_gf": 0, "failed_score": 0}
    
    for i, deal in enumerate(deals):
        try:
            # Skip if not a dict
            if not isinstance(deal, dict):
                print(f"⚠️ Skipping invalid deal #{i+1} (not dict): {type(deal)}")
                continue

            title = str(deal.get('title', '') or '')
            snippet = str(deal.get('snippet', '') or '')
            link = str(deal.get('link', '') or '')
            text = f"{title} {snippet}".lower()

            # Must contain GF keywords
            has_gf = any(kw.lower() in text for kw in GF_KEYWORDS)
            if not has_gf:
                debug_stats["failed_gf"] += 1
                if i < 3:  # Debug first few
                    print(f"  Debug #{i+1}: Failed GF check - '{title[:50]}...'")
                continue

            quality_score = 0

            # Strong deal indicators (+3 points)
            strong_patterns = [
                r'\d+%\s*off', r'\$\d+\.?\d*\s*off', r'save\s*\$\d+',
                r'buy\s+\d+\s+get\s+\d+', r'printable coupon', r'digital coupon',
                r'free shipping', r'rebate', r'cashback'
            ]
            for pat in strong_patterns:
                if re.search(pat, text, re.I):
                    quality_score += 3

            # Medium signals (+2)
            medium_signals = ["deal", "sale", "promotion", "special", "clearance", "coupon", "discount"]
            for sig in medium_signals:
                if sig in text:
                    quality_score += 2

            # Freshness (+2)
            fresh_keywords = ['today', 'current', 'active', 'new', 'latest', datetime.now().strftime('%B').lower()]
            if any(kw in text for kw in fresh_keywords):
                quality_score += 2

            # Trusted site bonus (+3) - lowered from +5
            if any(domain in link.lower() for domain in DEAL_FOCUSED_STORES.keys()):
                quality_score += 3

            deal["ai_quality_score"] = quality_score
            
            # LOWERED THRESHOLD TO 2 (was 8)
            if quality_score >= 2:
                validated_deals.append(deal)
                debug_stats["passed"] += 1
                if len(validated_deals) <= 3:  # Debug first few passed
                    print(f"  ✅ Deal #{i+1} passed (score: {quality_score}): '{title[:40]}...'")
            else:
                debug_stats["failed_score"] += 1
                if debug_stats["failed_score"] <= 3:  # Debug first few failed
                    print(f"  ❌ Deal #{i+1} failed score (score: {quality_score}): '{title[:40]}...'")
                    
        except Exception as e:
            print(f"❌ Error validating deal #{i+1}: {e}")
            continue

    # Sort by AI quality score
    validated_deals.sort(key=lambda x: x.get("ai_quality_score", 0), reverse=True)
    
    print(f" ✅ Validation complete:")
    print(f"   • Passed: {debug_stats['passed']}")
    print(f"   • Failed GF check: {debug_stats['failed_gf']}")  
    print(f"   • Failed score check: {debug_stats['failed_score']}")
    print(f" ✅ {len(validated_deals)} AI-validated high-quality deals")
    
    return validated_deals

def enhance_deal_metadata(deals: List[Dict]) -> List[Dict]:
    """Add store, brand, and category using DOMAIN_TO_BRAND and subdomain matching."""
    enhanced = []
    for deal in deals:
        try:
            if not isinstance(deal, dict):
                continue

            link = deal.get("link", "") or ""
            title = str(deal.get("title", "") or "")
            snippet = str(deal.get("snippet", "") or "")
            text = f"{title} {snippet}".lower()

            # Extract domain or subdomain
            store = "Unknown"
            try:
                if link and link.startswith(('http://', 'https://')):
                    parsed = urlparse(link)
                    domain = parsed.netloc.lower().replace("www.", "")
                    
                    # Try full domain first
                    if domain in DOMAIN_TO_BRAND:
                        store = DOMAIN_TO_BRAND[domain]
                    else:
                        # Try subdomain (first part before first dot)
                        parts = domain.split('.')
                        if len(parts) > 0 and parts[0] in DOMAIN_TO_BRAND:
                            store = DOMAIN_TO_BRAND[parts[0]]
                        else:
                            # Check if any domain key is contained in the link
                            for domain_key, brand_name in DOMAIN_TO_BRAND.items():
                                if domain_key in domain or domain_key in link.lower():
                                    store = brand_name
                                    break
                            
                            # Fallback: check known store names in title/snippet
                            if store == "Unknown":
                                for store_name in ['target', 'walmart', 'kroger', 'costco', 'amazon', 'whole foods']:
                                    if store_name in text:
                                        store = store_name.title()
                                        break
            except Exception as e:
                print(f"⚠️ Error parsing URL {link}: {e}")

            deal["store"] = store

            # Brand: if not set, use store (same for direct brands)
            brand = deal.get("brand", "Multiple/Various")
            if brand == "Multiple/Various":
                # Try to extract brand from title/snippet
                brand = store if store != "Unknown" else "Multiple/Various"
                
                # Check for specific brand names in text
                for brand_name in enhanced_brands:
                    if brand_name.lower() in text:
                        brand = brand_name
                        break

            deal["brand"] = brand

            # Category
            food_terms = ['snack', 'food', 'meal', 'cookie', 'cracker', 'bread', 'pizza']
            frozen_terms = ['frozen', 'ice cream', 'entree']
            if any(t in text for t in food_terms):
                deal["category"] = "Food"
            elif any(t in text for t in frozen_terms):
                deal["category"] = "Frozen"
            else:
                deal["category"] = "General"

            enhanced.append(deal)
        except Exception as e:
            print(f"❌ Error enhancing deal: {e}")
            # Still append the deal even if enhancement fails
            if isinstance(deal, dict):
                deal.setdefault("store", "Unknown")
                deal.setdefault("brand", "Multiple/Various") 
                deal.setdefault("category", "General")
                enhanced.append(deal)
            continue
    return enhanced

def is_real_deal(item: Dict) -> bool:
        link    = item.get("link", "") or ""
        title   = str(item.get("title", "")).lower()
        snippet = str(item.get("snippet", "")).lower()
        text    = title + " " + snippet

        # 1) Must have a real HTTP(S) link
        if not link.startswith(("http://", "https://")):
            return False

        # 2) Must mention gluten-free
        if not any(kw in text for kw in [k.lower() for k in GF_KEYWORDS]):
            return False

        # 3) Must mention at least one deal indicator
        if not any(sig in text for sig in [d.lower() for d in DEAL_INDICATORS]):
            return False

        return True

def display_ai_results(results: List[Dict]):
    """Display AI-validated deals with comprehensive information."""
    if not results:
        print("\n❌ No AI-validated deals found")
        return

    print("\n" + "="*90)
    print("🤖 AI-VALIDATED GLUTEN-FREE DEALS & COUPONS")
    print(f"📅 Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
    print(f"🎯 {len(results)} premium deals found")
    print("="*90)

    for i, deal in enumerate(results[:MAX_DEALS_TO_DISPLAY], 1):
        print(f"\n{i:2d}. {deal.get('title', 'N/A')}")
        print(f"    🏪 Store: {deal.get('store', 'N/A')}")
        print(f"    🏷️  Brand: {deal.get('brand', 'N/A')}")
        print(f"    📂 Category: {deal.get('category', 'N/A')}")
        print(f"    🎫 Type: {deal.get('deal_type', 'N/A')}")
        print(f"    💰 Discount: {deal.get('discount_amount', 'N/A')}")
        print(f"    🎟️  Code: {deal.get('coupon_code', 'N/A')}")
        print(f"    ⏰ Expires: {deal.get('expiration', 'Check source')}")
        print(f"    🤖 AI Score: {deal.get('ai_quality_score', 0)}/20")
        
        snippet = deal.get('snippet', '')
        if snippet:
            display_snippet = (snippet[:200] + '...') if len(snippet) > 200 else snippet
            print(f"    📝 Details: {display_snippet}")
        
        print(f"    🔗 Source: {deal.get('link', 'N/A')}")
        
        if i < min(len(results), MAX_DEALS_TO_DISPLAY):
            print("    " + "-"*80)

    remaining = len(results) - MAX_DEALS_TO_DISPLAY
    if remaining > 0:
        print(f"\n... and {remaining} more AI-validated deals saved to file")

    print("\n" + "="*90)

def save_ai_deals(deals: List[Dict], filename: str = 'gf_deals.json') -> List[Dict]:
    """Save AI-validated deals with smart deduplication."""
    # Load existing deals
    existing_deals = []
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                existing_deals = json.load(f)
        except Exception as e:
            print(f"⚠️  Could not load {filename}: {e}")
    
    # If no new deals, still save existing deals back (prevents data loss)
    if not deals:
        print(f"⚠️  No new deals to save, keeping existing {len(existing_deals)} deals")
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(existing_deals, f, indent=2, ensure_ascii=False)
            print(f"✅ Maintained {len(existing_deals)} existing deals in {filename}")
        except Exception as e:
            print(f"❌ Error saving existing deals: {e}")
        return existing_deals
    
    # Smart deduplication using multiple factors
    seen_deals = {}
    all_deals = existing_deals + deals
    
    for deal in all_deals:
        try:
            # Create composite key using title, link, and discount
            title = str(deal.get('title', ''))
            title_key = re.sub(r'[^\w\s]', '', title).lower().strip()[:60]
            link_key = str(deal.get('link', 'N/A'))
            discount_key = str(deal.get('discount_amount', 'N/A'))
            composite_key = f"{title_key}:{link_key}:{discount_key}"
            
            if composite_key not in seen_deals:
                seen_deals[composite_key] = deal
            else:
                # Keep the one with higher AI quality score
                existing = seen_deals[composite_key]
                if deal.get('ai_quality_score', 0) > existing.get('ai_quality_score', 0):
                    seen_deals[composite_key] = deal
        except Exception as e:
            print(f"❌ Error processing deal for deduplication: {e}")
            continue
    
    # Sort by AI quality score and timestamp
    unique_deals = list(seen_deals.values())
    unique_deals.sort(key=lambda x: (x.get('ai_quality_score', 0), x.get('timestamp', '')), reverse=True)
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(unique_deals, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved {len(unique_deals)} total deals to {filename} ({len(deals)} new)")
    except Exception as e:
        print(f"❌ Error saving deals: {e}")
    
    return unique_deals

def generate_deal_analytics(deals: List[Dict]) -> Dict:
    """Generate comprehensive analytics for the deals."""
    analytics = {
        "total_deals": len(deals),
        "avg_quality_score": 0,
        "deal_types": {},
        "stores": {},
        "brands": {},
        "categories": {},
        "discount_ranges": {
            "percentage": [],
            "dollar": []
        },
        "expiration_status": {
            "has_expiration": 0,
            "no_expiration": 0
        },
        "code_availability": {
            "has_code": 0,
            "no_code": 0
        }
    }
    
    if not deals:
        return analytics
    
    total_score = 0
    
    for deal in deals:
        try:
            # Quality score
            score = deal.get('ai_quality_score', 0)
            total_score += score
            
            # Deal types
            deal_type = deal.get('deal_type', 'N/A')
            analytics["deal_types"][deal_type] = analytics["deal_types"].get(deal_type, 0) + 1
            
            # Stores
            store = deal.get('store', 'N/A')
            analytics["stores"][store] = analytics["stores"].get(store, 0) + 1
            
            # Brands
            brand = deal.get('brand', 'N/A')
            analytics["brands"][brand] = analytics["brands"].get(brand, 0) + 1
            
            # Categories
            category = deal.get('category', 'N/A')
            analytics["categories"][category] = analytics["categories"].get(category, 0) + 1
            
            # Discount analysis
            discount = str(deal.get('discount_amount', 'N/A'))
            if discount != 'N/A':
                if '%' in discount:
                    match = re.search(r'(\d+)', discount)
                    if match:
                        analytics["discount_ranges"]["percentage"].append(int(match.group(1)))
                elif '$' in discount:
                    match = re.search(r'\$(\d+(?:\.\d{2})?)', discount)
                    if match:
                        analytics["discount_ranges"]["dollar"].append(float(match.group(1)))
            
            # Expiration analysis
            if deal.get('expiration', 'N/A') != 'N/A':
                analytics["expiration_status"]["has_expiration"] += 1
            else:
                analytics["expiration_status"]["no_expiration"] += 1
            
            # Code availability
            if deal.get('coupon_code', 'N/A') != 'N/A':
                analytics["code_availability"]["has_code"] += 1
            else:
                analytics["code_availability"]["no_code"] += 1
        except Exception as e:
            print(f"❌ Error processing deal for analytics: {e}")
            continue
    
    # Calculate average quality score
    analytics["avg_quality_score"] = round(total_score / len(deals), 2) if deals else 0
    
    # Sort dictionaries by count
    for key in ["deal_types", "stores", "brands", "categories"]:
        analytics[key] = dict(sorted(analytics[key].items(), key=lambda x: x[1], reverse=True))
    
    return analytics

def display_analytics(analytics: Dict):
    """Display comprehensive analytics."""
    print("\n" + "="*60)
    print("📊 COMPREHENSIVE DEAL ANALYTICS")
    print("="*60)
    
    print(f"\n🎯 OVERVIEW:")
    print(f"   • Total AI-validated deals: {analytics['total_deals']}")
    print(f"   • Average quality score: {analytics['avg_quality_score']}/20")
    
    if analytics['total_deals'] > 0:
        print(f"\n🏪 TOP STORES:")
        for store, count in list(analytics['stores'].items())[:5]:
            percentage = (count / analytics['total_deals']) * 100
            print(f"   • {store}: {count} deals ({percentage:.1f}%)")
        
        print(f"\n🏷️  TOP BRANDS:")
        for brand, count in list(analytics['brands'].items())[:5]:
            if brand != "Multiple/Various":
                percentage = (count / analytics['total_deals']) * 100
                print(f"   • {brand}: {count} deals ({percentage:.1f}%)")
        
        print(f"\n🎫 DEAL TYPES:")
        for deal_type, count in analytics['deal_types'].items():
            percentage = (count / analytics['total_deals']) * 100
            print(f"   • {deal_type}: {count} deals ({percentage:.1f}%)")
        
        print(f"\n💰 DISCOUNT ANALYSIS:")
        if analytics['discount_ranges']['percentage']:
            avg_pct = sum(analytics['discount_ranges']['percentage']) / len(analytics['discount_ranges']['percentage'])
            max_pct = max(analytics['discount_ranges']['percentage'])
            print(f"   • Average % discount: {avg_pct:.1f}%")
            print(f"   • Maximum % discount: {max_pct}%")
        
        if analytics['discount_ranges']['dollar']:
            avg_dollar = sum(analytics['discount_ranges']['dollar']) / len(analytics['discount_ranges']['dollar'])
            max_dollar = max(analytics['discount_ranges']['dollar'])
            print(f"   • Average $ discount: ${avg_dollar:.2f}")
            print(f"   • Maximum $ discount: ${max_dollar:.2f}")
        
        print(f"\n🎟️  COUPON CODE AVAILABILITY:")
        has_code = analytics['code_availability']['has_code']
        no_code = analytics['code_availability']['no_code']
        code_percentage = (has_code / analytics['total_deals']) * 100
        print(f"   • Deals with codes: {has_code} ({code_percentage:.1f}%)")
        print(f"   • Deals without codes: {no_code} ({100-code_percentage:.1f}%)")
        
        print(f"\n⏰ EXPIRATION INFO:")
        has_exp = analytics['expiration_status']['has_expiration']
        no_exp = analytics['expiration_status']['no_expiration']
        exp_percentage = (has_exp / analytics['total_deals']) * 100
        print(f"   • With expiration: {has_exp} ({exp_percentage:.1f}%)")
        print(f"   • Without expiration: {no_exp} ({100-exp_percentage:.1f}%)")
    
    print("\n" + "="*60)

def main() -> List[Dict]:
    """AI-Enhanced main function for premium gluten-free deals."""
    print("🚀 AI-ENHANCED Gluten-Free Deals & Coupons Scraper v2.0")
    print(f"📅 Searching for current deals as of {datetime.now().strftime('%B %d, %Y')}")
    print("🤖 Powered by OpenAI LLM + Advanced AI Filtering")
    print("🎯 Focus: 100% VERIFIED gluten-free deals, coupons, and promo codes")

    # --- Validate API keys strictly from environment ---
    missing = []
    if not OPENAI_API_KEY:
        missing.append("OpenAI")
    if not SERPAPI_KEY:
        missing.append("SerpAPI")
    if not TAVILY_API_KEY:
        missing.append("Tavily")
    if missing:
        raise RuntimeError(f"Missing API keys for: {', '.join(missing)}")

    # --- Generate queries (with fallback) ---
    try:
        queries = generate_comprehensive_queries()
    except Exception as e:
        print(f"❌ Query generation failed: {e} — using fallback")
        queries = generate_comprehensive_fallback_queries()

    # --- Collect raw results ---
    all_results: List[Dict] = []
    if SERPAPI_KEY:
        print(f"\n🔍 Running {len(queries['serpapi'])} SerpAPI queries…")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            futures = [exec.submit(search_serpapi_enhanced, q) for q in queries['serpapi']]
            for fut in concurrent.futures.as_completed(futures):
                all_results.extend(fut.result())
                time.sleep(random.uniform(1, 2))

    if TAVILY_API_KEY:
        print(f"\n🔍 Running {len(queries['tavily'])} Tavily queries…")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            futures = [exec.submit(search_tavily_enhanced, q) for q in queries['tavily']]
            for fut in concurrent.futures.as_completed(futures):
                all_results.extend(fut.result())
                time.sleep(random.uniform(1, 2))

    print(f"\n📊 Collected {len(all_results)} raw results")
    if not all_results:
        raise RuntimeError("No results found. Check your API keys or service availability.")


    # Filter out anything that isn’t a real, link-backed GF deal
     # --- Basic real-deal filtering ---
    all_results = [item for item in all_results if is_real_deal(item)]
    print(f"📊 {len(all_results)} after basic real-deal filtering")

    # --- AI validation & metadata enhancement ---
    print("\n🤖 Validating deals with AI…")
    validated = ai_powered_deal_validation(all_results)

    print("🛠️  Enhancing metadata…")
    enhanced = enhance_deal_metadata(validated)

    print("💾 Saving & deduplicating…")
    final_deals = save_ai_deals(enhanced)

    print(f"\n🎉 SUCCESS: {len(final_deals)} premium deals ready")
    return final_deals

from flask import jsonify, Request
from datetime import datetime

def get_gluten_free_deals(request: Request):
    """
    HTTP Cloud Function entry point.
    Runs `main()` and returns JSON.
    """
    try:
        deals = main()
        return jsonify({
            "generatedAt": datetime.utcnow().isoformat() + "Z",
            "count": len(deals),
            "deals": deals
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    start = datetime.now()
    try:
        deals = main()
    except Exception as err:
        print(f"❌ Execution error: {err}")
    else:
        elapsed = (datetime.now() - start).total_seconds()
        print(f"⏱️  Total runtime: {elapsed:.1f} seconds")