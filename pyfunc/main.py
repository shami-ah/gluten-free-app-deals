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
from google.cloud import firestore
import hashlib

# --- Configuration ---
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SERPAPI_KEY    = os.getenv("SERPAPI_KEY")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

REQUEST_TIMEOUT = 30
MAX_QUERIES_PER_SOURCE = 8
MAX_DEALS_TO_DISPLAY = 25
MAX_WORKERS = 3

openai.api_key = OPENAI_API_KEY

# --- Constants ---
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

DEAL_FOCUSED_STORES = {
    "slickdeals.net":"Slickdeals", "retailmenot.com":"RetailMeNot", "coupons.com":"Coupons.com",
    "thekrazycouponlady.com":"Krazy Coupon Lady", "groupon.com":"Groupon", "dealsplus.com":"DealsPlus",
    "woot.com":"Woot", "hip2save.com":"Hip2Save", "couponmom.com":"CouponMom", "savingstar.com":"SavingStar",
    "ibotta.com":"Ibotta", "checkout51.com":"Checkout51", "dealspotr.com":"Dealspotr", "couponcabin.com":"CouponCabin",
    "hotdeals.com":"HotDeals",
    "target.com":"Target", "walmart.com":"Walmart", "amazon.com":"Amazon", "costco.com":"Costco", "kroger.com":"Kroger",
    "samsclub.com":"Sam's Club", "publix.com":"Publix", "safeway.com":"Safeway", "meijer.com":"Meijer", "hy-vee.com":"Hy-Vee",
    "thrivemarket.com":"Thrive Market", "vitacost.com":"Vitacost", "iherb.com":"iHerb", "glutenfreemall.com":"Gluten Free Mall",
    "bobsredmill.com":"Bob's Red Mill", "kingarthurbaking.com":"King Arthur Baking", "annies.com":"Annie's", "schar.com":"Schar",
    "glutino.com":"Glutino", "enjoylifefoods.com":"Enjoy Life", "canyonbakehouse.com":"Canyon Bakehouse", "simplemills.com":"Simple Mills",
    "banza.com":"Banza", "kinnikinnick.com":"Kinnikinnick", "barilla.com":"Barilla", "larabar.com":"Larabar", "kind.com":"KIND",
    "thai-kitchen.com":"Thai Kitchen", "utdfoods.com":"Udi's",
}

enhanced_brands = [
    "Bob's Red Mill","Katz Gluten Free","Schar","Canyon Bakehouse","Enjoy Life","Glutino","Udi's","Kinnikinnick",
    "Simple Mills","Barilla","Banza","Larabar","KIND","Thai Kitchen","Annie's","King Arthur","Pamela's",
    "Mary's Gone Crackers","Tinkyada","Ancient Harvest","Lundberg","GoGo Quinoa","Feel Good Foods","Namaste",
    "Good & Gather","Nature's Path","GFB","Late July","Quinn Snacks","Against The Grain"
]

INVALID_COUPON_TERMS = {
    "CODE","CODES","TIONS","GLUTEN","GF","SAVE","SALE","FREE","OFF","PROMO","DEAL","MILTON","ABSOLUTELY",
    "NAMASTE","FITJOY","REBATE","CASHBACK","CLIP","DIGITAL","COUPON"
}

DOMAIN_TO_BRAND = {
    'costco.com':'Costco','target.com':'Target','walmart.com':'Walmart','kroger.com':'Kroger','amazon.com':'Amazon',
    'happycampersgf.com':'Happy Campers GF','gfjules.com':'GfJules','katzglutenfree.com':'Katz Gluten Free','barian.com':'Barian',
    'pipandebby.com':'Pip & Ebby','tasterepublic.com':'Taste Republic','groundsforchange.com':'Grounds For Change',
    'hip2save.com':'Hip2Save','dealspotr.com':'Dealspotr','couponcabin.com':'CouponCabin','canyonbakehouse.com':'Canyon Bakehouse',
    'bobsredmill.com':"Bob's Red Mill",'enjoylifefoods.com':'Enjoy Life Foods','glutenfreemall.com':'Gluten Free Mall',
    'schar.com':'Schar','barilla.com':'Barilla','kind.com':'KIND','larabar.com':'Larabar','thai-kitchen.com':'Thai Kitchen',
    'utdfoods.com':"Udi's",'annie.com':"Annie's",'kingarthurbaking.com':'King Arthur Baking','simplemills.com':'Simple Mills',
    'banza.com':'Banza','kinnikinnick.com':'Kinnikinnick','glutino.com':'Glutino','vitalcheese.com':'Vital Cheese',
    'thegrindstone.com':'The Grindstone','sunfood.com':'Sunfood','poofyorganics.com':'Poofy Organics',
    'realfoodblends.com':'Real Food Blends','gainful.com':'Gainful','bakerly.com':'Bakerly','fakefoods.com':'FakeMeats.com',
    'skout.com':'Skout Backcountry','famousfoods.com':'Famous Foods',
    # subdomain keys
    'happycampersgf':'Happy Campers GF','glutenfreemall':'Gluten Free Mall','katzglutenfree':'Katz Gluten Free','canyonbakehouse':'Canyon Bakehouse',
    'gfjules':'GfJules','bobsredmill':"Bob's Red Mill",'enjoylifefoods':'Enjoy Life Foods','groundsforchange':'Grounds For Change',
    'sunfood':'Sunfood','poofyorganics':'Poofy Organics','thegrindstone':'The Grindstone','realfoodblends':'Real Food Blends',
    'tasterepublic':'Taste Republic','glutenproducts':'Gluten Products','newgrains':'New Grains Gluten Free Bakery',
    'thegfb':'The GFB: Gluten Free Bar','maggiebeer':'Maggie Beer','gainful':'Gainful','bakerly':'Bakerly','fakefoods':'FakeMeats.com',
    'skout':'Skout Backcountry','famousfoods':'Famous Foods','schar':'Schar','barilla':'Barilla','kind':'KIND','larabar':'Larabar',
    'annies':"Annie's",'naturepath':"Nature's Path",'quinn':'Quinn Snacks','simplesquare':'Simple Squares','againstthegrain':'Against The Grain',
    'cavemanfoods':'Caveman Foods','kinnikinnick':'Kinnikinnick','glutino':'Glutino','utdfoods':"Udi's",'thai-kitchen':'Thai Kitchen',
    'maryruthorganics':'Mary Ruth Organics','gelsons':"Gelson's",'hiddenvalley':'Hidden Valley','vitaminwater':'Vitamin Water',
    'planetorganic':'Planet Organic','energyfirst':'EnergyFirst',"kingshawaiian":"King's Hawaiian",'lundsandbyerlys':"Lunds & Byerly's",
    'pranaorganic':'Prana Organic','mexgrocer':'MexGrocer','healthyheartmarket':'Healthy Heart Market','truecitrus':'True Citrus',
    'sunbutter':'SunButter','mcevoyranch':'McEvoy Ranch','flora':'Flora','carringtonfarms':'Carrington Farms','cardenas':'Cardenas Markets',
    'coborns':"Coborn's",'foodmaxx':'FoodMaxx','newseasonsmarket':'New Seasons Market','waldenfarms':'Walden Farms','tastybite':'Tasty Bite',
    'farwestfungi':'Far West Fungi','mariani':'Mariani','kinetikasports':'Kinetica Sports','naturebox':'NatureBox'
}

# ---------------- Firestore replace-all writer ----------------
def replace_deals_firestore(deals: List[Dict], run_ts: datetime, batch_size: int = 300) -> Dict[str, int]:
    """
    Delete all docs in 'gf_deals', then insert the new list of deals.
    Uses batched deletes + batched writes.
    """
    db = firestore.Client()
    coll = db.collection("gf_deals")

    # Delete existing docs in batches
    deleted = 0
    while True:
        docs = list(coll.limit(batch_size).stream())
        if not docs:
            break
        for d in docs:
            d.reference.delete()
        deleted += len(docs)
    print(f"🧹 Deleted {deleted} existing docs from gf_deals")

    # Write new deals
    count = 0
    batch = db.batch()
    for deal in deals:
        key = f"{deal.get('link','')}|{deal.get('discount_amount','')}"
        doc_id = hashlib.sha1(key.encode('utf-8')).hexdigest()
        doc = coll.document(doc_id)
        data = {**deal, "runAt": run_ts, "updatedAt": firestore.SERVER_TIMESTAMP}
        batch.set(doc, data)
        count += 1
        if count % 450 == 0:
            batch.commit()
            batch = db.batch()
    batch.commit()
    print(f"✅ Inserted {count} new docs into gf_deals")
    return {"deleted": deleted, "inserted": count}

# ---------------- Query gen, search, AI filters (unchanged) ----------------
def generate_comprehensive_queries() -> Dict[str, List[str]]:
    print("🤖 Generating comprehensive queries for all stores & brands using OpenAI LLM...")
    current_month_year = datetime.now().strftime('%B %Y')
    current_year = str(datetime.now().year)
    all_stores = list(DEAL_FOCUSED_STORES.keys())
    all_brands = enhanced_brands
    print(f"📊 Target coverage: {len(all_stores)} stores + {len(all_brands)} brands")

    llm_prompt = f"""
You are a comprehensive gluten-free deal researcher building a complete database. Generate search queries to find ALL current gluten-free deals, coupons, promo codes, sales, and discounts.

- Generate exactly 2 queries for EACH store ({len(all_stores)} stores)
- Generate exactly 2 queries for EACH brand ({len(all_brands)} brands)
- Use indicators like {current_month_year}, {current_year}, today, current, active
- Each query MUST include "gluten free" or "gluten-free"
One query per line; no numbering or bullets.
Current date: {current_month_year}
"""
    try:
        resp = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role":"system","content":"You are a search query expert for gluten-free deals."},
                {"role":"user","content":llm_prompt}
            ],
            max_tokens=2000, temperature=0.7, top_p=0.9
        )
        generated = resp.choices[0].message.content.strip().splitlines()
        clean = [q.strip("•- ").strip() for q in generated if q and len(q.strip()) > 15 and not re.match(r'^\d+[\.\)]', q.strip())]
        print(f"✅ Generated {len(clean)} comprehensive queries")
        mid = len(clean)//2
        serp = clean[:mid][:50]
        tav  = clean[mid:][:50]
        print(f"📊 Query distribution: {len(serp)} SerpAPI + {len(tav)} Tavily")
        print(f"🎯 Expected coverage: ~{len(all_stores)} stores + ~{len(all_brands)} brands")
        return {"serpapi": serp, "tavily": tav}
    except Exception as e:
        print(f"❌ Comprehensive LLM query generation failed: {e}")
        return generate_comprehensive_fallback_queries()

def generate_comprehensive_fallback_queries() -> Dict[str, List[str]]:
    print("🔄 Using comprehensive fallback query generation...")
    current_date = datetime.now().strftime('%B %Y')
    current_year = str(datetime.now().year)
    top_stores = ['target.com','walmart.com','kroger.com','costco.com','amazon.com','publix.com','safeway.com','thrivemarket.com','vitacost.com']
    deal_types = ['coupons','deals','promo codes','sales','discounts','BOGO offers']
    time_indicators = [current_date, current_year, 'today', 'current', 'active']

    all_q = []
    for store in top_stores:
        name = DEAL_FOCUSED_STORES.get(store, store.replace('.com','').title())
        dt = random.choice(deal_types); ti = random.choice(time_indicators)
        all_q.append(f"{name} gluten free {dt} {ti}")
        all_q.append(f"gluten free {dt} at {name} {ti}")

    for brand in enhanced_brands[:15]:
        dt = random.choice(deal_types); ti = random.choice(time_indicators)
        all_q.append(f"{brand} gluten free {dt} {ti}")
        all_q.append(f"{brand} {dt} gluten free products {ti}")

    random.shuffle(all_q)
    mid = len(all_q)//2
    return {"serpapi": all_q[:mid], "tavily": all_q[mid:]}

def is_high_quality_deal(item: Dict) -> bool:
    try:
        title = str(item.get("title", "") or "")
        snippet = str(item.get("snippet", "") or "")
        link = str(item.get("link", "") or "")
        text = (title + " " + snippet).lower()
        if not any(kw.lower() in text for kw in GF_KEYWORDS):
            return False
        quality_score = 0
        strong_patterns = [
            r'\d+%\s*off', r'\$\d+\.?\d*\s*off', r'save\s*\$\d+', r'buy\s+\d+\s+get\s+\d+',
            r'for\s+\$\d+', r'printable coupon', r'digital coupon', r'free shipping', r'rebate', r'cashback'
        ]
        for pat in strong_patterns:
            if re.search(pat, text, re.I): quality_score += 3
        if re.search(r'(promo|coupon|discount)\s*code', text, re.I): quality_score += 2
        for sig in ["deal","sale","promotion","special","clearance","markdown"]:
            if sig in text: quality_score += 2
        fresh_keywords = ['today','current','active','new','latest','this week', datetime.now().strftime('%B').lower()]
        if any(kw in text for kw in fresh_keywords): quality_score += 2
        if any(domain in link.lower() for domain in DEAL_FOCUSED_STORES.keys()): quality_score += 5
        coupon_code = item.get("coupon_code", "N/A").upper()
        if coupon_code in INVALID_COUPON_TERMS or len(coupon_code) > 20: quality_score -= 5
        if item.get("discount_amount") == "N/A": quality_score -= 2
        return quality_score >= 3
    except Exception as e:
        print(f"❌ Error in quality check: {e}")
        return False

def extract_comprehensive_deal_details(item: Dict) -> Dict:
    try:
        title = str(item.get('title','') or '')
        snippet = str(item.get('snippet','') or '')
        link = str(item.get('link','') or '')
        text = f"{title} {snippet}".lower()
        details = {"deal_type":"N/A","discount_amount":"N/A","coupon_code":"N/A","expiration":"N/A","restrictions":"N/A"}
        for pattern, dtype in [
            (r'coupon|promo code|discount code','Coupon/Promo Code'),
            (r'rebate|cashback|cash back','Rebate/Cashback'),
            (r'sale|clearance|\d+%\s*off','Sale/Discount'),
            (r'free shipping','Free Shipping'),
            (r'bogo|buy one get|buy \d+ get','BOGO/Bundle'),
            (r'printable|digital coupon','Printable/Digital Coupon'),
            (r'limited time|while supplies last','Limited Time Offer'),
            (r'flash sale|daily deal','Flash/Daily Deal'),
            (r'member|exclusive|app only','Exclusive Deal')
        ]:
            if re.search(pattern, text, re.I): details["deal_type"]=dtype; break
        for pattern, fmt in [
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
        ]:
            m = re.search(pattern, text, re.I)
            if m: details["discount_amount"]=re.sub(pattern, fmt, m.group(0), flags=re.I); break
        for pattern in [
            r'(?:code|use|enter|promo|coupon)[:;\s]*([A-Z0-9]{4,20})\b',
            r'\b(SAVE[A-Z0-9]+)\b', r'\b(GET[A-Z0-9]+)\b',
            r'\b([A-Z]{3,}[0-9]{2,})\b', r'\b([A-Z0-9]{6,15})\b',
            r'code\s*[:;]\s*([A-Z0-9]+)', r'"([A-Z0-9]{4,})"'
        ]:
            matches = re.findall(pattern, text, re.I)
            for m in matches:
                raw = m.upper() if isinstance(m, str) else m[0].upper()
                if raw in INVALID_COUPON_TERMS or len(raw) > 15: continue
                if re.fullmatch(r'[A-Z0-9]{4,}', raw): details["coupon_code"]=raw; break
            if details["coupon_code"]!="N/A": break
        for pattern in [
            r'expires? (?:on|at|in|by)?\s*([^\.,\n]+)',
            r'valid (?:through|until|by)?\s*([^\.,\n]+)',
            r'good (?:through|until)?\s*([^\.,\n]+)',
            r'offer ends?\s*([^\.,\n]+)',
            r'while supplies last|limited time|quantities limited'
        ]:
            m = re.search(pattern, text, re.I)
            if m:
                fm = m.group(0).strip()
                details["expiration"] = "While Supplies Last" if 'while supplies last' in fm.lower() else fm
                break
        if any(k in text for k in ['restrictions apply','exclusions','not valid with other offers','one per customer']):
            details["restrictions"] = "Restrictions apply"
        return details
    except Exception as e:
        print(f"❌ Error extracting deal details: {e}")
        return {"deal_type":"N/A","discount_amount":"N/A","coupon_code":"N/A","expiration":"N/A","restrictions":"N/A"}

def search_serpapi_enhanced(query: str) -> List[Dict]:
    if not SERPAPI_KEY:
        print("⚠️  SerpAPI key not configured.")
        return []
    url = "https://serpapi.com/search"
    all_items, seen_links = [], set()
    engines = ["google","bing","duckduckgo"]
    for engine in engines:
        params = {"engine":engine,"q":query,"api_key":SERPAPI_KEY,"num":20,"gl":"us","hl":"en","safe":"off","tbs":"qdr:m"}
        print(f"-> SerpAPI [{engine}]: {query}")
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                print(f"⚠️  Rate limited on {engine}, waiting..."); time.sleep(10); continue
            r.raise_for_status(); data = r.json()
            if "error" in data: print(f"❌ SerpAPI error on {engine}: {data['error']}"); continue
        except Exception as e:
            print(f"❌ Error searching SerpAPI [{engine}]: {e}"); continue
        engine_items = 0
        for item in data.get("organic_results", []):
            link = item.get("link","")
            if not link or link in seen_links: continue
            result = {"title":item.get("title",""),"snippet":item.get("snippet",""),"link":link,"source":f"SerpAPI ({engine})","timestamp":datetime.now().isoformat()}
            if is_high_quality_deal(result):
                result.update(extract_comprehensive_deal_details(result))
                seen_links.add(link); all_items.append(result); engine_items += 1
        print(f"  ✅ {engine_items} high-quality deals from {engine}")
        time.sleep(random.uniform(2,4))
    print(f"  📊 Total SerpAPI deals: {len(all_items)}")
    return all_items

def search_tavily_enhanced(query: str) -> List[Dict]:
    if not TAVILY_API_KEY:
        print("⚠️  Tavily key not configured.")
        return []
    print(f"-> Tavily: {query}")
    url = "https://api.tavily.com/search"
    payload = {"api_key":TAVILY_API_KEY,"query":query,"search_depth":"advanced","include_answer":True,"max_results":25,"include_domains":list(DEAL_FOCUSED_STORES.keys()),"days":30}
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT); r.raise_for_status(); data = r.json()
    except Exception as e:
        print(f"❌ Error searching Tavily: {e}"); return []
    items = []
    answer = (data.get("answer","") or "").strip()
    if answer and len(answer) > 50:
        ans_item = {"title": f"AI Summary: {query[:50]}...","snippet": answer[:500],"link":"N/A","source":"Tavily AI Summary","timestamp":datetime.now().isoformat()}
        if is_high_quality_deal(ans_item):
            ans_item.update(extract_comprehensive_deal_details(ans_item)); items.append(ans_item)
    valid = 0
    for ritem in data.get("results", []):
        item = {"title":ritem.get("title",""),"snippet":(ritem.get("content","") or "")[:600],"link":ritem.get("url",""),"source":"Tavily","timestamp":datetime.now().isoformat()}
        if is_high_quality_deal(item):
            item.update(extract_comprehensive_deal_details(item)); items.append(item); valid += 1
    print(f"  ✅ {valid} high-quality Tavily deals")
    return items

def ai_powered_deal_validation(deals: List[Dict]) -> List[Dict]:
    print(f"🤖 AI-powered validation of {len(deals)} deals...")
    validated, dbg = [], {"passed":0,"failed_gf":0,"failed_score":0}
    for i, deal in enumerate(deals):
        try:
            if not isinstance(deal, dict): continue
            title = str(deal.get('title','') or ''); snippet = str(deal.get('snippet','') or ''); link = str(deal.get('link','') or '')
            text = f"{title} {snippet}".lower()
            if not any(kw.lower() in text for kw in GF_KEYWORDS): dbg["failed_gf"] += 1; continue
            quality = 0
            for pat in [r'\d+%\s*off', r'\$\d+\.?\d*\s*off', r'save\s*\$\d+', r'buy\s+\d+\s+get\s+\d+', r'printable coupon', r'digital coupon', r'free shipping', r'rebate', r'cashback']:
                if re.search(pat, text, re.I): quality += 3
            for sig in ["deal","sale","promotion","special","clearance","coupon","discount"]:
                if sig in text: quality += 2
            if any(k in text for k in ['today','current','active','new','latest', datetime.now().strftime('%B').lower()]): quality += 2
            if any(d in link.lower() for d in DEAL_FOCUSED_STORES.keys()): quality += 3
            deal["ai_quality_score"] = quality
            if quality >= 2: validated.append(deal); dbg["passed"] += 1
            else: dbg["failed_score"] += 1
        except Exception as e:
            print(f"❌ Error validating deal #{i+1}: {e}")
    validated.sort(key=lambda x: x.get("ai_quality_score",0), reverse=True)
    print(f" ✅ Validation complete: {dbg}")
    return validated

def enhance_deal_metadata(deals: List[Dict]) -> List[Dict]:
    enhanced = []
    for deal in deals:
        try:
            if not isinstance(deal, dict): continue
            link = deal.get("link","") or ""; title = str(deal.get("title","") or ""); snippet = str(deal.get("snippet","") or ""); text = f"{title} {snippet}".lower()
            store = "Unknown"
            try:
                if link.startswith(('http://','https://')):
                    parsed = urlparse(link); domain = parsed.netloc.lower().replace("www.","")
                    if domain in DOMAIN_TO_BRAND:
                        store = DOMAIN_TO_BRAND[domain]
                    else:
                        parts = domain.split('.')
                        if parts and parts[0] in DOMAIN_TO_BRAND:
                            store = DOMAIN_TO_BRAND[parts[0]]
                        else:
                            for dk, bn in DOMAIN_TO_BRAND.items():
                                if dk in domain or dk in link.lower(): store = bn; break
                            if store == "Unknown":
                                for sn in ['target','walmart','kroger','costco','amazon','whole foods']:
                                    if sn in text: store = sn.title(); break
            except Exception as e:
                print(f"⚠️ Error parsing URL {link}: {e}")
            deal["store"] = store

            brand = deal.get("brand","Multiple/Various")
            if brand == "Multiple/Various":
                brand = store if store != "Unknown" else "Multiple/Various"
                for bn in enhanced_brands:
                    if bn.lower() in text: brand = bn; break
            deal["brand"] = brand

            food_terms = ['snack','food','meal','cookie','cracker','bread','pizza']
            frozen_terms = ['frozen','ice cream','entree']
            deal["category"] = "Food" if any(t in text for t in food_terms) else ("Frozen" if any(t in text for t in frozen_terms) else "General")
            enhanced.append(deal)
        except Exception as e:
            print(f"❌ Error enhancing deal: {e}")
            deal.setdefault("store","Unknown"); deal.setdefault("brand","Multiple/Various"); deal.setdefault("category","General")
            enhanced.append(deal)
    return enhanced

def is_real_deal(item: Dict) -> bool:
    link = item.get("link","") or ""
    title = str(item.get("title","")).lower()
    snippet = str(item.get("snippet","")).lower()
    text = f"{title} {snippet}"
    if not link.startswith(("http://","https://")): return False
    if not any(kw in text for kw in [k.lower() for k in GF_KEYWORDS]): return False
    if not any(sig in text for sig in [d.lower() for d in DEAL_INDICATORS]): return False
    return True

# ---------------- Main pipeline ----------------
def main() -> List[Dict]:
    print("🚀 AI-ENHANCED Gluten-Free Deals & Coupons Scraper v2.0")
    print(f"📅 Searching for current deals as of {datetime.now().strftime('%B %d, %Y')}")
    print("🤖 Powered by OpenAI LLM + Advanced AI Filtering")
    print("🎯 Focus: 100% VERIFIED gluten-free deals, coupons, and promo codes")

    missing = []
    if not OPENAI_API_KEY: missing.append("OpenAI")
    if not SERPAPI_KEY:    missing.append("SerpAPI")
    if not TAVILY_API_KEY: missing.append("Tavily")
    if missing: raise RuntimeError(f"Missing API keys for: {', '.join(missing)}")

    try:
        queries = generate_comprehensive_queries()
    except Exception as e:
        print(f"❌ Query generation failed: {e} — using fallback")
        queries = generate_comprehensive_fallback_queries()

    all_results: List[Dict] = []
    if SERPAPI_KEY:
        print(f"\n🔍 Running {len(queries['serpapi'])} SerpAPI queries…")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as execp:
            futures = [execp.submit(search_serpapi_enhanced, q) for q in queries['serpapi']]
            for fut in concurrent.futures.as_completed(futures):
                all_results.extend(fut.result()); time.sleep(random.uniform(1,2))
    if TAVILY_API_KEY:
        print(f"\n🔍 Running {len(queries['tavily'])} Tavily queries…")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as execp:
            futures = [execp.submit(search_tavily_enhanced, q) for q in queries['tavily']]
            for fut in concurrent.futures.as_completed(futures):
                all_results.extend(fut.result()); time.sleep(random.uniform(1,2))

    print(f"\n📊 Collected {len(all_results)} raw results")
    if not all_results: raise RuntimeError("No results found. Check your API keys or service availability.")

    all_results = [item for item in all_results if is_real_deal(item)]
    print(f"📊 {len(all_results)} after basic real-deal filtering")

    validated = ai_powered_deal_validation(all_results)
    enhanced  = enhance_deal_metadata(validated)

    # 🔁 Firestore replace-all
    run_ts = datetime.utcnow()
    _ = replace_deals_firestore(enhanced, run_ts)

    print(f"\n🎉 SUCCESS: {len(enhanced)} premium deals saved to Firestore")
    return enhanced

# ---------------- HTTP entry point ----------------
from flask import jsonify, Request

def get_gluten_free_deals(request: Request):
    """
    Synchronous HTTP entry point.
    Runs the scrape and replaces Firestore collection.
    NOTE: This may take several minutes. Deploy with long timeout in Gen2.
    """
    try:
        deals = main()
        return jsonify({
            "generatedAt": datetime.utcnow().isoformat() + "Z",
            "count": len(deals),
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------------- Local run ----------------
if __name__ == "__main__":
    start = datetime.now()
    try:
        deals = main()
    except Exception as err:
        print(f"❌ Execution error: {err}")
    else:
        elapsed = (datetime.now() - start).total_seconds()
        print(f"⏱️  Total runtime: {elapsed:.1f} seconds")