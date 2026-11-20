"""
Pure Gamer — Automated Multi-Source Gaming News & Reviews Magazine Pipeline
Categories:
1. GlobalNews: IGN, GameSpot, PC Gamer, Eurogamer, VGC
2. JapanNews: IGN Japan, 4Gamer.net, Automaton JP, Gematsu
3. GlobalReview: GameSpot Reviews, Eurogamer Reviews
4. JapanReview: IGN Japan Reviews, 4Gamer Reviews/Impressions

Features:
- Multi-source story clustering & cross-feed context attachment.
- Copyright-safe editorial synthesis (Gemini acts as Pure Gamer Magazine Editor).
- Multi-source attribution (`sources` array) with original source links.
- Preserves Minimal Dark UI, TTS Thai voice reader, A-/A+ font sizing, 16:9 covers,
  inline screenshots, and YouTube trailer embeds.
"""
import calendar
import concurrent.futures
import datetime
import html
import json
import logging
import os
import random
import re
import sys
import threading
import time
import urllib.parse

import feedparser
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("PureGamerPipeline")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_FILE = os.path.join(DATA_DIR, "news.json")
INDEX_HTML = os.path.join(BASE_DIR, "index.html")
README_FILE = os.path.join(BASE_DIR, "README.md")


def _load_env_value(key: str) -> str:
    val = (os.getenv(key) or "").strip()
    if not val:
        env_file = os.path.join(BASE_DIR, ".env")
        if os.path.exists(env_file):
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line_s = line.strip()
                    if line_s and not line_s.startswith("#") and line_s.startswith(f"{key}="):
                        val = line_s.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    return val


GEMINI_API_KEY = _load_env_value("GEMINI_API_KEY")
GEMINI_MODEL = _load_env_value("GEMINI_MODEL") or "gemini-3.1-flash-lite"

try:
    TARGET_PER_CATEGORY = max(1, int(_load_env_value("MAX_ARTICLES") or "5"))
except ValueError:
    TARGET_PER_CATEGORY = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

_rate_limit_lock = threading.Lock()
_last_api_call_ts = 0.0
_daily_quota_exhausted = False
try:
    MIN_API_INTERVAL_SEC = float(_load_env_value("MIN_API_INTERVAL_SEC") or "4.5")
except ValueError:
    MIN_API_INTERVAL_SEC = 4.5

THAI_MONTHS = [
    "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.",
    "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."
]

TRACKING_QUERY_PARAMS = {
    "fbclid", "gclid", "ref", "source", "ncid", "_ga", "mc_cid", "mc_eid", "cmpid"
}

BAD_IMG_KEYWORDS = [
    "flexiimages", ".svg", "nosize", "flags/", "logo", "avatar", "icon",
    "user-", "banner", "badge", "tracking", "sponsored", "pixel", "ad-",
    "scorecardresearch", "placeholder", "author", "profile", "button", "1x1",
    "ign30-logo", "ign_kw_logo", "/user/", "/avatars/", "data:image", "gravatar"
]

CURATED_GAMING_COVERS = [
    "https://images.unsplash.com/photo-1550745165-9bc0b252726f?w=1200&q=80",
    "https://images.unsplash.com/photo-1542751371-adc38448a05e?w=1200&q=80",
    "https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80",
    "https://images.unsplash.com/photo-1538481199705-c710c4e965fc?w=1200&q=80",
    "https://images.unsplash.com/photo-1579373903781-fd5c0c30c4cd?w=1200&q=80",
    "https://images.unsplash.com/photo-1563089145-599997674d42?w=1200&q=80",
    "https://images.unsplash.com/photo-1518709268805-4e9042af9f23?w=1200&q=80",
    "https://images.unsplash.com/photo-1612287233207-6b4d32a90ca4?w=1200&q=80",
    "https://images.unsplash.com/photo-1580234811497-9df7fd2f357e?w=1200&q=80",
    "https://images.unsplash.com/photo-1509198397868-475647b2a1e5?w=1200&q=80",
]

CATEGORY_FALLBACK_COVERS = {
    "GlobalNews": "https://images.unsplash.com/photo-1542751371-adc38448a05e?w=1200&q=80",
    "JapanNews": "https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80",
    "GlobalReview": "https://images.unsplash.com/photo-1538481199705-c710c4e965fc?w=1200&q=80",
    "JapanReview": "https://images.unsplash.com/photo-1550745165-9bc0b252726f?w=1200&q=80",
}

CATEGORIES_ORDER = ["GlobalNews", "JapanNews", "GlobalReview", "JapanReview"]

CATEGORIES_CONFIG = {
    "GlobalNews": [
        "https://feeds.feedburner.com/ign/games-all",
        "https://www.gamespot.com/feeds/game-news/",
        "https://www.pcgamer.com/rss/",
        "https://www.eurogamer.net/feed/news",
        "https://www.videogameschronicle.com/feed/",
    ],
    "JapanNews": [
        "https://jp.ign.com/games.xml",
        "https://www.4gamer.net/rss/index.xml",
        "https://automaton-media.com/feed/",
        "https://www.gematsu.com/feed",
    ],
    "GlobalReview": [
        "https://www.gamespot.com/feeds/reviews/",
        "https://www.eurogamer.net/feed/reviews",
    ],
    "JapanReview": [
        "https://jp.ign.com/review.xml",
        "https://www.4gamer.net/rss/index.xml",
    ],
}

CATEGORY_LABELS = {
    "GlobalNews": "🌍 GLOBAL NEWS",
    "JapanNews": "🇯🇵 JAPAN NEWS",
    "GlobalReview": "⭐ GLOBAL REVIEWS",
    "JapanReview": "⛩️ JAPAN REVIEWS",
}

GAME_SUBTAG_PATTERNS = [
    (r"zelda|ocarina|ゼルダ", "ZELDA"),
    (r"metal\s*gear|メタルギア", "METAL GEAR"),
    (r"monster\s*hunter|モンスターハンター|モンハン", "MONSTER HUNTER"),
    (r"fire\s*emblem|ファイアーエムブレム", "FIRE EMBLEM"),
    (r"call\s*of\s*duty|modern\s*warfare", "CALL OF DUTY"),
    (r"grand\s*theft\s*auto|\bgta\b", "GTA"),
    (r"final\s*fantasy|ファイナルファンタジー", "FINAL FANTASY"),
    (r"dragon\s*quest|ドラゴンクエスト|ドラクエ", "DRAGON QUEST"),
    (r"elder\s*scrolls", "ELDER SCROLLS"),
    (r"dragon.?s\s*dogma", "DRAGON'S DOGMA"),
    (r"silent\s*hill|サイレントヒル", "SILENT HILL"),
    (r"resident\s*evil|biohazard|バイオハザード", "RESIDENT EVIL"),
    (r"elden\s*ring|エルデンリング", "ELDEN RING"),
    (r"onimusha|鬼武者", "ONIMUSHA"),
    (r"castlevania|悪魔城", "CASTLEVANIA"),
    (r"ace\s*combat|エースコンバット", "ACE COMBAT"),
    (r"gears\s*of\s*war", "GEARS OF WAR"),
    (r"mega\s*man|ロックマン", "MEGA MAN"),
    (r"league\s*of\s*legends|\blol\b", "LEAGUE OF LEGENDS"),
    (r"phantom\s*blade", "PHANTOM BLADE"),
    (r"minecraft|マインクラフト", "MINECRAFT"),
    (r"marvel|wolverine", "MARVEL"),
    (r"houkai|honkai|star\s*rail|崩壊", "HONKAI: STAR RAIL"),
    (r"genshin|原神", "GENSHIN IMPACT"),
    (r"persona|ペルソナ", "PERSONA"),
    (r"pokemon|pokémon|ポケモン", "POKEMON"),
    (r"mario|マリオ", "MARIO"),
    (r"yakuza|like\s*a\s*dragon|龍が如く", "LIKE A DRAGON"),
    (r"switch[\s-]*2", "SWITCH 2"),
    (r"\bps5\b|playstation[\s-]*5|プレイステーション", "PS5"),
    (r"\bxbox\b", "XBOX"),
    (r"\bsteam\b|pc\s*gaming", "PC GAMING"),
]

STOPWORDS_EN = {
    "about", "after", "again", "against", "almost", "already", "also", "among",
    "another", "around", "article", "because", "before", "being", "below",
    "between", "both", "could", "daily", "deals", "during", "every", "first",
    "from", "game", "games", "gaming", "getting", "going", "great", "here",
    "hours", "into", "just", "latest", "launch", "launches", "like", "made",
    "make", "makes", "making", "more", "most", "much", "never", "news", "next",
    "night", "official", "only", "other", "over", "part", "players", "playing",
    "please", "post", "release", "released", "releases", "report", "reports",
    "reveal", "revealed", "reveals", "review", "reviews", "said", "says", "show",
    "shows", "since", "some", "still", "story", "such", "than", "that", "their",
    "them", "then", "there", "these", "they", "this", "those", "through",
    "time", "today", "trailer", "under", "until", "update", "updated", "using",
    "very", "video", "want", "week", "weeks", "were", "what", "when", "where",
    "which", "while", "will", "with", "would", "year", "years", "your"
}


def get_source_name(url: str) -> str:
    """Map feed or article URL domain to a clean publication name."""
    u = (url or "").lower()
    if "jp.ign.com" in u:
        return "IGN Japan"
    if "ign.com" in u or "feedburner.com/ign" in u:
        return "IGN"
    if "gamespot.com" in u:
        return "GameSpot"
    if "pcgamer.com" in u:
        return "PC Gamer"
    if "eurogamer.net" in u:
        return "Eurogamer"
    if "videogameschronicle.com" in u:
        return "VGC"
    if "4gamer.net" in u:
        return "4Gamer"
    if "automaton-media.com" in u:
        return "Automaton JP"
    if "gematsu.com" in u:
        return "Gematsu"
    try:
        host = urllib.parse.urlsplit(u).netloc.replace("www.", "")
        return host.split(".")[0].upper() if host else "SOURCE"
    except Exception:
        return "SOURCE"


def extract_sub_tag(item: dict) -> str:
    """Derive a clean uppercase sub-category tag from URL path and article title/bullets."""
    cat = item.get("tag") or item.get("category_hint") or ""
    link = item.get("link", "")
    title_raw = (item.get("original_title") or item.get("title") or "") + " " + (item.get("thai_title") or "")
    title = title_raw.lower()
    bullets_txt = " ".join(item.get("bullet_points") or [])[:400].lower()

    seg = ""
    if link:
        try:
            parts = urllib.parse.urlsplit(link).path.strip("/").split("/")
            seg = parts[0].lower() if parts and parts[0] else ""
        except Exception:
            seg = ""

    body = f"{seg.replace('-', ' ')} {title} {bullets_txt}"
    for pat, label in GAME_SUBTAG_PATTERNS:
        if re.search(pat, body, re.IGNORECASE):
            return label

    if "switch 2" in body or "nintendo" in body or "任天堂" in body:
        return "NINTENDO"
    if "ps5" in body or "playstation" in body:
        return "PLAYSTATION"
    if "xbox" in body:
        return "XBOX"
    if "steam" in body or "pc" in body:
        return "PC GAMING"
    if cat in ("GlobalReview", "JapanReview"):
        return "GAME REVIEW"
    return "GAMING"


SYSTEM_PROMPT = """คุณคือ "บรรณาธิการบริหารนิตยสารเกม Pure Gamer" (Executive Gaming Magazine Editor & Journalist)

ภารกิจหลัก (สำคัญมากที่สุด — กฎเรื่องลิขสิทธิ์และคุณภาพข่าว):
- ห้ามแปลบทความต้นฉบับแบบย่อหน้าต่อย่อหน้า (No 1:1 Paragraph Translation) เด็ดขาด เพื่อไม่ให้ละเมิดลิขสิทธิ์โครงสร้างบทความของสื่อต้นทาง
- ให้ทำหน้าที่สกัดเฉพาะ "ข้อเท็จจริง (Facts)" จากบทความต้นฉบับทั้งหมดที่แนบให้ (อาจมาจาก 1 แหล่ง หรือหลายสำนักข่าวพร้อมกัน) แล้วนำมา "สังเคราะห์และเขียนเรียบเรียงขึ้นใหม่ทั้งหมดด้วยสำนวนของนิตยสารเกม Pure Gamer"
- ห้ามแต่งเติมข้อมูล ตัวเลข วันที่ คะแนนรีวิว หรือคำพูดที่ไม่ได้ปรากฏอยู่ในข้อมูลต้นทางโดยเด็ดขาด
- หากข้อมูลใดเป็นข่าวลือ (Rumor/Leak) ต้องระบุให้ชัดว่าเป็นข่าวลือหรือรายงานจากแหล่งข่าวนั้นๆ
- หากมีการอ้างอิงรายละเอียดเด่น ให้ระบุชื่อสำนักข่าวต้นทางอย่างเป็นธรรมชาติในเนื้อเรื่อง (เช่น "ตามรายงานของ IGN และ PC Gamer..." หรือ "สื่อญี่ปุ่นอย่าง 4Gamer และ IGN Japan เปิดเผยว่า...")

โครงสร้างบทความเรียบเรียงใหม่ ("full_paragraphs"):
- ความยาว 4–6 ย่อหน้า อ่านสนุก กระชับ ชวนติดตามเหมือนคอลัมน์ในนิตยสารเกมระดับพรีเมียม:
  * ย่อหน้าที่ 1 (The Lead): เปิดเรื่องให้น่าสนใจ สรุปใจความสำคัญที่สุดว่าเกิดอะไรขึ้น กับเกมอะไร ค่ายไหน แพลตฟอร์มใด
  * ย่อหน้าที่ 2–4 (Synthesized In-Depth Report): เจาะลึกรายละเอียด ฟีเจอร์ ระบบเกมเพลย์ กำหนดการ หรือประเด็นสำคัญ โดยผสมผสานข้อมูลจากทุกแหล่งข่าวที่แนบให้เข้าด้วยกัน
  * ย่อหน้าสุดท้าย (Editor's Note / Context): สรุปบริบทของเกมนี้ในตลาด หรือจุดที่เกมเมอร์ไทยควรจับตามองต่อไป

ข้อกำหนดเพิ่มเติม:
1. พาดหัวข่าว ("thai_title"): เขียนพาดหัวข่าวภาษาไทยสไตล์นิตยสารเกมให้กระชับ ดึงดูด ตรงตามข้อเท็จจริง (ห้ามคืนค่าเป็นภาษาต้นฉบับล้วน)
2. สรุปประเด็นสำคัญ ("bullet_points"): สรุปสาระสำคัญ 4–5 ข้อสั้นๆ อ่านจบใน 10 วินาที
3. ชื่อเกมและศัพท์เฉพาะ:
   - แปลงชื่อเกมภาษาญี่ปุ่น/คาตาคานะ เป็นชื่อภาษาอังกฤษสากลที่เกมเมอร์ไทยคุ้นเคยเสมอ (เช่น 『ゼルダの伝説』 -> The Legend of Zelda) ห้ามแปลชื่อเกมเป็นภาษาไทยแบบตรงตัว
   - คงศัพท์เกมเมอร์และแพลตฟอร์มสากล (PS5, Xbox Series X|S, Switch 2, Steam, RPG, Roguelike, DLC, Patch, Frame Rate, Early Access)
4. กฎแท็ก ("tag"): ให้คืนค่าเป็นหนึ่งในสี่หมวดนี้เท่านั้น: "GlobalNews", "JapanNews", "GlobalReview", "JapanReview"
"""

CATEGORY_PROMPTS = {
    "GlobalNews": """บริบทเฉพาะหมวด (Global Gaming News — สื่อฝั่งตะวันตก):
- แหล่งข่าวมาจากสื่อเกมชั้นนำฝั่งตะวันตก (เช่น IGN, GameSpot, PC Gamer, Eurogamer, VGC)
- เรียบเรียงข่าวในมุมมองกองบรรณาธิการข่าวเกมระดับโลก เน้นความชัดเจนของข้อมูลแพลตฟอร์ม วันวางจำหน่าย สเปก PC/คอนโซล และความเคลื่อนไหวในอุตสาหกรรมเกม""",
    "JapanNews": """บริบทเฉพาะหมวด (Japan Gaming News — สื่อฝั่งญี่ปุ่น & ข่าวเกมญี่ปุ่น):
- แหล่งข่าวมาจากสื่อเกมญี่ปุ่นชั้นนำ (เช่น IGN Japan, 4Gamer, Automaton JP, Gematsu)
- แปลงชื่อเกม ชื่อตัวละคร และค่ายเกมญี่ปุ่นเป็นชื่อสากลให้ถูกต้อง (เช่น Nintendo, Capcom, Square Enix, Konami, FromSoftware, Bandai Namco, Sega, Atlus)
- ถ่ายทอดเสน่ห์และบริบทของวงการเกมญี่ปุ่นให้เกมเมอร์ไทยอ่านเข้าใจง่าย""",
    "GlobalReview": """บริบทเฉพาะหมวด (Global Game Reviews — คอลัมน์สรุปรีวิวจากสื่อตะวันตก):
- เรียบเรียงเป็นคอลัมน์สรุปรีวิวฉบับบรรณาธิการ 4–6 ย่อหน้า โดยห้ามคัดลอกโครงสร้างรีวิวต้นฉบับ:
  * ย่อหน้าแรก: แนะนำเกมและภาพรวมความรู้สึกที่มีต่อเกมนี้
  * ย่อหน้ากลาง: สรุปจุดเด่นที่สื่อชื่นชม (Pros) ทั้งเกมเพลย์ เนื้อเรื่อง งานภาพ และจุดสังเกต/ข้อจำกัด (Cons)
  * ย่อหน้าสุดท้าย: บทสรุปความคุ้มค่า (Verdict) ว่าเกมนี้เหมาะกับเกมเมอร์สายไหน
- หากในเนื้อหามีคะแนนรีวิว (เช่น 8/10 หรือ 4/5 ดาว) ให้แปลงเป็นคะแนนเต็ม 10 ใส่ในฟิลด์ "review_score" (เช่น "8" หรือ "8.5") ถ้าไม่มีให้เว้นว่าง "" """,
    "JapanReview": """บริบทเฉพาะหมวด (Japan Game Reviews — คอลัมน์สรุปรีวิวจากสื่อญี่ปุ่น):
- เรียบเรียงบทความรีวิวจากสื่อญี่ปุ่น (IGN Japan / 4Gamer) ให้เป็นคอลัมน์สรุปรีวิวสไตล์นิตยสารเกม Pure Gamer 4–6 ย่อหน้า:
  * ภาพรวมของเกมเพลย์และการออกแบบโลก/ระบบต่อสู้
  * จุดเด่น (Pros) และจุดสังเกต (Cons) ตามข้อมูลจากนักวิจารณ์ญี่ปุ่น
  * บทสรุปความคุ้มค่า (Verdict) สำหรับผู้เล่น
- แปลงชื่อเกมญี่ปุ่นเป็นชื่อสากลเสมอ และหากมีคะแนนรีวิวเต็ม 10 ให้ระบุในฟิลด์ "review_score" (ถ้าไม่มีให้เว้นว่าง "")"""
}


def has_thai_chars(text: str) -> bool:
    return bool(text and any("\u0e00" <= c <= "\u0e7f" for c in text))


def clean_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    clean = re.sub(r"<[^>]+>", "", raw_html).strip()
    return " ".join(html.unescape(clean).split())


def normalize_url(url: str) -> str:
    """Canonicalize URL by enforcing HTTPS, lowercasing host, removing tracking params and trailing slash."""
    if not url:
        return ""
    raw = url.strip()
    try:
        parts = urllib.parse.urlsplit(raw)
    except Exception:
        return raw
    if not parts.scheme and not parts.netloc:
        return raw.rstrip("/")

    scheme = "https" if parts.scheme.lower() in ("http", "https", "") else parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or ""
    if len(path) > 1:
        path = path.rstrip("/")

    filtered_q = []
    for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True):
        kl = k.lower()
        if kl.startswith("utm_") or kl.startswith("at_") or kl in TRACKING_QUERY_PARAMS:
            continue
        filtered_q.append((k, v))
    query = urllib.parse.urlencode(filtered_q, doseq=True)

    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))


def normalize_guid(guid: str, link: str = "") -> str:
    """Normalize entry GUID/ID or fall back to canonicalized link."""
    raw = (guid or "").strip()
    if raw:
        if raw.lower().startswith(("http://", "https://")):
            return normalize_url(raw)
        return raw
    return normalize_url(link)


def to_thailand_time(dt_utc: datetime.datetime) -> str:
    """Convert UTC datetime to Thailand Time (UTC+7) string."""
    th_tz = datetime.timezone(datetime.timedelta(hours=7))
    th_dt = dt_utc.astimezone(th_tz)
    day = th_dt.day
    month = THAI_MONTHS[th_dt.month - 1]
    year = th_dt.year
    hour = th_dt.strftime("%H:%M")
    return f"{day} {month} {year} • {hour} น."


def parse_existing_timestamp(item: dict, fallback_utc: datetime.datetime) -> datetime.datetime:
    """Parse stored timestamps (published_ts, published_at, or published_th) into UTC datetime."""
    ts = item.get("published_ts")
    if isinstance(ts, (int, float)) and ts > 0:
        return datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc)

    pub_at = (item.get("published_at") or "").strip()
    if pub_at:
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M UTC", "%Y-%m-%d %H:%M:%S UTC"):
            try:
                dt = datetime.datetime.strptime(pub_at, fmt)
                return dt.replace(tzinfo=datetime.timezone.utc)
            except ValueError:
                pass
        try:
            iso_dt = datetime.datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
            if iso_dt.tzinfo is None:
                iso_dt = iso_dt.replace(tzinfo=datetime.timezone.utc)
            return iso_dt.astimezone(datetime.timezone.utc)
        except ValueError:
            pass

    th_tz = datetime.timezone(datetime.timedelta(hours=7))
    pub_th = (item.get("published_th") or "").strip()
    if pub_th:
        m2 = re.match(r"^(\d{1,2})\s+([^\s]+)\s+(\d{4})\s*[•·-]\s*(\d{2}):(\d{2})", pub_th)
        if m2:
            day_s, month_th, year_s, hour_s, min_s = m2.groups()
            if month_th in THAI_MONTHS:
                month_idx = THAI_MONTHS.index(month_th) + 1
                year_val = int(year_s)
                if year_val > 2400:
                    year_val -= 543
                return datetime.datetime(
                    year_val, month_idx, int(day_s), int(hour_s), int(min_s), tzinfo=th_tz
                ).astimezone(datetime.timezone.utc)

    return fallback_utc


def is_valid_translation(existing_item: dict) -> bool:
    """Return True if an article already has a valid Thai editorial synthesis."""
    if existing_item.get("_translation_failed"):
        return False
    thai_title = (existing_item.get("thai_title") or "").strip()
    if not has_thai_chars(thai_title):
        return False
    full_paras = existing_item.get("full_paragraphs")
    if not isinstance(full_paras, list) or not full_paras:
        return False
    if not any(has_thai_chars(p) for p in full_paras if isinstance(p, str)):
        return False
    bullets = existing_item.get("bullet_points")
    if not isinstance(bullets, list) or not bullets:
        return False
    return True


def make_hd_image_url(url: str) -> str:
    """Upgrade known gaming media thumbnails to HD resolution."""
    if not url:
        return ""
    url = url.strip()
    url = re.sub(r"\.\d+\.(jpg|jpeg|png|webp)(\?.*)?$", r".1200.\1", url, flags=re.IGNORECASE)
    if "assetsio.gnwcdn.com" in url:
        url = re.sub(r"width=\d+", "width=1200", url)
    if "gamespot.com" in url:
        url = re.sub(r"([?&])w=\d+", r"\1w=1200", url)
    if "futurecdn.net" in url:
        url = re.sub(r"-\d+-\d+\.jpg$", "-1920-80.jpg", url)
    url = re.sub(r"-\d+x\d+(\.\w+)$", r"\1", url)
    return url


def _is_usable_image_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        return False
    ul = u.lower()
    return not any(bad in ul for bad in BAD_IMG_KEYWORDS)


def _image_stem(url: str) -> str:
    """Return filename stem without size suffixes so cover images aren't duplicated in gallery."""
    if not url:
        return ""
    base = os.path.basename(url.split("?")[0]).lower()
    base = re.sub(r"\.\d+(\.\w+)$", r"\1", base)
    return base


def extract_entry_thumbnail(entry) -> str:
    """Extract best thumbnail URL across RSS/Atom standards."""
    media_content = entry.get("media_content") or []
    for mc in media_content:
        u = (mc.get("url") or "").strip()
        mtype = (mc.get("type") or "").lower()
        medium = (mc.get("medium") or "").lower()
        if u and (medium == "image" or mtype.startswith("image/") or (not mtype.startswith(("video/", "audio/")) and _is_usable_image_url(u))):
            return make_hd_image_url(u)

    media_thumb = entry.get("media_thumbnail") or []
    for mt in media_thumb:
        u = (mt.get("url") or "").strip()
        if _is_usable_image_url(u):
            return make_hd_image_url(u)

    enclosures = entry.get("enclosures") or []
    for enc in enclosures:
        u = (enc.get("href") or enc.get("url") or "").strip()
        etype = (enc.get("type") or "").lower()
        u_clean = u.split("?")[0].lower()
        if u and (etype.startswith("image/") or any(u_clean.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp"))):
            if _is_usable_image_url(u):
                return make_hd_image_url(u)

    html_blobs = []
    for c in (entry.get("content") or []):
        if isinstance(c, dict) and c.get("value"):
            html_blobs.append(c["value"])
    for field in ("summary", "description"):
        val = entry.get(field)
        if val:
            html_blobs.append(val)

    for blob in html_blobs:
        for img_m in re.finditer(r'<img[^>]+(?:src|data-src)=["\']([^"\']+)["\']', blob, re.IGNORECASE):
            cand = html.unescape(img_m.group(1)).strip()
            if _is_usable_image_url(cand):
                return make_hd_image_url(cand)

    return ""


def _extract_meta_image(page_html: str) -> str:
    patterns = [
        r'<meta[^>]+(?:property|name)=["\']og:image:secure_url["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']og:image:secure_url["\']',
        r'<meta[^>]+(?:property|name)=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']og:image["\']',
        r'<meta[^>]+(?:property|name)=["\']twitter:image(?::src)?["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']twitter:image(?::src)?["\']',
    ]
    for pat in patterns:
        m = re.search(pat, page_html, re.IGNORECASE)
        if m:
            cand = html.unescape(m.group(1)).strip()
            if _is_usable_image_url(cand):
                return make_hd_image_url(cand)
    return ""


PLATFORM_GENERIC_ENTITIES = {"PS5", "XBOX", "SWITCH 2", "PC GAMING", "NINTENDO", "PLAYSTATION"}


def _extract_topic_signature(title: str) -> tuple[set[str], set[str], set[str]]:
    """Extract English tokens, recognized game entities, and JP/quoted titles for clustering."""
    t = title or ""
    en_tokens = {
        w for w in re.findall(r"[a-zA-Z0-9]{4,}", t.lower())
        if w not in STOPWORDS_EN and not w.isdigit() and w not in {"xbox", "playstation", "nintendo", "steam", "switch"}
    }
    entities = set()
    for pat, label in GAME_SUBTAG_PATTERNS:
        if label in PLATFORM_GENERIC_ENTITIES:
            continue
        if re.search(pat, t, re.IGNORECASE):
            entities.add(label)

    quoted = set()
    for q in re.findall(r"[『「\"'“]([^』」\"'”]{2,40})[』」\"'”]", t):
        q_clean = q.strip().lower()
        if len(q_clean) >= 2:
            quoted.add(q_clean)
            entities.add(q_clean)
            for w in re.findall(r"[a-zA-Z0-9]{3,}", q_clean):
                if w not in STOPWORDS_EN:
                    en_tokens.add(w)
    return en_tokens, entities, quoted


def cluster_candidates(candidates: list[dict], all_pool: list[dict]) -> list[dict]:
    """
    Merge stories from multiple sources covering the same game/news event into clusters,
    and attach secondary cross-feed references (up to 3 sources per story).
    """
    clusters: list[dict] = []

    for item in candidates:
        en_toks, entities, quoted = _extract_topic_signature(item["title"])
        item_src = {"name": item["source_name"], "url": item["link"], "title": item["title"]}
        item_ts = item["published_ts"]

        matched_cluster = None
        for cl in clusters:
            cl_sources = {s["name"] for s in cl["sources"]}
            if item["source_name"] in cl_sources:
                continue
            if abs(item_ts - cl["published_ts"]) > 36 * 3600:
                continue

            cl_en, cl_ent, cl_quoted = cl["_sig"]
            shared_en = en_toks & cl_en
            shared_ent = entities & cl_ent
            shared_quoted = quoted & cl_quoted

            # Strong match: shared exact quoted game title, or >= 2 rare tokens,
            # or same recognized franchise entity + at least 1 overlapping event token
            is_match = (
                bool(shared_quoted)
                or len(shared_en) >= 2
                or (bool(shared_ent) and len(shared_en) >= 1)
            )
            if is_match:
                matched_cluster = cl
                break

        if matched_cluster:
            if len(matched_cluster["sources"]) < 4:
                matched_cluster["sources"].append(item_src)
                if item_ts > matched_cluster["published_ts"]:
                    matched_cluster["published_ts"] = item_ts
                    matched_cluster["published_at"] = item["published_at"]
                    matched_cluster["published_th"] = item["published_th"]
                if not matched_cluster.get("feed_image") and item.get("feed_image"):
                    matched_cluster["feed_image"] = item["feed_image"]
                if item.get("description") and item["description"] not in matched_cluster["extra_descriptions"]:
                    matched_cluster["extra_descriptions"].append(f"[{item['source_name']}] {item['description']}")
        else:
            cl_item = dict(item)
            cl_item["sources"] = [item_src]
            cl_item["extra_descriptions"] = []
            cl_item["_sig"] = (en_toks, entities, quoted)
            clusters.append(cl_item)

    # Cross-feed enrichment: attach secondary sources from global/japan pool mentioning same game entity within 24h
    for cl in clusters:
        if len(cl["sources"]) >= 3:
            continue
        cl_en, cl_ent, cl_quoted = cl["_sig"]
        if not cl_ent and not cl_quoted:
            continue
        cl_src_names = {s["name"] for s in cl["sources"]}
        cl_ts = cl["published_ts"]
        for other in all_pool:
            if other["source_name"] in cl_src_names:
                continue
            if abs(other["published_ts"] - cl_ts) > 24 * 3600:
                continue
            o_en, o_ent, o_quoted = _extract_topic_signature(other["title"])
            if (cl_quoted & o_quoted) or ((cl_ent & o_ent) and len(cl_en & o_en) >= 1):
                cl["sources"].append({
                    "name": other["source_name"],
                    "url": other["link"],
                    "title": other["title"],
                })
                cl_src_names.add(other["source_name"])
                if other.get("description"):
                    cl["extra_descriptions"].append(f"[{other['source_name']}] {other['description']}")
                if len(cl["sources"]) >= 3:
                    break

    return clusters


def _scrape_single_url(url: str, fallback_desc: str = "") -> dict:
    """Scrape paragraphs, cover image, gallery screenshots, YouTube videos, and review score from one URL."""
    lead_img = ""
    gallery_images = []
    videos = []
    clean_paras = []
    review_score = ""
    verdict_text = ""
    score_label = ""

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        if not resp.encoding or resp.encoding.lower() in ("iso-8859-1", "latin-1"):
            resp.encoding = resp.apparent_encoding or "utf-8"
        content = resp.text

        meta_img = _extract_meta_image(content)
        if meta_img:
            lead_img = meta_img

        # IGN Japan review score
        if "jp.ign.com" in url.lower():
            rev_block_m = re.search(
                r'<div class=["\']article-review["\']>(.*?)<div class=["\']page-content["\']\s+id=["\']bottom-list-reviews["\']>',
                content, re.S | re.I
            )
            rev_source = rev_block_m.group(1) if rev_block_m else content
            score_m = re.search(
                r'<span class=["\'][^"\']*hexagon-content["\']>\s*(?:<div>)?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:</div>)?',
                rev_source, re.I
            )
            if score_m:
                review_score = score_m.group(1).strip()
            score_text_m = re.search(r'<div class=["\']score-text["\']>([^<]+)</div>', rev_source, re.I)
            blurb_m = re.search(r'<div class=["\']blurb["\'][^>]*>(.*?)</div>', rev_source, re.S | re.I)
            verdict_text = clean_text(blurb_m.group(1)) if blurb_m else ""
            score_label = clean_text(score_text_m.group(1)) if score_text_m else ""
        else:
            # JSON-LD Rating fallback (e.g. Eurogamer 5-star -> scale to 10, GameSpot 10-point)
            ld_m = re.search(r'"ratingValue"\s*:\s*"?([0-9]+(?:\.[0-9]+)?)"?', content)
            best_m = re.search(r'"bestRating"\s*:\s*"?([0-9]+(?:\.[0-9]+)?)"?', content)
            if ld_m:
                try:
                    val = float(ld_m.group(1))
                    best = float(best_m.group(1)) if best_m else (5.0 if "eurogamer.net" in url.lower() else 10.0)
                    if best == 5.0 and val <= 5.0:
                        scaled = val * 2.0
                        review_score = str(int(scaled)) if scaled.is_integer() else f"{scaled:.1f}"
                    elif best == 10.0 and val <= 10.0:
                        review_score = str(int(val)) if val.is_integer() else f"{val:.1f}"
                except ValueError:
                    pass

        art_m = (
            re.search(r'<article[^>]+class=["\'][^"\']*article-page[^"\']*["\'][^>]*>(.*?)</article>', content, re.DOTALL | re.IGNORECASE)
            or re.search(r"<article[^>]*>(.*?)</article>", content, re.DOTALL | re.IGNORECASE)
            or re.search(r'<div[^>]+class=["\'][^"\']*(?:article[_-]body|entry-content|post-content|main[_-]body)[^"\']*["\'][^>]*>(.*?)</div>', content, re.DOTALL | re.IGNORECASE)
        )
        article_html = art_m.group(1) if art_m else content
        article_html = re.sub(r'<aside[^>]*class=["\'][^"\']*floatrecirc[^"\']*["\'][^>]*>.*?</aside>', "", article_html, flags=re.S | re.I)
        article_html = re.sub(r'<div[^>]*class=["\'][^"\']*recirc-title[^"\']*["\'][^>]*>.*?</div>', "", article_html, flags=re.S | re.I)
        article_html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", article_html, flags=re.S | re.I)

        raw_p = re.findall(
            r"<p[^>]*>(.*?)(?=<p[>\s]|</p>|<div|<section|<article|<h[1-6]|<blockquote|$)",
            article_html, re.DOTALL | re.IGNORECASE
        )
        bad_phrases = [
            "terms of service", "privacy policy", "cookie", "sign up", "newsletter",
            "subscribe", "all rights reserved", "follow us", "comments", "please login",
            "image source", "image caption", "image copyright", "published", "hours ago",
            "days ago", "minutes ago", "read more:", "also read:", "related:", "loading...",
            "advertisement", "sponsored", "listen to the latest", "watch:", "quiz:",
            "welcome to pc gamer club", "membership journey starts here",
            "keep exploring and earning", "free article view", "posts from this",
            "sign up for", "social links navigation", "please follow our community guidelines",
            "you must confirm your public display name",
        ]
        seen_p = set()
        for p in raw_p:
            clean = re.sub(r"<[^>]+>", "", p).strip()
            clean = " ".join(html.unescape(clean).split())
            if len(clean) > 20 and not any(bad in clean.lower() for bad in bad_phrases):
                if clean not in seen_p:
                    seen_p.add(clean)
                    clean_paras.append(clean)

        if verdict_text:
            verdict_prefix = f"Verdict [{review_score}/10" + (f" - {score_label}" if score_label else "") + "]: "
            verdict_line = verdict_prefix + verdict_text
            if verdict_line not in seen_p:
                clean_paras.append(verdict_line)

        img_tags = re.findall(r"<img[^>]+>", article_html, re.IGNORECASE)
        seen_stems = set()
        lead_stem = _image_stem(lead_img)
        if lead_stem:
            seen_stems.add(lead_stem)

        for tag_str in img_tags:
            m_src = (
                re.search(r'data-src=["\'](https?://[^"\']+)["\']', tag_str, re.IGNORECASE)
                or re.search(r'src=["\'](https?://[^"\']+)["\']', tag_str, re.IGNORECASE)
            )
            if not m_src:
                continue
            g = html.unescape(m_src.group(1)).strip()
            g_clean = g.split("?")[0].lower()
            if any(ext in g_clean for ext in [".jpg", ".jpeg", ".png", ".webp"]) and _is_usable_image_url(g):
                stem = _image_stem(g)
                if stem and stem not in seen_stems:
                    seen_stems.add(stem)
                    gallery_images.append(make_hd_image_url(g))

        yt_ids = re.findall(
            r"(?:youtube\.com/embed/|youtube-nocookie\.com/embed/|youtu\.be/|youtube\.com/watch\?v=)([a-zA-Z0-9_-]{11})",
            article_html, re.IGNORECASE
        )
        seen_vid = set()
        for vid in yt_ids:
            if vid not in seen_vid:
                seen_vid.add(vid)
                videos.append(f"https://www.youtube.com/embed/{vid}")

    except Exception as e:
        logger.debug(f"Direct scrape fallback for {url}: {e}")

    if not clean_paras:
        try:
            jina_url = f"https://r.jina.ai/{url}"
            resp = requests.get(jina_url, headers=HEADERS, timeout=12)
            resp.raise_for_status()
            lines = resp.text.splitlines()
            start = False
            for line in lines:
                if line.startswith("Markdown Content:"):
                    start = True
                    continue
                if not start:
                    continue
                line = line.strip()
                if not line or line.startswith("!") or line.startswith("#"):
                    continue
                line = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", line)
                if len(line) > 25 and not any(bad in line.lower() for bad in ["source:", "sign up", "cookie"]):
                    clean_paras.append(line)
        except Exception:
            pass

    if not clean_paras and fallback_desc:
        clean_paras = [fallback_desc]

    return {
        "lead_img": lead_img,
        "gallery_images": gallery_images,
        "videos": videos,
        "review_score": review_score,
        "paragraphs": list(dict.fromkeys(clean_paras)),
    }


def scrape_article_details(item: dict) -> dict:
    """Scrape primary source plus up to 2 secondary sources in cluster for multi-source synthesis."""
    feed_img = item.get("feed_image", "")
    lead_img = make_hd_image_url(feed_img) if _is_usable_image_url(feed_img) else ""
    gallery_images = []
    videos = []
    review_score = item.get("review_score", "")
    combined_blocks = []

    sources = item.get("sources") or [{"name": item.get("source_name", "Source"), "url": item["link"], "title": item["title"]}]

    for idx_src, src in enumerate(sources[:3]):
        src_url = src["url"]
        src_name = src["name"]
        fallback_desc = item.get("description", "") if idx_src == 0 else src.get("title", "")
        res = _scrape_single_url(src_url, fallback_desc=fallback_desc)

        if idx_src == 0:
            if (not lead_img or "jp.ign.com" in src_url.lower()) and res["lead_img"]:
                lead_img = res["lead_img"]
        elif not lead_img and res["lead_img"]:
            lead_img = res["lead_img"]

        if not review_score and res["review_score"]:
            review_score = res["review_score"]

        for g in res["gallery_images"]:
            if g not in gallery_images and _image_stem(g) != _image_stem(lead_img):
                gallery_images.append(g)
        for v in res["videos"]:
            if v not in videos:
                videos.append(v)

        paras = res["paragraphs"][:20 if idx_src == 0 else 10]
        if paras:
            header = f"[แหล่งข่าว: {src_name} | หัวข้อ: {src.get('title', item['title'])}]"
            combined_blocks.append(header + "\n" + "\n".join(paras))

    for extra_d in item.get("extra_descriptions", [])[:3]:
        if extra_d:
            combined_blocks.append(extra_d)

    item["cover_image_url"] = lead_img
    item["gallery_images"] = gallery_images[:8]
    item["videos"] = videos[:3]
    item["review_score"] = review_score
    item["raw_paragraphs"] = combined_blocks if combined_blocks else [item.get("description", item["title"])]
    return item


def _wait_for_rate_limit():
    """Enforce minimum interval between Gemini API calls across threads."""
    global _last_api_call_ts
    with _rate_limit_lock:
        now = time.monotonic()
        target_ts = max(now, _last_api_call_ts + MIN_API_INTERVAL_SEC)
        sleep_for = target_ts - now
        _last_api_call_ts = target_ts
    if sleep_for > 0:
        time.sleep(sleep_for)


def call_gemini(item_info: dict) -> dict:
    """Call Gemini as Pure Gamer Magazine Editor to synthesize facts from multi-source context."""
    global _daily_quota_exhausted
    title = item_info.get("title", item_info.get("original_title", ""))
    category_hint = item_info.get("category_hint", item_info.get("tag", "GlobalNews"))
    cat_prompt = CATEGORY_PROMPTS.get(category_hint, CATEGORY_PROMPTS["GlobalNews"])
    paras_text = "\n\n".join(item_info.get("raw_paragraphs", []))
    sources_list = item_info.get("sources") or [{"name": item_info.get("source_name", "Source"), "url": item_info.get("link", "")}]
    source_names_str = ", ".join(s["name"] for s in sources_list)

    if not GEMINI_API_KEY or _daily_quota_exhausted:
        if _daily_quota_exhausted:
            logger.warning(f"Daily Gemini RPD quota exhausted; skipping editorial synthesis for '{title[:40]}'")
        else:
            logger.warning(f"GEMINI_API_KEY missing; skipping editorial synthesis for '{title[:40]}'")
        return {
            "thai_title": title,
            "tag": category_hint,
            "bullet_points": [item_info.get("description", title)],
            "full_paragraphs": item_info.get("raw_paragraphs", [])[:4],
            "review_score": item_info.get("review_score", ""),
            "_translation_failed": True,
        }

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    system_instruction_text = f"{SYSTEM_PROMPT}\n\n{cat_prompt}"

    user_prompt = f"""หัวข้อข่าวตั้งต้น: {title}
หมวดหมู่: {category_hint}
แหล่งข้อมูลอ้างอิงที่รวบรวมมาได้ ({len(sources_list)} แหล่ง): {source_names_str}

{cat_prompt}

เนื้อหาข้อเท็จจริงจากแหล่งข่าว:
{paras_text}

คำสั่งสำหรับกองบรรณาธิการ Pure Gamer:
1. สกัดเฉพาะข้อเท็จจริงจากข้อมูลข้างต้น แล้วนำมา "เขียนเรียบเรียงใหม่ทั้งหมด" เป็นคอลัมน์ข่าวเกม/รีวิวเกมสไตล์นิตยสาร Pure Gamer (ความยาว 4-6 ย่อหน้า)
2. ห้ามแปลตามโครงสร้างย่อหน้าต้นฉบับ 1:1 และห้ามแต่งเติมข้อเท็จจริงที่ไม่มีในต้นฉบับ
3. ระบุชื่อสื่อต้นทาง ({source_names_str}) อย่างเป็นธรรมชาติในเนื้อข่าว
4. ตอบกลับเป็น JSON เท่านั้น:
{{
  "thai_title": "พาดหัวข่าวภาษาไทยสไตล์นิตยสารเกมที่กระชับ ตรง Fact",
  "tag": "{category_hint}",
  "review_score": "{item_info.get('review_score', '')}",
  "bullet_points": [
    "ประเด็นสำคัญที่ 1 (ภาษาไทย)...",
    "ประเด็นสำคัญที่ 2 (ภาษาไทย)...",
    "ประเด็นสำคัญที่ 3 (ภาษาไทย)...",
    "ประเด็นสำคัญที่ 4 (ภาษาไทย)..."
  ],
  "full_paragraphs": [
    "ย่อหน้าที่ 1 (The Lead): เปิดเรื่องและสรุปประเด็นหลัก...",
    "ย่อหน้าที่ 2: รายละเอียดเชิงลึกจากแหล่งข่าว...",
    "ย่อหน้าที่ 3: ข้อมูลเพิ่มเติมและบริบทเกมเพลย์/แพลตฟอร์ม...",
    "ย่อหน้าที่ 4 (Editor's Note): มุมมองบรรณาธิการและสิ่งที่น่าจับตามอง..."
  ]
}}"""

    payload = {
        "system_instruction": {"parts": [{"text": system_instruction_text}]},
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {"response_mime_type": "application/json", "temperature": 0.25},
    }

    for attempt in range(3):
        try:
            _wait_for_rate_limit()
            resp = requests.post(url, json=payload, timeout=45)
            if resp.status_code == 429:
                body_lower = (resp.text or "").lower()
                if any(k in body_lower for k in ("perday", "per_day", "requestsperday", "daily")):
                    _daily_quota_exhausted = True
                    logger.error(f"Gemini daily quota exhausted (429) on '{title[:30]}...'; halting remaining API calls.")
                    break
                wait_s = (8 * (2 ** attempt)) + random.uniform(1.0, 3.5)
                logger.warning(f"Gemini HTTP 429 for '{title[:30]}...'. Backing off {wait_s:.1f}s...")
                time.sleep(wait_s)
                continue
            if resp.status_code in (500, 502, 503, 504):
                wait_s = (4 * (2 ** attempt)) + random.uniform(1.0, 3.5)
                logger.warning(f"Gemini HTTP {resp.status_code} for '{title[:30]}...'. Backing off {wait_s:.1f}s...")
                time.sleep(wait_s)
                continue
            resp.raise_for_status()

            data = resp.json()
            raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
            parsed = json.loads(raw_text)
            parsed["tag"] = category_hint

            thai_t = (parsed.get("thai_title") or "").strip()
            full_p = parsed.get("full_paragraphs") or []
            bullets = parsed.get("bullet_points") or []
            if (
                has_thai_chars(thai_t)
                and isinstance(full_p, list)
                and any(has_thai_chars(p) for p in full_p if isinstance(p, str))
                and isinstance(bullets, list)
                and len(bullets) > 0
            ):
                parsed["_translation_failed"] = False
                return parsed

            wait_s = (2 * (2 ** attempt)) + random.uniform(0.5, 2.0)
            logger.warning(f"Incomplete Thai output for '{title[:30]}...'; retrying in {wait_s:.1f}s")
            time.sleep(wait_s)
        except Exception as e:
            wait_s = (4 * (2 ** attempt)) + random.uniform(1.0, 3.5)
            logger.warning(f"Gemini attempt {attempt + 1} failed for '{title[:30]}...': {e} (wait {wait_s:.1f}s)")
            time.sleep(wait_s)

    return {
        "thai_title": title,
        "tag": category_hint,
        "bullet_points": [item_info.get("description", title)],
        "full_paragraphs": item_info.get("raw_paragraphs", [])[:4],
        "review_score": item_info.get("review_score", ""),
        "_translation_failed": True,
    }


def sanitize_article(item: dict) -> dict:
    """Return a clean dictionary without internal temporary fields."""
    return {k: v for k, v in item.items() if not k.startswith("_") and k != "raw_paragraphs" and k != "extra_descriptions"}


def update_ui_files(news_list: list[dict]):
    """Generate README.md and index.html for Pure Gamer."""
    clean_news = [sanitize_article(x) for x in news_list]
    now_th = to_thailand_time(datetime.datetime.now(datetime.timezone.utc))

    g_news_cnt = sum(1 for x in clean_news if x.get("tag") == "GlobalNews")
    j_news_cnt = sum(1 for x in clean_news if x.get("tag") == "JapanNews")
    g_rev_cnt = sum(1 for x in clean_news if x.get("tag") == "GlobalReview")
    j_rev_cnt = sum(1 for x in clean_news if x.get("tag") == "JapanReview")

    readme_lines = [
        "# 🎮 Pure Gamer — AI Editorial Gaming News & Reviews Magazine",
        "",
        "> **Live Automated Gaming Magazine:** Synthesized & rewritten in Thai by an AI Gaming Editor from multiple Global (IGN, GameSpot, PC Gamer, Eurogamer, VGC) and Japanese (IGN Japan, 4Gamer, Automaton JP, Gematsu) sources with full source attribution.",
        f"> *อัปเดตล่าสุด: `{now_th}`* • *รวมทั้งหมด: `{len(clean_news)} รายการ`* (`🌍 Global News: {g_news_cnt}` | `🇯🇵 Japan News: {j_news_cnt}` | `⭐ Global Reviews: {g_rev_cnt}` | `⛩️ Japan Reviews: {j_rev_cnt}`)",
        "",
        "### 🌐 [กดที่นี่เพื่อเปิดหน้าเว็บ Pure Gamer (GitHub Pages)](https://puresinghara.github.io/pure-gamer/)",
        "",
        "---",
        "",
        f"### 📰 บทความข่าวและรีวิวเกมล่าสุด ({len(clean_news)} รายการ)",
        "",
        "| หมวดหมู่ | หัวข้อข่าว & ประเด็นสำคัญ | เวลาเผยแพร่ (เวลาไทย) | แหล่งข้อมูลอ้างอิง |",
        "| :--- | :--- | :--- | :--- |",
    ]
    for item in clean_news:
        score_str = f" ⭐{item['review_score']}/10" if item.get("review_score") else ""
        tag_badge = f"`[{item['tag']}{score_str}]`"
        bullets_md = "<br>".join([f"• {b}" for b in item.get("bullet_points", [])[:3]])
        headline = f"**{item['thai_title']}**<br>{bullets_md}"
        srcs = item.get("sources") or [{"name": item.get("source_name", "Source"), "url": item.get("link", "#")}]
        links_md = " • ".join([f"[{s['name']}]({s['url']})" for s in srcs])
        pub = item.get("published_th", item.get("published_at", ""))
        readme_lines.append(f"| {tag_badge} | {headline} | {pub} | {links_md} |")

    readme_lines.append("")
    readme_lines.append("---")
    readme_lines.append("*Automated by GitHub Actions & Google Gemini AI — Editorial Synthesis with Multi-Source Attribution.*")

    with open(README_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(readme_lines))

    json_data_str = json.dumps(clean_news, ensure_ascii=False).replace("</", "<\\/")
    html_content = f'''<!DOCTYPE html>
<html lang="th" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Pure Gamer — Gaming Magazine</title>
  <link rel="icon" type="image/jpeg" href="logo.jpg">
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@500;700;800&family=Prompt:wght@300;400;500;600;700&display=swap');
    body {{ font-family: 'Prompt', sans-serif; }}
    .brand-title {{ font-family: 'Chakra Petch', sans-serif; letter-spacing: -0.02em; }}
  </style>
</head>
<body class="bg-[#09090b] text-zinc-100 min-h-screen antialiased selection:bg-[#ff3366] selection:text-white">

  <!-- Clean Minimal Header (Logo + Pure Gamer + Category Tabs) -->
  <header class="border-b border-zinc-800 bg-[#0c0c0e]/95 sticky top-0 z-40 backdrop-blur-md">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 py-3.5 flex flex-col lg:flex-row lg:items-center justify-between gap-4">
      
      <!-- Brand Logo -->
      <div class="flex items-center gap-3">
        <img src="logo.jpg" alt="Pure Gamer" class="w-11 h-11 rounded-md object-cover border border-zinc-700/80 shadow-sm bg-[#faf7f2]">
        <div class="flex flex-col">
          <div class="flex items-center gap-2">
            <h1 class="brand-title text-2xl md:text-3xl font-extrabold uppercase text-white tracking-wider leading-none">
              PURE GAMER
            </h1>
            <span class="px-2 py-0.5 rounded bg-[#ff3366]/15 text-[#ff3366] border border-[#ff3366]/30 text-[10px] font-mono font-bold tracking-widest uppercase">
              GAMING MAGAZINE
            </span>
          </div>
          <span class="text-[11px] font-mono text-zinc-500 mt-0.5">GLOBAL & JAPAN EDITORIAL SYNTHESIS</span>
        </div>
      </div>

      <!-- Clean Category Filter Tabs -->
      <div class="flex items-center gap-2 overflow-x-auto pb-1 lg:pb-0">
        <button onclick="filterTag('ALL')" id="filterALL" class="px-3.5 py-1.5 bg-white text-black font-mono font-bold text-xs rounded transition whitespace-nowrap">ALL</button>
        <button onclick="filterTag('GlobalNews')" id="filterGlobalNews" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-cyan-400 font-mono text-xs rounded transition whitespace-nowrap">🌍 GLOBAL NEWS</button>
        <button onclick="filterTag('JapanNews')" id="filterJapanNews" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-[#ff3366] font-mono text-xs rounded transition whitespace-nowrap">🇯🇵 JAPAN NEWS</button>
        <button onclick="filterTag('GlobalReview')" id="filterGlobalReview" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-purple-400 font-mono text-xs rounded transition whitespace-nowrap">⭐ GLOBAL REVIEWS</button>
        <button onclick="filterTag('JapanReview')" id="filterJapanReview" class="px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 text-amber-400 font-mono text-xs rounded transition whitespace-nowrap">⛩️ JAPAN REVIEWS</button>
        <button onclick="refreshLiveNews(this)" title="ดึงข้อมูลข่าวล่าสุด" class="p-1.5 bg-zinc-900 hover:bg-zinc-800 border border-zinc-800 text-zinc-300 rounded transition ml-1 shrink-0">
          <svg class="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
        </button>
      </div>

    </div>
  </header>

  <!-- Main Container -->
  <main class="max-w-7xl mx-auto px-4 sm:px-6 py-8">
    <div id="newsGrid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      <!-- Cards rendered here -->
    </div>
  </main>

  <!-- Top-Pinned Modal Dialog -->
  <div id="modalOverlay" onclick="if(event.target===this) closeModal()" class="fixed inset-0 bg-black/85 backdrop-blur-md z-50 hidden flex items-start sm:items-center justify-center p-0 sm:p-4 overflow-y-auto">
    <div class="w-full max-w-3xl bg-[#0c0c0e] sm:border sm:border-zinc-800 sm:rounded-lg shadow-2xl overflow-hidden my-0 sm:my-auto max-h-screen sm:max-h-[92vh] flex flex-col">
      
      <!-- Modal Header (Sticky) -->
      <div class="flex items-center justify-between px-3.5 sm:px-6 py-3 border-b border-zinc-800 bg-[#0e0e11] sticky top-0 z-10 gap-2">
        <div class="flex items-center gap-2 min-w-0">
          <span id="modalTag" class="px-2 py-0.5 rounded text-[11px] sm:text-xs font-mono font-bold uppercase tracking-wider bg-[#ff3366]/15 text-[#ff3366] border border-[#ff3366]/30 truncate"></span>
        </div>
        <div class="flex items-center gap-1.5 sm:gap-2 shrink-0">
          <div id="ttsControls" class="flex items-center bg-zinc-900 border border-zinc-800 rounded-full p-0.5">
            <button id="ttsBtn" onclick="toggleSpeech()" title="ฟังเสียงอ่านข่าว" class="px-2.5 py-1 text-xs font-mono font-bold text-emerald-400 hover:text-white hover:bg-zinc-800 rounded-full transition flex items-center gap-1">
              <span id="ttsIcon">🔊</span>
              <span id="ttsLabel" class="hidden sm:inline">ฟังข่าว</span>
            </button>
            <button id="ttsRateBtn" onclick="cycleSpeechRate()" title="ความเร็วเสียงอ่าน" class="px-2 py-1 text-[11px] font-mono text-zinc-400 hover:text-white hover:bg-zinc-800 rounded-full transition">1x</button>
          </div>
          <div class="flex items-center bg-zinc-900 border border-zinc-800 rounded-full p-0.5">
            <button onclick="changeFontSize(-1)" title="ลดขนาดตัวอักษร" class="px-2 sm:px-2.5 py-1 text-xs font-mono font-bold text-zinc-300 hover:text-white hover:bg-zinc-800 rounded-full transition">A-</button>
            <span id="fontSizeLabel" class="hidden sm:inline text-[11px] font-mono text-zinc-500 px-1.5 select-none">100%</span>
            <button onclick="changeFontSize(1)" title="เพิ่มขนาดตัวอักษร" class="px-2 sm:px-2.5 py-1 text-xs font-mono font-bold text-zinc-300 hover:text-white hover:bg-zinc-800 rounded-full transition">A+</button>
          </div>
          <button onclick="closeModal()" aria-label="Close" class="w-8 h-8 rounded-full bg-zinc-900 hover:bg-zinc-800 text-zinc-400 hover:text-white flex items-center justify-center text-xl transition leading-none">&times;</button>
        </div>
      </div>

      <!-- Modal Body -->
      <div id="modalBody" class="p-5 sm:p-6 md:p-8 overflow-y-auto space-y-5 sm:space-y-6">
        <div class="w-full aspect-video overflow-hidden rounded-md border border-zinc-800 bg-zinc-950 relative">
          <img id="modalImg" src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7" referrerpolicy="no-referrer" alt="" class="w-full h-full object-cover">
          <div id="modalScoreBadge" class="hidden absolute top-3 right-3 bg-[#ff3366] text-white font-mono font-extrabold text-sm px-3 py-1.5 rounded shadow-xl border border-white/20"></div>
        </div>

        <div class="flex items-center flex-wrap gap-2 text-xs font-mono text-zinc-400">
          <span id="modalSubTag" class="hidden px-2 py-0.5 rounded text-[11px] font-mono font-semibold uppercase tracking-wider bg-zinc-800 text-zinc-200 border border-zinc-700"></span>
          <span id="modalSourceBadges" class="hidden px-2 py-0.5 rounded text-[11px] font-mono font-semibold bg-emerald-950/60 text-emerald-300 border border-emerald-800/60"></span>
          <span id="modalDate" class="text-xs font-mono text-zinc-400"></span>
        </div>

        <h2 id="modalTitle" class="brand-title text-xl md:text-2xl font-bold text-white leading-snug"></h2>

        <div class="bg-black/60 border-l-2 border-[#ff3366] p-4 rounded-sm">
          <div class="text-xs font-mono text-[#ff3366] uppercase font-bold tracking-wider mb-2">📌 สรุปประเด็นสำคัญ (KEY FACTS):</div>
          <ul id="modalBullets" class="space-y-2 text-sm text-zinc-200"></ul>
        </div>

        <div id="modalVideoSection" class="hidden space-y-3 pt-1">
          <div class="text-xs font-mono text-red-400 uppercase tracking-widest flex items-center gap-1.5 font-bold">
            <span>🎬 วิดีโอตัวอย่าง / TRAILER ในข่าว :</span>
          </div>
          <div id="modalVideoContainer" class="space-y-4"></div>
        </div>

        <div class="space-y-4 pt-2">
          <div class="text-xs font-mono text-zinc-400 uppercase tracking-widest">// บทความเรียบเรียงโดยกองบรรณาธิการ PURE GAMER :</div>
          <div id="modalFullContent" class="space-y-4 text-zinc-300 text-sm md:text-base leading-relaxed font-light"></div>
        </div>

        <div id="modalGallerySection" class="hidden pt-4 border-t border-zinc-800/80">
          <div class="text-xs font-mono text-amber-400 uppercase tracking-widest mb-3 flex items-center gap-1.5">
            <span>📸 ภาพประกอบเพิ่มเติม :</span>
          </div>
          <div id="modalGalleryGrid" class="grid grid-cols-2 sm:grid-cols-3 gap-2.5"></div>
        </div>
      </div>

      <!-- Modal Footer: Multi-Source Attribution -->
      <div class="px-4 sm:px-6 py-3.5 border-t border-zinc-800 bg-[#0e0e11] flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div class="flex flex-col gap-1.5 min-w-0">
          <span class="text-[10px] font-mono uppercase tracking-wider text-zinc-500">🔗 แหล่งข้อมูลอ้างอิง / ORIGINAL SOURCES:</span>
          <div id="modalSourcesList" class="flex items-center flex-wrap gap-2"></div>
        </div>
        <button onclick="closeModal()" class="px-4 py-2 bg-zinc-800 hover:bg-zinc-700 text-xs font-mono text-zinc-300 rounded-sm transition shrink-0 self-end sm:self-center">ปิด</button>
      </div>

    </div>
  </div>

  <script>
    const TRANSPARENT_GIF = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';
    const SVG_FALLBACK_URI = "data:image/svg+xml;charset=UTF-8," + encodeURIComponent(
      '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 450">' +
      '<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1">' +
      '<stop offset="0%" stop-color="#18181b"/><stop offset="100%" stop-color="#09090b"/>' +
      '</linearGradient></defs>' +
      '<rect width="800" height="450" fill="url(#g)"/>' +
      '<text x="50%" y="48%" dominant-baseline="middle" text-anchor="middle" fill="#ff3366" font-family="monospace" font-size="36" font-weight="bold">PURE GAMER</text>' +
      '<text x="50%" y="58%" dominant-baseline="middle" text-anchor="middle" fill="#71717a" font-family="monospace" font-size="16">IMAGE UNAVAILABLE</text>' +
      '</svg>'
    );
    const CATEGORY_FALLBACKS = {{
      GlobalNews: 'https://images.unsplash.com/photo-1542751371-adc38448a05e?w=1200&q=80',
      JapanNews: 'https://images.unsplash.com/photo-1511512578047-dfb367046420?w=1200&q=80',
      GlobalReview: 'https://images.unsplash.com/photo-1538481199705-c710c4e965fc?w=1200&q=80',
      JapanReview: 'https://images.unsplash.com/photo-1550745165-9bc0b252726f?w=1200&q=80'
    }};
    const CATEGORY_LABELS = {{
      GlobalNews: '🌍 GLOBAL NEWS',
      JapanNews: '🇯🇵 JAPAN NEWS',
      GlobalReview: '⭐ GLOBAL REVIEWS',
      JapanReview: '⛩️ JAPAN REVIEWS'
    }};

    const FONT_STEPS = [
      {{ pct: '85%', body: '0.875rem', bullet: '0.8125rem', title: '1.25rem' }},
      {{ pct: '100%', body: '1rem', bullet: '0.875rem', title: '1.5rem' }},
      {{ pct: '115%', body: '1.125rem', bullet: '1rem', title: '1.65rem' }},
      {{ pct: '130%', body: '1.25rem', bullet: '1.0625rem', title: '1.8rem' }},
      {{ pct: '150%', body: '1.4rem', bullet: '1.15rem', title: '2rem' }}
    ];
    let currentFontIdx = 1;
    try {{
      const savedIdx = parseInt(localStorage.getItem('pureGamerFontIdx') || '1', 10);
      if (!isNaN(savedIdx) && savedIdx >= 0 && savedIdx < FONT_STEPS.length) {{
        currentFontIdx = savedIdx;
      }}
    }} catch (e) {{}}

    let allNews = {json_data_str};
    let currentTag = 'ALL';
    let isModalOpen = false;

    const SPEECH_RATES = [1.0, 1.25, 1.5];
    let speechRateIdx = 0;
    let isSpeaking = false;
    let speechQueue = [];
    let currentSpeechStep = 0;
    let currentModalItem = null;
    let speechSessionId = 0;

    if ('speechSynthesis' in window) {{
      window.speechSynthesis.onvoiceschanged = () => {{}};
    }}

    function getThaiVoice() {{
      if (!('speechSynthesis' in window)) return null;
      const voices = window.speechSynthesis.getVoices() || [];
      const thaiVoices = voices.filter(v => v.lang && v.lang.toLowerCase().startsWith('th'));
      if (thaiVoices.length === 0) return null;
      const preferred = thaiVoices.find(v => {{
        const n = (v.name || '').toLowerCase();
        return n.includes('natural') || n.includes('premwadee') || n.includes('niwat') || n.includes('google') || n.includes('kanya');
      }});
      return preferred || thaiVoices[0];
    }}

    function highlightSpeakingParagraph(paraIdx) {{
      const activeClasses = ['bg-zinc-800/60', 'border-l-2', 'border-[#ff3366]', 'pl-3', 'py-1', 'rounded-r', 'text-white'];
      document.querySelectorAll('#modalFullContent p[data-para-idx]').forEach(p => {{
        p.classList.remove(...activeClasses);
      }});
      if (paraIdx >= 0) {{
        const target = document.querySelector(`#modalFullContent p[data-para-idx="${{paraIdx}}"]`);
        if (target) {{
          target.classList.add(...activeClasses);
        }}
      }}
    }}

    function updateTtsButtonUI() {{
      const btn = document.getElementById('ttsBtn');
      const icon = document.getElementById('ttsIcon');
      const label = document.getElementById('ttsLabel');
      if (!btn || !icon || !label) return;
      if (isSpeaking) {{
        icon.textContent = '⏹';
        label.textContent = 'หยุด';
        btn.className = 'px-2.5 py-1 text-xs font-mono font-bold text-[#ff3366] hover:text-white hover:bg-zinc-800 rounded-full transition flex items-center gap-1';
      }} else {{
        icon.textContent = '🔊';
        label.textContent = 'ฟังข่าว';
        btn.className = 'px-2.5 py-1 text-xs font-mono font-bold text-emerald-400 hover:text-white hover:bg-zinc-800 rounded-full transition flex items-center gap-1';
      }}
    }}

    function stopSpeech() {{
      speechSessionId++;
      isSpeaking = false;
      speechQueue = [];
      currentSpeechStep = 0;
      if ('speechSynthesis' in window) {{
        window.speechSynthesis.cancel();
      }}
      highlightSpeakingParagraph(-1);
      updateTtsButtonUI();
    }}

    function speakCurrentStep(sessionId) {{
      if (!isSpeaking || sessionId !== speechSessionId || currentSpeechStep >= speechQueue.length) {{
        if (sessionId === speechSessionId) stopSpeech();
        return;
      }}
      const stepItem = speechQueue[currentSpeechStep];
      highlightSpeakingParagraph(stepItem.paraIdx);

      const u = new SpeechSynthesisUtterance(stepItem.text);
      u.lang = 'th-TH';
      u.rate = SPEECH_RATES[speechRateIdx] || 1.0;
      const thVoice = getThaiVoice();
      if (thVoice) u.voice = thVoice;

      u.onend = () => {{
        if (sessionId === speechSessionId && isSpeaking) {{
          currentSpeechStep++;
          speakCurrentStep(sessionId);
        }}
      }};
      u.onerror = (e) => {{
        if (e && e.error === 'interrupted') return;
        if (sessionId === speechSessionId && isSpeaking) {{
          currentSpeechStep++;
          speakCurrentStep(sessionId);
        }}
      }};
      window.speechSynthesis.speak(u);
    }}

    function startSpeech(resumeStep = 0) {{
      if (!('speechSynthesis' in window) || !currentModalItem) return;
      window.speechSynthesis.cancel();
      speechSessionId++;
      const sessionId = speechSessionId;

      speechQueue = [];
      const headline = (currentModalItem.thai_title || currentModalItem.original_title || '').trim();
      if (headline) {{
        speechQueue.push({{ text: headline, paraIdx: -1 }});
      }}
      const bodyParas = Array.isArray(currentModalItem.full_paragraphs) ? currentModalItem.full_paragraphs : [];
      bodyParas.forEach((p, idx) => {{
        if (p && String(p).trim()) {{
          speechQueue.push({{ text: String(p).trim(), paraIdx: idx }});
        }}
      }});

      if (speechQueue.length === 0) return;
      currentSpeechStep = Math.max(0, Math.min(resumeStep, speechQueue.length - 1));
      isSpeaking = true;
      updateTtsButtonUI();
      speakCurrentStep(sessionId);
    }}

    function toggleSpeech() {{
      if (isSpeaking) {{
        stopSpeech();
      }} else {{
        startSpeech(0);
      }}
    }}

    function cycleSpeechRate() {{
      speechRateIdx = (speechRateIdx + 1) % SPEECH_RATES.length;
      const rateBtn = document.getElementById('ttsRateBtn');
      if (rateBtn) rateBtn.textContent = SPEECH_RATES[speechRateIdx] + 'x';
      if (isSpeaking) {{
        startSpeech(currentSpeechStep);
      }}
    }}

    window.addEventListener('beforeunload', () => stopSpeech());

    function applyFontSize() {{
      const step = FONT_STEPS[currentFontIdx] || FONT_STEPS[1];
      const fullEl = document.getElementById('modalFullContent');
      const bulletEl = document.getElementById('modalBullets');
      const titleEl = document.getElementById('modalTitle');
      const labelEl = document.getElementById('fontSizeLabel');
      if (fullEl) fullEl.style.fontSize = step.body;
      if (bulletEl) bulletEl.style.fontSize = step.bullet;
      if (titleEl) titleEl.style.fontSize = step.title;
      if (labelEl) labelEl.textContent = step.pct;
    }}

    function changeFontSize(delta) {{
      const next = Math.max(0, Math.min(FONT_STEPS.length - 1, currentFontIdx + delta));
      if (next !== currentFontIdx) {{
        currentFontIdx = next;
        try {{ localStorage.setItem('pureGamerFontIdx', String(currentFontIdx)); }} catch (e) {{}}
        applyFontSize();
      }}
    }}

    function escapeHtml(str) {{
      if (str === null || str === undefined) return '';
      return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    function handleImgError(img, tag) {{
      const step = parseInt(img.getAttribute('data-fb-step') || '0', 10);
      if (step === 0) {{
        img.setAttribute('data-fb-step', '1');
        img.src = CATEGORY_FALLBACKS[tag] || CATEGORY_FALLBACKS.GlobalNews;
      }} else {{
        img.onerror = null;
        img.src = SVG_FALLBACK_URI;
      }}
    }}

    function handleGalleryImgError(img) {{
      img.onerror = null;
      const wrapper = img.parentElement;
      if (wrapper) wrapper.remove();
      const grid = document.getElementById('modalGalleryGrid');
      const sec = document.getElementById('modalGallerySection');
      if (grid && sec && grid.children.length === 0) {{
        sec.classList.add('hidden');
      }}
    }}

    async function refreshLiveNews(btn) {{
      const svg = btn ? btn.querySelector('svg') : null;
      if (svg) svg.classList.add('animate-spin');
      try {{
        const r = await fetch('data/news.json?t=' + Date.now(), {{ cache: 'no-store' }});
        if (r.ok) {{
          const data = await r.json();
          if (Array.isArray(data) && data.length > 0) {{
            allNews = data;
            render();
          }}
        }}
      }} catch (err) {{
      }} finally {{
        if (svg) svg.classList.remove('animate-spin');
      }}
    }}

    function filterTag(tag) {{
      currentTag = tag;
      ['ALL', 'GlobalNews', 'JapanNews', 'GlobalReview', 'JapanReview'].forEach(t => {{
        const btn = document.getElementById('filter' + t);
        if (btn) {{
          if (t === tag) {{
            btn.className = "px-3.5 py-1.5 bg-white text-black font-mono font-bold text-xs rounded transition whitespace-nowrap";
          }} else {{
            let textColor = 'text-zinc-300';
            if (t === 'GlobalNews') textColor = 'text-cyan-400';
            if (t === 'JapanNews') textColor = 'text-[#ff3366]';
            if (t === 'GlobalReview') textColor = 'text-purple-400';
            if (t === 'JapanReview') textColor = 'text-amber-400';
            btn.className = `px-3.5 py-1.5 bg-zinc-900 hover:bg-zinc-800 ${{textColor}} font-mono text-xs rounded transition whitespace-nowrap`;
          }}
        }}
      }});
      render();
      window.scrollTo({{ top: 0, left: 0, behavior: 'instant' }});
    }}

    function formatSourceNames(item) {{
      const srcs = Array.isArray(item.sources) && item.sources.length > 0
        ? item.sources.map(s => s.name).filter(Boolean)
        : [item.source_name || 'SOURCE'];
      return [...new Set(srcs)].join(' + ');
    }}

    function render() {{
      const grid = document.getElementById('newsGrid');
      grid.innerHTML = '';

      const filtered = allNews
        .filter(item => (currentTag === 'ALL') || (item.tag === currentTag))
        .slice()
        .sort((a, b) => (b.published_ts || 0) - (a.published_ts || 0));

      if (filtered.length === 0) {{
        grid.innerHTML = '<div class="col-span-3 text-center py-16 text-zinc-500 font-mono">ไม่มีข่าวในหมวดนี้</div>';
        return;
      }}

      filtered.forEach((item, cardIdx) => {{
        const originalIndex = allNews.indexOf(item);
        const div = document.createElement('div');
        div.className = 'bg-zinc-900/50 border border-zinc-800 hover:border-zinc-700 transition flex flex-col justify-between overflow-hidden rounded cursor-pointer group';
        div.onclick = () => openModal(originalIndex);
        
        const bulletsHtml = (item.bullet_points || []).slice(0, 3).map(b => 
          `<li class="flex items-start gap-1.5"><span class="text-[#ff3366] font-bold">›</span><span>${{escapeHtml(b)}}</span></li>`
        ).join('');

        let tagColor = 'text-cyan-400';
        if (item.tag === 'JapanNews') tagColor = 'text-[#ff3366]';
        else if (item.tag === 'GlobalReview') tagColor = 'text-purple-400';
        else if (item.tag === 'JapanReview') tagColor = 'text-amber-400';

        const reviewBadge = item.review_score ? `<div class="absolute top-2.5 right-2.5 bg-[#ff3366] text-white font-mono font-extrabold text-xs px-2.5 py-1 rounded shadow-lg border border-white/20">⭐ ${{escapeHtml(item.review_score)}} / 10</div>` : '';
        const imgSrc = item.cover_image_url || CATEGORY_FALLBACKS[item.tag] || CATEGORY_FALLBACKS.GlobalNews;
        const loadingAttr = cardIdx < 6 ? 'eager' : 'lazy';
        const safeTag = escapeHtml(item.tag || '');
        const safeCatLabel = escapeHtml(CATEGORY_LABELS[item.tag] || item.tag || '');
        const safeSubTag = escapeHtml(item.sub_tag || '');
        const subTagBadge = safeSubTag ? `<span class="px-1.5 py-0.5 rounded bg-zinc-800/90 text-zinc-200 border border-zinc-700/80 text-[10px] font-mono font-semibold tracking-wide uppercase">${{safeSubTag}}</span>` : '';
        const safeSources = escapeHtml(formatSourceNames(item));
        const safeDate = escapeHtml(item.published_th || item.published_at || '');
        const safeTitle = escapeHtml(item.thai_title || item.original_title || '');

        div.innerHTML = `
          <div>
            <div class="w-full aspect-video overflow-hidden bg-zinc-950 relative">
              <img src="${{escapeHtml(imgSrc)}}" data-tag="${{safeTag}}" referrerpolicy="no-referrer" loading="${{loadingAttr}}" decoding="async" class="w-full h-full object-cover group-hover:scale-105 transition duration-500" onerror="handleImgError(this, this.dataset.tag)">
              ${{reviewBadge}}
            </div>
            <div class="p-5">
              <div class="flex items-center flex-wrap gap-1.5 text-[11px] font-mono mb-2.5">
                <span class="${{tagColor}} font-bold uppercase">${{safeCatLabel}}</span>
                ${{subTagBadge}}
                <span class="text-zinc-600">•</span>
                <span class="text-zinc-500">${{safeDate}}</span>
              </div>
              <h3 class="brand-title text-base font-bold text-white group-hover:text-[#ff3366] transition mb-3 leading-snug line-clamp-2">
                ${{safeTitle}}
              </h3>
              <ul class="text-xs text-zinc-300 space-y-1.5 font-light">
                ${{bulletsHtml}}
              </ul>
            </div>
          </div>
          <div class="p-5 pt-3 border-t border-zinc-800/80 text-[11px] font-mono text-zinc-500 flex items-center justify-between gap-2">
            <span class="text-zinc-300 group-hover:text-white transition shrink-0">อ่านบทความ →</span>
            <span class="px-2 py-0.5 rounded bg-emerald-950/50 text-emerald-300 border border-emerald-800/50 truncate">📰 ${{safeSources}}</span>
          </div>
        `;
        grid.appendChild(div);
      }});
    }}

    function openModal(idx) {{
      const item = allNews[idx];
      if (!item) return;

      stopSpeech();
      currentModalItem = item;
      const ttsWrap = document.getElementById('ttsControls');
      if (ttsWrap && !('speechSynthesis' in window)) {{
        ttsWrap.classList.add('hidden');
      }}

      isModalOpen = true;
      history.pushState({{ modalOpen: true, newsId: item.id }}, '', '#news-' + item.id);

      document.getElementById('modalTag').textContent = CATEGORY_LABELS[item.tag] || item.tag || '';
      const modalSubTag = document.getElementById('modalSubTag');
      if (modalSubTag) {{
        if (item.sub_tag) {{
          modalSubTag.textContent = item.sub_tag;
          modalSubTag.classList.remove('hidden');
        }} else {{
          modalSubTag.classList.add('hidden');
          modalSubTag.textContent = '';
        }}
      }}
      const modalSourceBadges = document.getElementById('modalSourceBadges');
      if (modalSourceBadges) {{
        modalSourceBadges.textContent = '📰 อ้างอิง: ' + formatSourceNames(item);
        modalSourceBadges.classList.remove('hidden');
      }}

      const modalImg = document.getElementById('modalImg');
      modalImg.onerror = null;
      modalImg.src = TRANSPARENT_GIF;
      modalImg.setAttribute('data-fb-step', '0');
      modalImg.onerror = () => handleImgError(modalImg, item.tag);
      modalImg.src = item.cover_image_url || CATEGORY_FALLBACKS[item.tag] || SVG_FALLBACK_URI;

      const scoreBadge = document.getElementById('modalScoreBadge');
      if (scoreBadge) {{
        if (item.review_score) {{
          scoreBadge.textContent = `⭐ ${{item.review_score}} / 10`;
          scoreBadge.classList.remove('hidden');
        }} else {{
          scoreBadge.classList.add('hidden');
          scoreBadge.textContent = '';
        }}
      }}

      document.getElementById('modalTitle').textContent = item.thai_title || item.original_title || '';
      document.getElementById('modalDate').textContent = item.published_th || item.published_at || '';

      const bulletsList = document.getElementById('modalBullets');
      bulletsList.innerHTML = '';
      (item.bullet_points || []).forEach(b => {{
        const li = document.createElement('li');
        li.className = 'flex items-start gap-2';
        li.innerHTML = `<span class="text-[#ff3366] font-bold">›</span><span>${{escapeHtml(b)}}</span>`;
        bulletsList.appendChild(li);
      }});

      const videoSec = document.getElementById('modalVideoSection');
      const videoContainer = document.getElementById('modalVideoContainer');
      videoContainer.innerHTML = '';
      if (Array.isArray(item.videos) && item.videos.length > 0) {{
        videoSec.classList.remove('hidden');
        item.videos.forEach(vUrl => {{
          const wrap = document.createElement('div');
          wrap.className = 'w-full aspect-video rounded-md overflow-hidden border border-zinc-800 bg-black shadow-lg';
          wrap.innerHTML = `<iframe src="${{escapeHtml(vUrl)}}?rel=0" class="w-full h-full" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen loading="lazy"></iframe>`;
          videoContainer.appendChild(wrap);
        }});
      }} else {{
        videoSec.classList.add('hidden');
      }}

      const fullContentDiv = document.getElementById('modalFullContent');
      fullContentDiv.innerHTML = '';
      const paras = Array.isArray(item.full_paragraphs) && item.full_paragraphs.length > 0 ? item.full_paragraphs : (item.bullet_points || []);
      const images = Array.isArray(item.gallery_images) ? item.gallery_images.slice() : [];
      const maxInline = Math.min(images.length, 4);
      const step = maxInline > 0 ? Math.max(2, Math.floor(paras.length / (maxInline + 1))) : 0;
      let inlineUsedCount = 0;

      paras.forEach((p, pIdx) => {{
        const pElem = document.createElement('p');
        pElem.setAttribute('data-para-idx', String(pIdx));
        pElem.className = 'transition-all duration-200';
        pElem.textContent = p;
        fullContentDiv.appendChild(pElem);

        if (step > 0 && inlineUsedCount < maxInline && (pIdx + 1) % step === 0 && (pIdx + 1) < paras.length) {{
          const imgUrl = images[inlineUsedCount++];
          const fig = document.createElement('figure');
          fig.className = 'my-5 rounded-md overflow-hidden border border-zinc-800 bg-zinc-950';
          fig.innerHTML = `<a href="${{escapeHtml(imgUrl)}}" target="_blank" rel="noopener noreferrer" class="block aspect-video overflow-hidden group"><img src="${{escapeHtml(imgUrl)}}" referrerpolicy="no-referrer" loading="lazy" decoding="async" onerror="this.closest('figure').remove()" class="w-full h-full object-cover group-hover:scale-105 transition duration-500"></a>`;
          fullContentDiv.appendChild(fig);
        }}
      }});

      const gallerySec = document.getElementById('modalGallerySection');
      const galleryGrid = document.getElementById('modalGalleryGrid');
      galleryGrid.innerHTML = '';
      const remainingImages = images.slice(inlineUsedCount);
      if (remainingImages.length > 0) {{
        gallerySec.classList.remove('hidden');
        remainingImages.forEach(imgUrl => {{
          const a = document.createElement('a');
          a.href = imgUrl;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = 'block aspect-video overflow-hidden rounded bg-zinc-950 border border-zinc-800 hover:border-[#ff3366] transition group relative';
          a.innerHTML = `<img src="${{escapeHtml(imgUrl)}}" referrerpolicy="no-referrer" loading="lazy" decoding="async" onerror="handleGalleryImgError(this)" class="w-full h-full object-cover group-hover:scale-105 transition duration-300">`;
          galleryGrid.appendChild(a);
        }});
      }} else {{
        gallerySec.classList.add('hidden');
      }}

      const sourcesListEl = document.getElementById('modalSourcesList');
      sourcesListEl.innerHTML = '';
      const srcs = Array.isArray(item.sources) && item.sources.length > 0
        ? item.sources
        : [{{ name: item.source_name || 'ต้นฉบับ', url: item.link || '#' }}];
      srcs.forEach(src => {{
        const a = document.createElement('a');
        a.href = src.url || '#';
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        a.className = 'px-3 py-1.5 bg-zinc-900 border border-zinc-700 text-xs font-mono text-white rounded-sm hover:bg-zinc-800 hover:border-[#ff3366] flex items-center gap-1.5 transition';
        a.innerHTML = `<span>อ่านต้นฉบับที่ ${{escapeHtml(src.name || 'Source')}}</span><span>↗</span>`;
        sourcesListEl.appendChild(a);
      }});

      applyFontSize();
      
      const modalOverlay = document.getElementById('modalOverlay');
      const modalBody = document.getElementById('modalBody');
      modalOverlay.classList.remove('hidden');
      document.body.style.overflow = 'hidden';

      modalOverlay.scrollTop = 0;
      if (modalBody) modalBody.scrollTop = 0;
      requestAnimationFrame(() => {{
        modalOverlay.scrollTop = 0;
        if (modalBody) modalBody.scrollTop = 0;
      }});
    }}

    function closeModal(fromHistory = false) {{
      if (!isModalOpen) return;
      isModalOpen = false;
      stopSpeech();
      currentModalItem = null;
      document.getElementById('modalVideoContainer').innerHTML = '';
      document.getElementById('modalOverlay').classList.add('hidden');
      document.body.style.overflow = '';

      if (!fromHistory && history.state && history.state.modalOpen) {{
        history.back();
      }}
    }}

    window.addEventListener('popstate', () => {{
      if (isModalOpen) {{
        closeModal(true);
      }}
    }});

    document.addEventListener('keydown', (e) => {{
      if (e.key === 'Escape' && isModalOpen) {{
        closeModal();
      }}
    }});

    window.addEventListener('DOMContentLoaded', () => {{
      const hash = window.location.hash;
      if (hash && hash.startsWith('#news-')) {{
        const newsId = hash.replace('#news-', '');
        const idx = allNews.findIndex(n => String(n.id) === newsId);
        if (idx !== -1) {{
          openModal(idx);
        }}
      }}
    }});

    render();
  </script>
</body>
</html>'''

    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)


def fetch_feed_entries(url: str, category: str, now_utc: datetime.datetime) -> list[dict]:
    """Fetch entries from a single RSS/Atom feed with timeout, source detection, and category filtering."""
    results = []
    source_name = get_source_name(url)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        if getattr(feed, "bozo", False) and not getattr(feed, "entries", None):
            logger.warning(f"[{category}] Malformed feed {url}: {getattr(feed, 'bozo_exception', 'unknown')}")
            return results

        for entry in feed.entries:
            link = (getattr(entry, "link", "") or "").strip()
            title = (getattr(entry, "title", "") or "").strip()
            if not link or not title:
                continue

            t_lower = title.lower()
            if any(x in t_lower for x in ["best deals today", "best deals for", "wordle", "connections hint", "strands hint", "quiz"]):
                continue

            entry_tags = [
                t.get("term", "") for t in (getattr(entry, "tags", None) or [])
                if isinstance(t, dict)
            ]
            tags_str = " ".join(entry_tags)

            if category == "JapanReview":
                if "jp.ign.com" in link.lower():
                    if any(x in entry_tags for x in ["映画", "アニメ", "ドラマ", "エンタメ"]) or title.startswith("映画"):
                        continue
                elif "4gamer.net" in link.lower():
                    rev_kw = ["レビュー", "インプレッション", "プレイレポート", "試遊"]
                    if not any(k in title or k in tags_str for k in rev_kw):
                        continue
            elif category == "JapanNews":
                if "レビュー" in entry_tags or "レビュー" in title:
                    continue

            raw_guid = getattr(entry, "id", "") or getattr(entry, "guid", "") or link
            canonical_link = normalize_url(link)
            guid = normalize_guid(raw_guid, link)

            pub_tuple = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
            if pub_tuple:
                pub_dt = datetime.datetime.fromtimestamp(calendar.timegm(pub_tuple), tz=datetime.timezone.utc)
                if pub_dt > now_utc:
                    pub_dt = now_utc
            else:
                pub_dt = now_utc

            feed_img = extract_entry_thumbnail(entry)
            raw_summary = getattr(entry, "summary", "") or getattr(entry, "description", "")

            results.append({
                "guid": guid,
                "title": title,
                "link": link,
                "canonical_link": canonical_link,
                "source_name": source_name,
                "description": clean_text(raw_summary),
                "published_ts": int(pub_dt.timestamp()),
                "published_at": pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "published_th": to_thailand_time(pub_dt),
                "category_hint": category,
                "feed_image": feed_img,
                "_pub_dt": pub_dt,
            })
    except Exception as e:
        logger.warning(f"[{category}] Feed error for {url}: {e}")
    return results


def fetch_category_feeds(cat: str, feeds: list[str], now_utc: datetime.datetime) -> list[dict]:
    """Fetch all feeds for a category in parallel with isolated fault handling."""
    candidates = []
    seen_guids = set()
    seen_links = set()

    max_workers = min(6, max(1, len(feeds)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_feed_entries, url, cat, now_utc): url for url in feeds}
        for future in concurrent.futures.as_completed(future_to_url):
            feed_url = future_to_url[future]
            try:
                entries = future.result()
            except Exception as e:
                logger.warning(f"[{cat}] Unhandled feed exception for {feed_url}: {e}")
                continue
            for e in entries:
                g = e["guid"]
                cl = e["canonical_link"]
                if g in seen_guids or cl in seen_links:
                    continue
                seen_guids.add(g)
                seen_links.add(cl)
                candidates.append(e)

    return candidates


def normalize_existing_item(it: dict, now_utc: datetime.datetime) -> dict | None:
    """Normalize schema, GUID, canonical link, sources, timestamps, and images for an existing DB entry."""
    link = (it.get("link") or "").strip()
    if not link:
        return None

    tag = it.get("tag", "") or it.get("category_hint", "")
    if tag not in CATEGORIES_ORDER:
        tag = "GlobalNews"
    it["tag"] = tag
    it["category_hint"] = tag

    canonical_link = normalize_url(link)
    guid = normalize_guid(it.get("guid", ""), link)
    it["canonical_link"] = canonical_link
    it["guid"] = guid

    if not it.get("source_name"):
        it["source_name"] = get_source_name(link)
    if not isinstance(it.get("sources"), list) or not it["sources"]:
        it["sources"] = [{"name": it["source_name"], "url": link, "title": it.get("original_title", "")}]

    pub_dt = parse_existing_timestamp(it, now_utc)
    it["published_ts"] = int(pub_dt.timestamp())
    it["published_at"] = pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    it["published_th"] = to_thailand_time(pub_dt)

    if it.get("gallery_images"):
        it["gallery_images"] = [
            make_hd_image_url(g) for g in it["gallery_images"]
            if _is_usable_image_url(g)
        ]
    if "videos" not in it or not isinstance(it["videos"], list):
        it["videos"] = []
    if "review_score" not in it:
        it["review_score"] = ""
    it["sub_tag"] = extract_sub_tag(it)
    it["cover_image_url"] = make_hd_image_url(it.get("cover_image_url", ""))
    return it


def _deduplicate_cover_images(articles: list[dict], cat: str, global_used_images: set):
    for idx_art, art in enumerate(articles):
        img = (art.get("cover_image_url") or "").strip()
        if not img or img in global_used_images:
            found_unique = False
            for g in art.get("gallery_images", []):
                if g and g not in global_used_images:
                    art["cover_image_url"] = g
                    global_used_images.add(g)
                    found_unique = True
                    break
            if not found_unique:
                fallback_img = CURATED_GAMING_COVERS[idx_art % len(CURATED_GAMING_COVERS)]
                if fallback_img in global_used_images:
                    base_fb = CATEGORY_FALLBACK_COVERS.get(cat, CURATED_GAMING_COVERS[0])
                    fallback_img = f"{base_fb}&sig={cat.lower()}-{idx_art}"
                art["cover_image_url"] = fallback_img
                global_used_images.add(fallback_img)
        else:
            global_used_images.add(img)


def load_existing_database(now_utc: datetime.datetime) -> tuple[list[dict], dict[str, dict], dict[str, dict], dict[str, list[dict]]]:
    existing_news = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                existing_news = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load existing database {DATA_FILE}: {e}")

    normalized_items = []
    existing_by_guid = {}
    existing_by_link = {}
    existing_by_cat = {cat: [] for cat in CATEGORIES_ORDER}

    for raw in existing_news:
        it = normalize_existing_item(dict(raw), now_utc)
        if not it:
            continue
        cat = it.get("tag", "")
        normalized_items.append(it)
        existing_by_guid[(cat, it["guid"])] = it
        existing_by_link[(cat, it["canonical_link"])] = it
        existing_by_link[(cat, it["link"].strip())] = it
        for src in it.get("sources", []):
            s_url = normalize_url(src.get("url", ""))
            if s_url:
                existing_by_link[(cat, s_url)] = it
        if cat in existing_by_cat:
            existing_by_cat[cat].append(it)

    for cat in existing_by_cat:
        existing_by_cat[cat].sort(key=lambda x: x.get("published_ts", 0), reverse=True)

    return normalized_items, existing_by_guid, existing_by_link, existing_by_cat


def run():
    logger.info("==================================================")
    logger.info(f"  STARTING PURE GAMER PIPELINE ({TARGET_PER_CATEGORY} PER TOPIC)")
    logger.info("==================================================")

    os.makedirs(DATA_DIR, exist_ok=True)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    normalized_items, existing_by_guid, existing_by_link, existing_by_cat = load_existing_database(now_utc)
    logger.info(f"Loaded {len(normalized_items)} existing stories from database.")

    # Fetch all category feeds up front; keep Global vs Japan pools separated for clean regional clustering
    raw_by_cat: dict[str, list[dict]] = {}
    for cat in CATEGORIES_ORDER:
        entries = fetch_category_feeds(cat, CATEGORIES_CONFIG[cat], now_utc)
        entries.sort(key=lambda x: x["published_ts"], reverse=True)
        raw_by_cat[cat] = entries
        logger.info(f"[{cat}] Fetched {len(entries)} raw feed candidates.")

    global_pool = raw_by_cat.get("GlobalNews", []) + raw_by_cat.get("GlobalReview", [])
    japan_pool = raw_by_cat.get("JapanNews", []) + raw_by_cat.get("JapanReview", [])

    category_results = {}
    global_used_images = set()

    for cat in CATEGORIES_ORDER:
        logger.info(f"--- Processing Category: {cat} ---")
        candidates = raw_by_cat[cat]
        region_pool = global_pool if cat.startswith("Global") else japan_pool
        clustered = cluster_candidates(candidates, region_pool)
        clustered.sort(key=lambda x: x["published_ts"], reverse=True)

        # Ensure source diversity among top selected stories while keeping newest-first priority
        selected_candidates = []
        picked_links = set()
        seen_primary_sources = set()
        for c in clustered:
            if c["source_name"] not in seen_primary_sources:
                selected_candidates.append(c)
                picked_links.add(c["canonical_link"])
                seen_primary_sources.add(c["source_name"])
            if len(selected_candidates) >= TARGET_PER_CATEGORY:
                break
        for c in clustered:
            if len(selected_candidates) >= TARGET_PER_CATEGORY:
                break
            if c["canonical_link"] not in picked_links:
                selected_candidates.append(c)
                picked_links.add(c["canonical_link"])

        final_category_articles = []
        new_items_to_synthesize = []
        matched_existing_map = {}

        for item in selected_candidates:
            link = item["link"]
            existing_item = (
                existing_by_guid.get((cat, item["guid"]))
                or existing_by_link.get((cat, item["canonical_link"]))
                or existing_by_link.get((cat, link))
            )
            if not existing_item:
                for src in item.get("sources", []):
                    existing_item = existing_by_link.get((cat, normalize_url(src.get("url", ""))))
                    if existing_item:
                        break

            if existing_item:
                matched_existing_map[item["canonical_link"]] = existing_item
                if is_valid_translation(existing_item):
                    existing_item["tag"] = cat
                    existing_item["category_hint"] = cat
                    existing_item["sub_tag"] = extract_sub_tag(existing_item)
                    final_category_articles.append(existing_item)
                else:
                    new_items_to_synthesize.append(item)
            else:
                new_items_to_synthesize.append(item)

        logger.info(
            f"[{cat}] Reusing {len(final_category_articles)} cached stories. "
            f"Synthesizing {len(new_items_to_synthesize)} new stories."
        )

        failed_fallback_entries = []
        if new_items_to_synthesize:
            logger.info(f"[{cat}] Scraping multi-source context for {len(new_items_to_synthesize)} stories...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                scraped_items = list(executor.map(scrape_article_details, new_items_to_synthesize))

            logger.info(f"[{cat}] Synthesizing editorial articles with {GEMINI_MODEL}...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                editorials = list(executor.map(call_gemini, scraped_items))

            for idx_new, (item, ed) in enumerate(zip(scraped_items, editorials)):
                prev_existing = matched_existing_map.get(item["canonical_link"])
                clean_sources = [
                    {"name": s["name"], "url": s["url"]}
                    for s in (item.get("sources") or [{"name": item["source_name"], "url": item["link"]}])
                ]
                entry = {
                    "id": int(time.time() * 1000) + len(final_category_articles) + idx_new,
                    "guid": item["guid"],
                    "original_title": item["title"],
                    "thai_title": ed.get("thai_title", item["title"]),
                    "tag": cat,
                    "sub_tag": "",
                    "source_name": item["source_name"],
                    "sources": clean_sources,
                    "bullet_points": ed.get("bullet_points", []),
                    "full_paragraphs": ed.get("full_paragraphs", item["raw_paragraphs"][:4]),
                    "link": item["link"],
                    "canonical_link": item["canonical_link"],
                    "cover_image_url": item["cover_image_url"],
                    "gallery_images": item.get("gallery_images", []),
                    "videos": item.get("videos", []),
                    "review_score": str(ed.get("review_score") or item.get("review_score") or "").strip(),
                    "published_ts": item["published_ts"],
                    "published_at": item["published_at"],
                    "published_th": item["published_th"],
                    "category_hint": cat,
                }
                entry["sub_tag"] = extract_sub_tag(entry)
                if ed.get("_translation_failed"):
                    if prev_existing and is_valid_translation(prev_existing):
                        final_category_articles.append(prev_existing)
                    else:
                        failed_fallback_entries.append(entry)
                    continue

                final_category_articles.append(entry)
                existing_by_guid[(cat, item["guid"])] = entry
                existing_by_link[(cat, item["canonical_link"])] = entry

        if len(final_category_articles) < TARGET_PER_CATEGORY:
            used_guids = {a.get("guid") for a in final_category_articles if a.get("guid")}
            used_links = {a.get("canonical_link") for a in final_category_articles if a.get("canonical_link")}
            for cached in existing_by_cat.get(cat, []):
                if cached.get("guid") in used_guids or cached.get("canonical_link") in used_links:
                    continue
                if not is_valid_translation(cached):
                    continue
                final_category_articles.append(cached)
                if len(final_category_articles) >= TARGET_PER_CATEGORY:
                    break

        if len(final_category_articles) < TARGET_PER_CATEGORY and failed_fallback_entries:
            for fb_entry in failed_fallback_entries:
                final_category_articles.append(fb_entry)
                if len(final_category_articles) >= TARGET_PER_CATEGORY:
                    break

        final_category_articles.sort(key=lambda x: x.get("published_ts", 0), reverse=True)
        final_category_articles = final_category_articles[:TARGET_PER_CATEGORY]

        _deduplicate_cover_images(final_category_articles, cat, global_used_images)
        category_results[cat] = [sanitize_article(x) for x in final_category_articles]
        logger.info(f"[{cat}] Completed! Total articles: {len(category_results[cat])}/{TARGET_PER_CATEGORY}")

    combined_db = []
    for cat in CATEGORIES_ORDER:
        combined_db.extend(category_results.get(cat, []))

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(combined_db, f, ensure_ascii=False, indent=2)

    update_ui_files(combined_db)

    logger.info("==================================================")
    logger.info(f"[✓] Pure Gamer pipeline complete! Maintained {len(combined_db)} stories.")
    logger.info("==================================================")


if __name__ == "__main__":
    run()
