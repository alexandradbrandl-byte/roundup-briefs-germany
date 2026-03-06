"""
scraper.py
Fetches articles from news sources worldwide and filters by keywords related to
women, feminism, and LGBTQIA+ topics. Saves results to a database.
Supports both SQLite (local) and PostgreSQL (production on Render).
Translates non-German titles to German via MyMemory (free, no API key needed).
"""

import feedparser
import hashlib
import re
import os
import requests as http_req
from datetime import datetime, timezone

# ── Database setup ─────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras
    USE_POSTGRES = True
else:
    import sqlite3
    USE_POSTGRES = False

DB_FILE = "news.db"


def get_connection():
    if USE_POSTGRES:
        return psycopg2.connect(DATABASE_URL)
    else:
        return sqlite3.connect(DB_FILE)


def translate_to_german(text, source_lang):
    """Translate text to German via MyMemory (free, no API key needed)."""
    if not text or source_lang == "DE":
        return text
    try:
        lang_pair = f"{source_lang.lower()}|de"
        resp = http_req.get(
            "https://api.mymemory.translated.net/get",
            params={
                "q": text[:500],
                "langpair": lang_pair,
                "de": "alexandra.d.brandl@gmail.com",
            },
            timeout=8,
        )
        if resp.ok:
            data = resp.json()
            if data.get("responseStatus") == 200:
                return data["responseData"]["translatedText"]
    except Exception:
        pass
    return text


# ─────────────────────────────────────────────────────────────────────────────
#  NEWS SOURCES
#  Format: "Display Name": {"url": "...", "country": "...", "language": "XX"}
# ─────────────────────────────────────────────────────────────────────────────
FEEDS = {
    # ── Deutschland ───────────────────────────────────────────────────────────
    "Spiegel Online":       {"url": "https://www.spiegel.de/schlagzeilen/tops/index.rss",              "country": "Germany",       "language": "DE"},
    "Zeit Online":          {"url": "https://newsfeed.zeit.de/index",                                  "country": "Germany",       "language": "DE"},
    "FAZ":                  {"url": "https://www.faz.net/rss/aktuell/",                                "country": "Germany",       "language": "DE"},
    "Sueddeutsche Zeitung": {"url": "https://rss.sueddeutsche.de/rss/Topthemen",                       "country": "Germany",       "language": "DE"},
    "Die Welt":             {"url": "https://www.welt.de/feeds/latest.rss",                            "country": "Germany",       "language": "DE"},
    "Tagesspiegel":         {"url": "https://www.tagesspiegel.de/feeds/",                              "country": "Germany",       "language": "DE"},
    "Focus Online":         {"url": "https://rss.focus.de/fol/XML/rss_folnews.xml",                    "country": "Germany",       "language": "DE"},
    "Tagesschau":           {"url": "https://www.tagesschau.de/xml/rss2/",                             "country": "Germany",       "language": "DE"},
    "ZDF heute":            {"url": "https://www.zdf.de/rss/zdf/nachrichten",                          "country": "Germany",       "language": "DE"},
    "Deutschlandfunk":      {"url": "https://www.deutschlandfunk.de/nachrichten-100.rss",              "country": "Germany",       "language": "DE"},
    "BR24":                 {"url": "https://www.br.de/nachrichten/rss/meldungen.xml",                 "country": "Germany",       "language": "DE"},
    "MDR Nachrichten":      {"url": "https://www.mdr.de/nachrichten/index-rss.xml",                    "country": "Germany",       "language": "DE"},
    "NDR Nachrichten":      {"url": "https://www.ndr.de/nachrichten/index-rss.xml",                    "country": "Germany",       "language": "DE"},
    "taz":                  {"url": "https://taz.de/!p4608;rss/",                                      "country": "Germany",       "language": "DE"},
    "Freitag":              {"url": "https://www.freitag.de/feeds/all",                                "country": "Germany",       "language": "DE"},
    "EMMA":                 {"url": "https://www.emma.de/feeds/gesamtinhalt",                          "country": "Germany",       "language": "DE"},
    "queer.de":             {"url": "https://www.queer.de/feed.php",                                   "country": "Germany",       "language": "DE"},
    "L-MAG":                {"url": "https://www.l-mag.de/feed/",                                      "country": "Germany",       "language": "DE"},

    # ── Österreich ────────────────────────────────────────────────────────────
    "Der Standard":         {"url": "https://derstandard.at/?page=rss&ressort=Frontpage",              "country": "Austria",       "language": "DE"},
    "ORF News":             {"url": "https://rss.orf.at/news.xml",                                     "country": "Austria",       "language": "DE"},
    "Die Presse":           {"url": "https://diepresse.com/rss/",                                      "country": "Austria",       "language": "DE"},
    "Kurier AT":            {"url": "https://kurier.at/rss",                                           "country": "Austria",       "language": "DE"},
    "Kleine Zeitung":       {"url": "https://www.kleinezeitung.at/rss",                                "country": "Austria",       "language": "DE"},
    "profil AT":            {"url": "https://www.profil.at/rss",                                       "country": "Austria",       "language": "DE"},
    "Falter AT":            {"url": "https://www.falter.at/api/rss/feed",                              "country": "Austria",       "language": "DE"},
    "News AT":              {"url": "https://www.news.at/rss",                                         "country": "Austria",       "language": "DE"},
    "Moment AT":            {"url": "https://moment.at/feed/",                                         "country": "Austria",       "language": "DE"},
    "Vienna AT":            {"url": "https://www.vienna.at/feed",                                      "country": "Austria",       "language": "DE"},
    "Trend AT":             {"url": "https://www.trend.at/rss",                                        "country": "Austria",       "language": "DE"},
    "Wiener Zeitung":       {"url": "https://www.wienerzeitung.at/rss",                                "country": "Austria",       "language": "DE"},
    "APA OTS":              {"url": "https://www.ots.at/rss",                                          "country": "Austria",       "language": "DE"},

    # ── Schweiz (deutschsprachig) ─────────────────────────────────────────────
    "NZZ":                  {"url": "https://www.nzz.ch/recent.rss",                                   "country": "Switzerland",   "language": "DE"},
    "SRF News":             {"url": "https://www.srf.ch/news/bnf/rss/1890",                            "country": "Switzerland",   "language": "DE"},
    "Tages-Anzeiger":       {"url": "https://www.tagesanzeiger.ch/rss",                                "country": "Switzerland",   "language": "DE"},
    "20 Minuten CH":        {"url": "https://www.20min.ch/rss/rss.tmpl",                               "country": "Switzerland",   "language": "DE"},
    "Blick CH":             {"url": "https://www.blick.ch/rss",                                        "country": "Switzerland",   "language": "DE"},
    "Watson CH":            {"url": "https://www.watson.ch/rss",                                       "country": "Switzerland",   "language": "DE"},
    "Aargauer Zeitung":     {"url": "https://www.aargauerzeitung.ch/rss",                              "country": "Switzerland",   "language": "DE"},
    "Basler Zeitung":       {"url": "https://www.bazonline.ch/rss",                                    "country": "Switzerland",   "language": "DE"},
    "Der Bund CH":          {"url": "https://www.derbund.ch/rss",                                      "country": "Switzerland",   "language": "DE"},
    "Infosperber CH":       {"url": "https://www.infosperber.ch/feed",                                 "country": "Switzerland",   "language": "DE"},
    "Republik CH":          {"url": "https://www.republik.ch/rss",                                     "country": "Switzerland",   "language": "DE"},
}

ALWAYS_INCLUDE_SOURCES = {
    "EMMA", "queer.de", "L-MAG",
}

# ─────────────────────────────────────────────────────────────────────────────
#  KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────
KEYWORDS = [
    # Deutsch
    "frauen", "frau", "maedchen", "weiblich", "feminismus", "feministisch",
    "gleichberechtigung", "frauenrechte", "lohnluecke", "entgeltungleichheit",
    "abtreibung", "schwangerschaftsabbruch", "sexismus", "misogynie",
    "haeusliche gewalt", "femizid", "sexuelle belaestigung", "metoo",
    "schwul", "lesbisch", "bisexuell", "transgender", "queer",
    "nicht-binaer", "homophobie", "transphobie", "csd", "pride",
    "fluechtling", "migration", "menschenrechte", "diskriminierung",
    "geschlechtergleichheit", "patriarchat", "frauenbewegung",
    "menstruation", "verhuetung", "mutterschaft", "elternzeit",
    "care-arbeit", "glaeserne decke", "frauenquote",
    # Englisch (internationale Begriffe auch in deutschsprachigen Medien)
    "women", "woman", "feminist", "feminism", "gender",
    "reproductive rights", "abortion", "sexism", "misogyny",
    "domestic violence", "femicide", "sexual harassment", "metoo",
    "lgbt", "lgbtq", "lgbtqia", "queer", "gay", "lesbian",
    "transgender", "nonbinary", "non-binary", "pride",
    "human rights", "discrimination",
]

# ─────────────────────────────────────────────────────────────────────────────
#  TOPIC KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────
TOPIC_KEYWORDS = {
    "Reproduktive Rechte": [
        "abtreibung", "schwangerschaftsabbruch", "reproduktive rechte",
        "geburtenkontrolle", "verhuetung", "schwangerschaft", "mutterschaft",
        "menstruation", "frauengesundheit",
        "reproductive", "abortion", "pro-choice", "birth control",
        "contraception", "fertility", "pregnancy", "maternal",
        "aborto", "kurtaj", "avortement",
    ],
    "Lohnluecke & Wirtschaft": [
        "lohnluecke", "entgeltungleichheit", "glaeserne decke", "frauenquote",
        "care-arbeit", "mutterschaftsstrafe", "elterngeld",
        "pay gap", "wage gap", "equal pay", "gender pay", "glass ceiling",
        "motherhood penalty", "parental leave",
        "brecha salarial", "tetto di cristallo", "ecart de salaire",
    ],
    "LGBTQIA+": [
        "schwul", "lesbisch", "bisexuell", "transgender", "queer",
        "nicht-binaer", "homophobie", "transphobie", "csd", "ehe fuer alle",
        "lgbt", "lgbtq", "lgbtqia", "gay", "lesbian",
        "trans rights", "homophobia", "transphobia", "pride", "same-sex",
        "lesbiana", "lesbica", "lesbo", "lezbiyen", "lesbienne",
    ],
    "Migration & Asyl": [
        "fluechtling", "gefluechtete", "migration", "abschiebung",
        "immigration", "refugee", "asylum", "migrant", "deportation",
        "refugiada", "rifugiata", "pakolainen", "multeci", "refugiee",
    ],
    "Menschenrechte": [
        "menschenrechte", "buergerrechte", "diskriminierung", "rassismus",
        "human rights", "civil rights", "discrimination", "racism",
        "derechos humanos", "diritti umani", "droits humains",
    ],
    "Gesundheit & Medizin": [
        "gesundheit", "medizin", "psychische gesundheit", "brustkrebs",
        "health", "healthcare", "mental health", "cancer", "gender affirming care",
        "salud", "salute", "terveys", "sante",
    ],
    "Recht & Politik": [
        "gesetz", "gericht", "urteil", "verbot",
        "law", "legal", "court", "lawsuit", "legislation", "ruling", "ban",
        "ley", "legge", "laki", "kanun", "loi",
    ],
    "Politik & Regierung": [
        "wahl", "bundestag", "politikerin", "koalition",
        "election", "parliament", "minister", "president", "government",
        "eleccion", "elezione", "vaali", "secim", "election",
    ],
    "Kultur & Medien": [
        "film", "buch", "musik", "kunst", "repraesentation",
        "film", "book", "music", "art", "representation",
        "pelicula", "elokuva", "film",
    ],
    "Sport": [
        "sport", "athletin", "olympia", "frauen im sport",
        "athlete", "olympic", "world cup", "women in sport", "transgender athlete",
        "deporte", "urheilu", "spor",
    ],
    "Gewalt & Sicherheit": [
        "gewalt", "femizid", "haeusliche gewalt", "vergewaltigung", "menschenhandel",
        "violence", "femicide", "domestic violence", "rape", "trafficking",
        "violencia", "violenza", "vaekivalta", "siddet", "violence",
    ],
    "Arbeit & Wirtschaft": [
        "arbeitsplatz", "karriere", "kinderbetreuung", "elternzeit",
        "workplace", "career", "leadership", "ceo", "childcare", "parental leave",
        "trabajo", "lavoro", "tyo", "is", "travail",
    ],
}

MAX_ARTICLES_PER_SOURCE = 30


# ─────────────────────────────────────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def setup_database():
    conn = get_connection()
    cursor = conn.cursor()
    if USE_POSTGRES:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id           SERIAL PRIMARY KEY,
                url_hash     TEXT    UNIQUE,
                title        TEXT,
                link         TEXT,
                summary      TEXT,
                source       TEXT,
                country      TEXT    DEFAULT '',
                category     TEXT,
                tags         TEXT,
                topics       TEXT    DEFAULT '',
                scraped_at   TEXT,
                published_at TEXT    DEFAULT '',
                image_url    TEXT    DEFAULT ''
            )
        """)
        cursor.execute("""
            ALTER TABLE articles ADD COLUMN IF NOT EXISTS published_at TEXT DEFAULT ''
        """)
        cursor.execute("""
            ALTER TABLE articles ADD COLUMN IF NOT EXISTS image_url TEXT DEFAULT ''
        """)
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                url_hash     TEXT    UNIQUE,
                title        TEXT,
                link         TEXT,
                summary      TEXT,
                source       TEXT,
                country      TEXT    DEFAULT '',
                category     TEXT,
                tags         TEXT,
                topics       TEXT    DEFAULT '',
                scraped_at   TEXT,
                published_at TEXT    DEFAULT '',
                image_url    TEXT    DEFAULT ''
            )
        """)
        try:
            cursor.execute("ALTER TABLE articles ADD COLUMN published_at TEXT DEFAULT ''")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE articles ADD COLUMN image_url TEXT DEFAULT ''")
        except Exception:
            pass
    conn.commit()
    conn.close()
    print("Database ready.")


def url_hash(url):
    return hashlib.md5(url.encode()).hexdigest()


def strip_html(text):
    return re.sub(r'<[^>]+>', '', text or '').strip()


# ─────────────────────────────────────────────────────────────────────────────
#  KEYWORD MATCHING
# ─────────────────────────────────────────────────────────────────────────────
def get_matching_tags(text):
    text_lower = text.lower()
    matched = []
    women_terms = [
        "frauen", "frau", "weiblich", "feminismus", "feministisch",
        "frauenrechte", "lohnluecke", "abtreibung", "sexismus", "femizid",
        "haeusliche gewalt", "metoo",
        "women", "woman", "girl", "female", "feminism", "feminist",
        "gender", "reproductive", "abortion", "sexism", "domestic violence",
        "mujeres", "mujer", "feminismo", "donne", "donna",
        "naiset", "nainen", "kadin", "femmes", "feminisme",
    ]
    lgbtq_terms = [
        "schwul", "lesbisch", "bisexuell", "transgender", "queer",
        "nicht-binaer", "homophobie", "transphobie", "csd",
        "lgbt", "lgbtq", "lgbtqia", "gay", "lesbian", "bisexual",
        "trans ", "nonbinary", "non-binary", "intersex", "pride",
        "homophobia", "transphobia",
        "lesbiana", "lesbica", "lesbo", "lezbiyen", "lesbienne",
    ]
    if any(t in text_lower for t in women_terms):
        matched.append("women")
    if any(t in text_lower for t in lgbtq_terms):
        matched.append("lgbtqia+")
    return matched


def matches_keywords(title, summary):
    combined = (title + " " + summary).lower()
    return any(kw in combined for kw in KEYWORDS)


def get_topics(text):
    text_lower = text.lower()
    matched = []
    for topic_name, keywords in TOPIC_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            matched.append(topic_name)
    return matched


# ─────────────────────────────────────────────────────────────────────────────
#  SCRAPING
# ─────────────────────────────────────────────────────────────────────────────
def scrape_all_feeds():
    total_new = 0
    ph = "%s" if USE_POSTGRES else "?"

    for source_name, feed_info in FEEDS.items():
        feed_url = feed_info["url"]
        country  = feed_info["country"]
        language = feed_info.get("language", "EN")
        print(f"  Scraping: {source_name}...", flush=True)

        try:
            feed    = feedparser.parse(feed_url)
            entries = feed.entries[:MAX_ARTICLES_PER_SOURCE]
            new_count = 0

            conn   = get_connection()
            cursor = conn.cursor()

            for entry in entries:
                link    = entry.get("link", "")
                title   = strip_html(entry.get("title", "No title"))
                summary = strip_html(entry.get("summary", ""))
                hash_id = url_hash(link)

                # Extract image URL from RSS entry (no extra HTTP requests)
                image_url = ""
                for mc in getattr(entry, "media_content", []) or []:
                    url = mc.get("url", "")
                    if url and (mc.get("medium") == "image" or
                                any(ext in url.lower() for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"])):
                        image_url = url
                        break
                if not image_url:
                    for th in getattr(entry, "media_thumbnail", []) or []:
                        image_url = th.get("url", "")
                        if image_url:
                            break
                if not image_url:
                    for enc in getattr(entry, "enclosures", []) or []:
                        if enc.get("type", "").startswith("image/"):
                            image_url = enc.get("href", enc.get("url", ""))
                            if image_url:
                                break
                if not image_url:
                    raw_content = entry.get("content", [{}])[0].get("value", "") if entry.get("content") else ""
                    img_match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw_content or entry.get("summary", ""))
                    if img_match:
                        image_url = img_match.group(1)

                pub_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                if pub_parsed:
                    try:
                        published_at = datetime(*pub_parsed[:6]).isoformat()
                    except Exception:
                        published_at = datetime.now().isoformat()
                else:
                    published_at = datetime.now().isoformat()

                DACH = {"Germany", "Austria", "Switzerland"}
always_keep = source_name in ALWAYS_INCLUDE_SOURCES or country not in DACH

                # Translate title for keyword matching (non-DE/EN sources)
                title_for_matching = title
                if language not in ("DE", "EN"):
                    title_for_matching = translate_to_german(title, language)

                if not always_keep and not matches_keywords(title_for_matching, summary):
                    continue

                # Use translated title for storage
                stored_title = title_for_matching if language not in ("DE", "EN") else title

                tags = get_matching_tags(title_for_matching + " " + summary)
                if source_name in {"queer.de", "L-MAG"}:
                    tags = list(set(tags + ["lgbtqia+"]))
                elif source_name in {"EMMA"}:
                    tags = list(set(tags + ["women"]))

                category = "lgbtqia+" if "lgbtqia+" in tags else "women"
                tags_str = ", ".join(sorted(set(tags))) if tags else "general"

                topics = get_topics(title_for_matching + " " + summary)
                if source_name in {"queer.de", "L-MAG"}:
                    topics = list(set(topics + ["LGBTQIA+"]))
                topics_str = ", ".join(sorted(set(topics))) if topics else ""

                try:
                    if USE_POSTGRES:
                        cursor.execute(f"""
                            INSERT INTO articles
                              (url_hash, title, link, summary, source, country,
                               category, tags, topics, scraped_at, published_at, image_url)
                            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                            ON CONFLICT (url_hash) DO NOTHING
                        """, (hash_id, stored_title, link, summary, source_name, country,
                              category, tags_str, topics_str, datetime.now().isoformat(),
                              published_at, image_url))
                        if cursor.rowcount > 0:
                            new_count += 1
                    else:
                        cursor.execute(f"""
                            INSERT OR IGNORE INTO articles
                              (url_hash, title, link, summary, source, country,
                               category, tags, topics, scraped_at, published_at, image_url)
                            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                        """, (hash_id, stored_title, link, summary, source_name, country,
                              category, tags_str, topics_str, datetime.now().isoformat(),
                              published_at, image_url))
                        if cursor.rowcount > 0:
                            new_count += 1
                except Exception:
                    pass

            conn.commit()
            conn.close()
            print(f"     {new_count} new articles from {source_name}", flush=True)
            total_new += new_count

        except Exception as e:
            print(f"     Error scraping {source_name}: {e}", flush=True)

    print(f"\nDone! {total_new} new articles saved.", flush=True)


def get_all_articles(category=None, source=None, search=None, topic=None,
                     country=None, time_range=None, date_to=None, limit=200):
    conn = get_connection()
    if USE_POSTGRES:
        import psycopg2.extras
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        ph = "%s"
    else:
        conn.row_factory = __import__('sqlite3').Row
        cursor = conn.cursor()
        ph = "?"

    query  = "SELECT * FROM articles WHERE 1=1"
    params = []

    if category:
        query += f" AND (category = {ph} OR tags LIKE {ph})"
        params += [category, f"%{category}%"]
    if source:
        query += f" AND source = {ph}"
        params.append(source)
    if country:
        query += f" AND country = {ph}"
        params.append(country)
    if search:
        if USE_POSTGRES:
            query += f" AND (title ILIKE {ph} OR summary ILIKE {ph})"
        else:
            query += f" AND (title LIKE {ph} OR summary LIKE {ph})"
        params += [f"%{search}%", f"%{search}%"]
    if topic:
        topic_list = [t.strip() for t in topic.split(",")]
        if USE_POSTGRES:
            topic_clauses = " OR ".join([f"topics ILIKE {ph}" for _ in topic_list])
        else:
            topic_clauses = " OR ".join([f"topics LIKE {ph}" for _ in topic_list])
        query += f" AND ({topic_clauses})"
        params += [f"%{t}%" for t in topic_list]
    if time_range:
        query += f" AND scraped_at >= {ph}"
        params.append(time_range)
    if date_to:
        query += f" AND scraped_at <= {ph}"
        params.append(date_to + "T23:59:59")

    query += f" ORDER BY scraped_at DESC LIMIT {ph}"
    params.append(limit)

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


if __name__ == "__main__":
    print("Shared Ground Scraper startet...\n")
    setup_database()
    scrape_all_feeds()
