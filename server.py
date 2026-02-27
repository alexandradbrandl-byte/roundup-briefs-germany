"""
server.py — Web server + API
"""

import os
import threading
import hashlib
import requests as http_requests
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from scraper import get_all_articles, get_connection, setup_database, scrape_all_feeds, USE_POSTGRES, KEYWORDS
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_folder=".")
CORS(app)

BREVO_API_KEY    = os.environ.get("BREVO_API_KEY", "")
BREVO_SENDER     = os.environ.get("BREVO_SENDER_EMAIL", "alexandra.d.brandl@gmail.com")
BREVO_SENDER_NAME = os.environ.get("BREVO_SENDER_NAME", "shared ground")

TOPIC_META = {
    "Reproduktive Rechte":    {"icon": "🩺", "color": "#E91E8C"},
    "Lohnlücke & Wirtschaft": {"icon": "💰", "color": "#FFA52C"},
    "LGBTQIA+":               {"icon": "🏳️‍🌈", "color": "#7B2FBE"},
    "Migration & Asyl":       {"icon": "🌍", "color": "#00BCD4"},
    "Menschenrechte":         {"icon": "⚖️", "color": "#FF0018"},
    "Gesundheit & Medizin":   {"icon": "🏥", "color": "#4CAF50"},
    "Recht & Politik":        {"icon": "📜", "color": "#9C27B0"},
    "Politik & Regierung":    {"icon": "🏛️", "color": "#3F51B5"},
    "Kultur & Medien":        {"icon": "🎭", "color": "#FF9800"},
    "Sport":                  {"icon": "⚽", "color": "#008018"},
    "Gewalt & Sicherheit":    {"icon": "🛡️", "color": "#F44336"},
    "Arbeit & Wirtschaft":    {"icon": "💼", "color": "#607D8B"},
}


def setup_subscribers():
    conn = get_connection()
    cursor = conn.cursor()
    if USE_POSTGRES:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                active BOOLEAN DEFAULT TRUE
            )
        """)
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                subscribed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                active INTEGER DEFAULT 1
            )
        """)
    conn.commit()
    conn.close()


def make_unsubscribe_token(email):
    secret = BREVO_API_KEY[:8] if BREVO_API_KEY else "sg-secret"
    return hashlib.sha256(f"{email}{secret}".encode()).hexdigest()[:16]


def build_newsletter_html(articles, unsubscribe_url):
    date_str = datetime.now().strftime("%-d. %B %Y")
    rows = ""
    for a in articles:
        source = a.get("source", "")
        title  = a.get("title", "")
        link   = a.get("link", "#")
        summary = (a.get("summary") or "")[:200]
        if summary:
            summary = f'<p style="font-size:14px;color:#555;line-height:1.6;margin:8px 0 0;">{summary}…</p>'
        rows += f"""
        <div style="border-bottom:1px solid #eee;padding:20px 0;">
          <p style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px;margin:0;">{source}</p>
          <p style="font-size:18px;font-weight:bold;color:#1a1a1a;margin:8px 0;line-height:1.4;">{title}</p>
          {summary}
          <p style="margin:10px 0 0;">
            <a href="{link}" style="color:#4c1d95;font-size:13px;font-weight:bold;text-decoration:none;">
              Artikel lesen &#x2192;
            </a>
          </p>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="de">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:Georgia,serif;">
  <div style="max-width:600px;margin:0 auto;background:white;">

    <!-- Header -->
    <div style="background:#4c1d95;padding:36px 32px;text-align:center;">
      <h1 style="color:white;font-size:28px;margin:0;letter-spacing:3px;font-weight:bold;">
        shared ground
      </h1>
      <p style="color:rgba(255,255,255,0.75);font-size:13px;margin:10px 0 0;">
        Die Woche im Rückblick &mdash; {date_str}
      </p>
    </div>

    <!-- Intro -->
    <div style="padding:28px 32px 0;">
      <p style="font-size:15px;color:#333;line-height:1.7;margin:0;">
        Hier sind die wichtigsten Nachrichten aus Feminismus, Frauen und LGBTQIA+
        der vergangenen Woche &mdash; kuratiert von shared ground.
      </p>
    </div>

    <!-- Articles -->
    <div style="padding:8px 32px 32px;">
      {rows}
    </div>

    <!-- Footer -->
    <div style="background:#f5f5f5;padding:24px 32px;text-align:center;border-top:1px solid #eee;">
      <p style="font-size:12px;color:#999;margin:0;">
        Du erhältst diesen Newsletter weil du dich auf
        <a href="https://shared-ground-frontend.vercel.app" style="color:#4c1d95;">shared-ground-frontend.vercel.app</a>
        angemeldet hast.
      </p>
      <p style="font-size:12px;color:#999;margin:8px 0 0;">
        <a href="{unsubscribe_url}" style="color:#999;">Abmelden</a>
      </p>
    </div>

  </div>
</body>
</html>"""


def get_top_articles_this_week(limit=8):
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
    cursor.execute(
        f"""SELECT title, link, summary, source, published_at, scraped_at
            FROM articles
            WHERE scraped_at >= {ph}
            ORDER BY scraped_at DESC
            LIMIT {limit}""",
        [seven_days_ago]
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {"title": r[0], "link": r[1], "summary": r[2], "source": r[3]}
        for r in rows
    ]


def get_active_subscribers():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT email FROM subscribers WHERE active = 1")
    emails = [row[0] for row in cursor.fetchall()]
    conn.close()
    return emails


def send_newsletter():
    if not BREVO_API_KEY:
        print("Kein BREVO_API_KEY gesetzt — Newsletter nicht gesendet.")
        return

    articles = get_top_articles_this_week(limit=8)
    if not articles:
        print("Keine Artikel für Newsletter gefunden.")
        return

    subscribers = get_active_subscribers()
    if not subscribers:
        print("Keine Subscribers — Newsletter nicht gesendet.")
        return

    print(f"Sende Newsletter an {len(subscribers)} Subscriber(s)...")
    date_str = datetime.now().strftime("%-d. %B %Y")
    success = 0

    for email in subscribers:
        token = make_unsubscribe_token(email)
        base_url = os.environ.get("BASE_URL", "https://roundup-briefs-germany.onrender.com")
        unsubscribe_url = f"{base_url}/api/newsletter/unsubscribe?email={email}&token={token}"
        html = build_newsletter_html(articles, unsubscribe_url)

        payload = {
            "sender": {"name": BREVO_SENDER_NAME, "email": BREVO_SENDER},
            "to": [{"email": email}],
            "subject": f"shared ground — Die Woche im Rückblick ({date_str})",
            "htmlContent": html,
        }
        try:
            res = http_requests.post(
                "https://api.brevo.com/v3/smtp/email",
                json=payload,
                headers={"api-key": BREVO_API_KEY, "Content-Type": "application/json"},
                timeout=15,
            )
            if res.status_code in (200, 201):
                success += 1
            else:
                print(f"Fehler bei {email}: {res.status_code} {res.text}")
        except Exception as e:
            print(f"Fehler beim Senden an {email}: {e}")

    print(f"Newsletter gesendet: {success}/{len(subscribers)} erfolgreich.")


def resolve_time_range(label):
    now = datetime.now()
    if label == "today":
        return now.replace(hour=0, minute=0, second=0).isoformat()
    elif label == "this_week":
        start = now - timedelta(days=now.weekday())
        return start.replace(hour=0, minute=0, second=0).isoformat()
    elif label == "last_week":
        start = now - timedelta(days=now.weekday() + 7)
        return start.replace(hour=0, minute=0, second=0).isoformat()
    elif label == "last_month":
        return (now - timedelta(days=30)).isoformat()
    elif label == "last_year":
        return (now - timedelta(days=365)).isoformat()
    return None


# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/api/articles")
def articles():
    category   = request.args.get("category")
    source     = request.args.get("source")
    country    = request.args.get("country")
    search     = request.args.get("search")
    topic      = request.args.get("topic")
    time_label = request.args.get("time")
    date_from  = request.args.get("date_from")
    date_to    = request.args.get("date_to")
    limit      = int(request.args.get("limit", 200))
    time_range = date_from if date_from else (resolve_time_range(time_label) if time_label else None)
    results = get_all_articles(
        category=category, source=source, search=search,
        topic=topic, country=country, time_range=time_range,
        date_to=date_to, limit=limit
    )
    return jsonify(results)


@app.route("/api/sources")
def sources():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT source FROM articles ORDER BY source")
    result = [row[0] for row in cursor.fetchall()]
    conn.close()
    return jsonify(result)


@app.route("/api/countries")
def countries():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT country FROM articles WHERE country != '' ORDER BY country")
    result = [row[0] for row in cursor.fetchall()]
    conn.close()
    return jsonify(result)


@app.route("/api/topics")
def topics():
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    result = []
    for topic_name, meta in TOPIC_META.items():
        cursor.execute(
            f"SELECT COUNT(*) FROM articles WHERE topics LIKE {ph}",
            [f"%{topic_name}%"]
        )
        count = cursor.fetchone()[0]
        result.append({
            "name": topic_name,
            "count": count,
            "icon": meta["icon"],
            "color": meta["color"],
        })
    conn.close()
    result.sort(key=lambda x: x["count"], reverse=True)
    return jsonify(result)


@app.route("/api/stats")
def stats():
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    cursor.execute("SELECT COUNT(*) FROM articles")
    total = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM articles WHERE tags LIKE {ph}", ['%lgbtqia+%'])
    lgbtq = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM articles WHERE tags LIKE {ph}", ['%women%'])
    women = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(scraped_at) FROM articles")
    last_scraped = cursor.fetchone()[0]
    conn.close()
    return jsonify({
        "total": total,
        "lgbtqia_plus": lgbtq,
        "women": women,
        "last_scraped": last_scraped
    })


@app.route("/api/analytics/sources")
def analytics_sources():
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
    cursor.execute(
        f"SELECT source, COUNT(*) as count FROM articles WHERE scraped_at >= {ph} GROUP BY source ORDER BY count DESC",
        [seven_days_ago]
    )
    result = [{"source": row[0], "count": row[1]} for row in cursor.fetchall()]
    conn.close()
    return jsonify(result)


@app.route("/api/analytics/daily")
def analytics_daily():
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    ninety_days_ago = (datetime.now() - timedelta(days=90)).isoformat()
    if USE_POSTGRES:
        cursor.execute(
            f"SELECT DATE(scraped_at::timestamp) as day, COUNT(*) as count FROM articles WHERE scraped_at >= {ph} GROUP BY day ORDER BY day",
            [ninety_days_ago]
        )
    else:
        cursor.execute(
            f"SELECT DATE(scraped_at) as day, COUNT(*) as count FROM articles WHERE scraped_at >= {ph} GROUP BY day ORDER BY day",
            [ninety_days_ago]
        )
    result = [{"date": str(row[0]), "count": row[1]} for row in cursor.fetchall()]
    conn.close()
    return jsonify(result)


@app.route("/api/analytics/keywords")
def analytics_keywords():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT title, summary FROM articles")
    rows = cursor.fetchall()
    conn.close()
    keyword_counts = {}
    for row in rows:
        text = (row[0] + " " + row[1]).lower()
        for kw in KEYWORDS:
            if kw in text and len(kw) > 4:
                keyword_counts[kw] = keyword_counts.get(kw, 0) + 1
    result = [
        {"keyword": k, "count": v}
        for k, v in sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    ]
    return jsonify(result)


# ── Newsletter ────────────────────────────────────────────────────────────────

@app.route("/api/newsletter/subscribe", methods=["POST"])
def newsletter_subscribe():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email or "." not in email.split("@")[-1]:
        return jsonify({"error": "Bitte gib eine gültige E-Mail-Adresse ein."}), 400
    try:
        conn = get_connection()
        cursor = conn.cursor()
        ph = "%s" if USE_POSTGRES else "?"
        cursor.execute(f"INSERT INTO subscribers (email) VALUES ({ph})", [email])
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        err = str(e).lower()
        if "unique" in err or "duplicate" in err:
            return jsonify({"error": "Diese E-Mail ist bereits angemeldet."}), 409
        return jsonify({"error": "Fehler beim Speichern. Bitte versuch es später."}), 500


@app.route("/api/newsletter/unsubscribe")
def newsletter_unsubscribe():
    email = (request.args.get("email") or "").strip().lower()
    token = request.args.get("token", "")
    if not email or token != make_unsubscribe_token(email):
        return "Ungültiger Abmelde-Link.", 400
    try:
        conn = get_connection()
        cursor = conn.cursor()
        ph = "%s" if USE_POSTGRES else "?"
        cursor.execute(f"UPDATE subscribers SET active = 0 WHERE email = {ph}", [email])
        conn.commit()
        conn.close()
        return "<html><body style='font-family:sans-serif;text-align:center;padding:60px'><h2>Abgemeldet</h2><p>Du wurdest erfolgreich vom shared ground Newsletter abgemeldet.</p></body></html>"
    except Exception as e:
        return "Fehler beim Abmelden.", 500


@app.route("/api/newsletter/subscribers")
def newsletter_subscribers():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM subscribers WHERE active = 1")
    count = cursor.fetchone()[0]
    conn.close()
    return jsonify({"count": count})


@app.route("/api/newsletter/send-now")
def newsletter_send_now():
    """Manual trigger for testing."""
    thread = threading.Thread(target=send_newsletter)
    thread.start()
    return jsonify({"status": "Newsletter wird gesendet..."})


# ── Scrape ────────────────────────────────────────────────────────────────────

@app.route("/api/scrape")
def trigger_scrape():
    def do_scrape():
        scrape_all_feeds()
    thread = threading.Thread(target=do_scrape)
    thread.start()
    return jsonify({"status": "Scraping gestartet!"})


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


def startup():
    setup_database()
    setup_subscribers()

    def initial_scrape():
        try:
            print("Running initial scrape...", flush=True)
            scrape_all_feeds()
            print("Initial scrape complete!", flush=True)
        except Exception as e:
            print(f"Initial scrape failed: {e}", flush=True)

    thread = threading.Thread(target=initial_scrape)
    thread.start()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        lambda: scrape_all_feeds(),
        'interval', hours=12,
        id='scheduled_scrape'
    )
    # Jeden Sonntag um 8:00 Uhr Newsletter versenden
    scheduler.add_job(
        send_newsletter,
        'cron',
        day_of_week='sun',
        hour=8,
        minute=0,
        id='sunday_newsletter'
    )
    scheduler.start()
    print("Scheduler aktiv — scrapet alle 12h, Newsletter jeden Sonntag um 8 Uhr.")


startup()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
