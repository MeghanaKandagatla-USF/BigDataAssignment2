import psycopg2
import psycopg2.extras as extras
import numpy as np
from datetime import datetime, timedelta, timezone
import random
import time
import sys

def connect_db():
    """Connect to PostgreSQL database."""
    print("Connecting to database...")
    return psycopg2.connect(
        host="localhost",
        database="streamflix",
        user="student",
        password="student"
    )

def create_base_table(conn):
    """Create the original monolithic table."""
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS viewing_events CASCADE;

        CREATE TABLE viewing_events (
            event_id BIGSERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            content_id INTEGER NOT NULL,
            event_timestamp TIMESTAMPTZ NOT NULL,
            event_type VARCHAR(50),
            watch_duration_seconds INTEGER,
            device_type VARCHAR(50),
            country_code VARCHAR(2),
            quality VARCHAR(10),
            bandwidth_mbps DECIMAL(6,2),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    print("Base table created")

def generate_viewing_events(conn, num_days=365, events_per_day=10000):
    """ Generate realistic viewing data with skews and batch inserts.
        - Peak hours 7–10 PM
        - 100k users (power-law)
        - 10k content (power-law)
        - Event types: 40% start, 20% pause, 20% resume, 15% complete, 5% skip
        - Devices: 40% mobile, 30% tv, 20% web, 10% tablet
        - Countries: US 40%, UK 15%, CA 10%, AU 10%, Others 25%
        - Batch size: 10k rows
    """
    cur = conn.cursor()

    users_n = 100_000
    content_n = 10_000

    # --- Popularity / power-law weights (Zipf) ---
    rng = np.random.default_rng()
    # Zipf returns 1..inf with heavy head; we map to IDs 1..N
    # Create discrete popularity lists for fast random choice
    user_popularity = rng.zipf(a=1.5, size=users_n * 2)
    user_ids = [((u - 1) % users_n) + 1 for u in user_popularity]

    content_popularity = rng.zipf(a=1.3, size=content_n * 2)
    content_ids = [((c - 1) % content_n) + 1 for c in content_popularity]

    # --- Categorical distributions ---
    event_types = ["start", "pause", "resume", "complete", "skip"]
    event_wts   = [0.40,   0.20,   0.20,    0.15,      0.05]

    devices     = ["mobile", "tv", "web", "tablet"]
    device_wts  = [0.40,     0.30,  0.20,   0.10]

    countries   = ["US", "UK", "CA", "AU", "OT"]  # OT = Others
    country_wts = [0.40, 0.15, 0.10, 0.10, 0.25]
    other_pool  = ["IN","DE","FR","BR","ES","IT","NL","SE","SG","MX"]

    qualities   = ["4K","HD","SD"]
    quality_wts = [0.15, 0.55, 0.30]

    # Peak hour weighting per hour of day
    hour_weights = [1]*24
    for h in [19, 20, 21, 22]:  # 7–10 PM local-ish (use UTC here)
        hour_weights[h] = 6

    batch_size = 10_000
    start_date = (datetime.now(timezone.utc) - timedelta(days=num_days)).date()

    def synth_row(ts):
        uid = int(random.choice(user_ids))
        cid = int(random.choice(content_ids))
        et  = random.choices(event_types, event_wts, k=1)[0]
        dev = random.choices(devices, device_wts, k=1)[0]
        cc  = random.choices(countries, country_wts, k=1)[0]
        if cc == "OT":
            cc = random.choice(other_pool)
        q   = random.choices(qualities, quality_wts, k=1)[0]

        # Watch duration based on event type
        if et == "start":
            dur = random.randint(30, 600)
        elif et in ("pause","resume"):
            dur = random.randint(5, 120)
        elif et == "complete":
            dur = random.randint(1200, 7200)  # 20–120 mins
        else:  # skip
            dur = random.randint(1, 60)

        # Bandwidth by quality (very rough)
        if q == "4K":
            bw = round(random.uniform(15, 45), 2)
        elif q == "HD":
            bw = round(random.uniform(5, 15), 2)
        else:
            bw = round(random.uniform(0.8, 5), 2)

        return (uid, cid, ts, et, dur, dev, cc, q, bw)

    total_rows = num_days * events_per_day
    print(f"Generating {total_rows:,} events over {num_days} days…")

    for d in range(num_days):
        day = start_date + timedelta(days=d)

        # Pre-sample hours with weighting; minutes/seconds uniform
        hours = random.choices(range(24), weights=hour_weights, k=events_per_day)

        # Build rows in batches
        produced = 0
        while produced < events_per_day:
            this_batch = min(batch_size, events_per_day - produced)
            rows = []
            for i in range(this_batch):
                h = hours[produced + i]
                m = random.randint(0, 59)
                s = random.randint(0, 59)
                ts = datetime(day.year, day.month, day.day, h, m, s, tzinfo=timezone.utc)
                rows.append(synth_row(ts))
            # Insert batch
            extras.execute_values(
                cur,
                """
                INSERT INTO viewing_events
                  (user_id, content_id, event_timestamp, event_type,
                   watch_duration_seconds, device_type, country_code,
                   quality, bandwidth_mbps)
                VALUES %s
                """,
                rows,
                page_size=this_batch
            )
            produced += this_batch

        conn.commit()
        print(f"   Day {d+1}/{num_days} inserted ({events_per_day:,} rows)")

    cur.close()
    print("Data generation complete")

def analyze_current_performance(conn):
    """
    Run EXPLAIN ANALYZE on critical queries and return results.
    Returns: dict[str, str]
    """
    cur = conn.cursor()
    results = {}

    q1 = """
    EXPLAIN ANALYZE
    SELECT date_trunc('day', event_timestamp) AS day,
           COUNT(DISTINCT user_id) AS dau
    FROM viewing_events
    WHERE event_timestamp >= NOW() - INTERVAL '7 days'
    GROUP BY 1
    ORDER BY 1;
    """
    q2 = """
    EXPLAIN ANALYZE
    SELECT content_id, COUNT(*) AS views
    FROM viewing_events
    WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
      AND event_type = 'start'
    GROUP BY content_id
    ORDER BY views DESC
    LIMIT 10;
    """
    q3 = """
    EXPLAIN ANALYZE
    SELECT device_type, COUNT(*) AS events
    FROM viewing_events
    WHERE event_timestamp >= date_trunc('month', NOW()) - INTERVAL '1 month'
    GROUP BY device_type
    ORDER BY events DESC;
    """

    for name, sql in [
        ("daily_active_users_7d", q1),
        ("top10_content_24h", q2),
        ("device_analytics_prev_month", q3),
    ]:
        cur.execute(sql)
        plan_lines = cur.fetchall()  # list of ('QUERY PLAN',)
        plan_text = "\n".join(line[0] for line in plan_lines)
        results[name] = plan_text

    cur.close()
    return results

if __name__ == "__main__":
    output_filename = '1_performance_analysis_output.txt'
    original_stdout = sys.stdout 

    with open(output_filename, 'w') as f:
        sys.stdout = f  

        conn = connect_db()
        create_base_table(conn)
        generate_viewing_events(conn, num_days=50, events_per_day=10000)
        plans = analyze_current_performance(conn)
        for k, v in plans.items():
            print(f"\n==== {k} ====\n{v}")
        conn.close()

    sys.stdout = original_stdout 
    print(f"Script finished. Output saved to '{output_filename}'")