import psycopg2
import time
import sys
import statistics as stats

# ---------- DB CONNECTION ----------
def connect_db():
    """Connects to the PostgreSQL database."""
    return psycopg2.connect(
        host="localhost",
        database="streamflix",   # change to "mydb" if that's your DB
        user="student",
        password="student"
    )

# ---------- TIMING HELPERS ----------
def time_query(cur, sql, runs=5):
    """Execute SQL multiple times and return median latency in ms."""
    # warm-up
    cur.execute(sql); cur.fetchall()
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        cur.execute(sql); cur.fetchall()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000.0)
    return stats.median(times)

# ---------- REPORT GENERATOR ----------
def generate_performance_report(conn):
    """
    Generates a comprehensive performance comparison report.

    Returns a dict with keys:
      executive_summary, query_performance, storage_analysis,
      maintenance_benefits, recommendations
    """
    # ---------- queries to benchmark ----------
    queries = {
        "daily_active_users": (
            "SELECT COUNT(DISTINCT user_id) "
            "FROM {} WHERE event_timestamp >= NOW() - INTERVAL '7 days';"
        ),
        "top_10_content": (
            "SELECT content_id, COUNT(*) AS view_count "
            "FROM {} "
            "WHERE event_timestamp >= NOW() - INTERVAL '24 hours' "
            "  AND event_type = 'start' "
            "GROUP BY content_id "
            "ORDER BY view_count DESC "
            "LIMIT 10;"
        ),
    }

    # ---------- timings ----------
    timings_before, timings_after = {}, {}
    with conn.cursor() as cur:
        for name, tpl in queries.items():
            timings_before[name] = time_query(cur, tpl.format('public.viewing_events'))
    with conn.cursor() as cur:
        for name, tpl in queries.items():
            timings_after[name]  = time_query(cur, tpl.format('public.viewing_events_partitioned'))

    query_performance = {}
    for name, before_ms in timings_before.items():
        after_ms = timings_after.get(name, 0.0)
        improvement = (before_ms - after_ms) / before_ms * 100.0 if before_ms > 0 else 0.0
        query_performance[name] = {
            "indexed_ms": f"{before_ms:.2f}",
            "partitioned_ms": f"{after_ms:.2f}",
            "improvement_%": f"{improvement:.2f}",
        }

    # ---------- storage ----------
    storage_analysis = {}
    mono = "public.viewing_events"
    part = "public.viewing_events_partitioned"
    with conn.cursor() as cur:
        # monolithic table+indexes+toast
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size(%s::regclass));", (mono,))
        storage_analysis["monolithic_table_size"] = cur.fetchone()[0]

        # monolithic indexes only
        cur.execute("SELECT pg_size_pretty(pg_indexes_size(%s::regclass));", (mono,))
        storage_analysis["monolithic_indexes_size"] = cur.fetchone()[0]

        # partitioned table total (sum children if inheritance, else parent)
        cur.execute("""
            SELECT pg_size_pretty(
              COALESCE(
                (SELECT SUM(pg_total_relation_size(i.inhrelid))
                   FROM pg_inherits i
                  WHERE i.inhparent = %s::regclass),
                pg_total_relation_size(%s::regclass)
              )
            );
        """, (part, part))
        storage_analysis["partitioned_table_size"] = cur.fetchone()[0]

        # partitioned indexes total (sum children if inheritance, else parent)
        cur.execute("""
            SELECT pg_size_pretty(
              COALESCE(
                (SELECT SUM(pg_indexes_size(i.inhrelid))
                   FROM pg_inherits i
                  WHERE i.inhparent = %s::regclass),
                pg_indexes_size(%s::regclass)
              )
            );
        """, (part, part))
        storage_analysis["partitioned_indexes_size"] = cur.fetchone()[0]

    # ---------- benefits & recommendations ----------
    maintenance_benefits = {
        "VACUUM/ANALYZE": "Run on smaller partitions to reduce maintenance windows and enable parallelism.",
        "Data Archival": "Drop old partitions instantly instead of slow bulk DELETE operations."
    }
    recommendations = [
        "Deploy targeted btree indexes: (event_timestamp), (event_type, event_timestamp), "
        "(content_id, event_timestamp), (user_id, event_timestamp).",
        "Add a BRIN index on event_timestamp for large range scans.",
        "Use monthly/daily partitions on event_timestamp; ensure queries include a time predicate.",
        "Track table and index sizes for the partitioned design; tune autovacuum on hot partitions.",
        "Consider declarative partitioning if you’re on inheritance and PG version supports it."
    ]

    executive_summary = (
        "Indexing plus time-based partitioning improves key analytics query latency and scales with data growth. "
        "Partition pruning reduces scanned data, while per-partition maintenance and archival keep operational overhead low."
    )

    report = {
        "executive_summary": executive_summary,
        "query_performance": query_performance,
        "storage_analysis": storage_analysis,
        "maintenance_benefits": maintenance_benefits,
        "recommendations": recommendations,
    }
    return report

# ---------- REPORT PRINTER ----------
def print_report(report):
    """Pretty print report to stdout."""
    print("\n" + "="*50)
    print("      StreamFlix Performance Optimization Report")
    print("="*50)
    print("\n## 1. Executive Summary\n" + report['executive_summary'])
    print("\n## 2. Query Performance (Indexed vs. Partitioned)")
    for name, data in report['query_performance'].items():
        print(f"  - {name}:\n"
              f"    - Indexed: {data['indexed_ms']} ms\n"
              f"    - Partitioned: {data['partitioned_ms']} ms\n"
              f"    - Improvement: {data['improvement_%']}%")
    print("\n## 3. Storage Impact Analysis")
    print(f"  - Monolithic Table Size: {report['storage_analysis']['monolithic_table_size']}")
    print(f"  - Index Size (Monolithic): {report['storage_analysis']['monolithic_indexes_size']}")
    print(f"  - Partitioned Table Size: {report['storage_analysis']['partitioned_table_size']}")
    print(f"  - Partitioned Index Size: {report['storage_analysis']['partitioned_indexes_size']}")
    print("\n## 4. Maintenance Benefits")
    for op, benefit in report['maintenance_benefits'].items():
        print(f"  - {op}: {benefit}")
    print("\n## 5. Recommendations")
    for rec in report['recommendations']:
        print(f"  - {rec}")
    print("\n" + "="*50)

# ---------- MAIN ----------
if __name__ == "__main__":
    output_filename = "4_optimization_report.txt"
    original_stdout = sys.stdout
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            sys.stdout = f
            conn = connect_db()
            final_report = generate_performance_report(conn)
            print_report(final_report)
            conn.close()
    finally:
        sys.stdout = original_stdout
        print(f"Script finished. Output saved to '{output_filename}'")
