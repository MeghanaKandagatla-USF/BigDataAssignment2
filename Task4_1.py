import psycopg2
import time

def connect_db():
    """Connects to the PostgreSQL database."""
    return psycopg2.connect(
        host="localhost",
        database="streamflix",
        user="student",
        password="student"
    )

def generate_performance_report(conn):
    """
    Generates a comprehensive performance comparison report.
    """
    print("Generating performance report...")

    # --- 2. Query performance table (before/after/improvement %) ---
    # 'before' refers to the indexed monolithic table, 'after' is the partitioned table.
    queries = {
        "daily_active_users": "SELECT COUNT(DISTINCT user_id) FROM {} WHERE event_timestamp >= NOW() - INTERVAL '7 days';",
        "top_10_content": "SELECT content_id, COUNT(*) AS view_count FROM {} WHERE event_timestamp >= NOW() - INTERVAL '24 hours' AND event_type = 'start' GROUP BY content_id ORDER BY view_count DESC LIMIT 10;"
    }
    
    # Benchmark the indexed monolithic table
    timings_before = {}
    with conn.cursor() as cur:
        for name, query_template in queries.items():
            start_time = time.perf_counter()
            cur.execute(query_template.format('viewing_events'))
            cur.fetchall()
            end_time = time.perf_counter()
            timings_before[name] = (end_time - start_time) * 1000

    # Benchmark the partitioned table
    timings_after = {}
    with conn.cursor() as cur:
        for name, query_template in queries.items():
            start_time = time.perf_counter()
            cur.execute(query_template.format('viewing_events_partitioned'))
            cur.fetchall()
            end_time = time.perf_counter()
            timings_after[name] = (end_time - start_time) * 1000
    
    query_performance = {}
    for name in timings_before:
        before_ms = timings_before[name]
        after_ms = timings_after.get(name, 0)
        improvement = (before_ms - after_ms) / before_ms * 100 if before_ms > 0 else 0
        query_performance[name] = {
            'indexed_ms': f"{before_ms:.2f}",
            'partitioned_ms': f"{after_ms:.2f}",
            'improvement_%': f"{improvement:.2f}"
        }

    # --- 3. Storage impact analysis ---
    storage_analysis = {}
    with conn.cursor() as cur:
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size('viewing_events'));")
        storage_analysis['monolithic_table_size'] = cur.fetchone()[0]
        cur.execute("SELECT pg_size_pretty(SUM(pg_relation_size(indexrelid))) FROM pg_stat_user_indexes WHERE relname = 'viewing_events';")
        storage_analysis['monolithic_indexes_size'] = cur.fetchone()[0]
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size('viewing_events_partitioned'));")
        storage_analysis['partitioned_table_size'] = cur.fetchone()[0]
    
    # Assemble the final report dictionary
    report = {
        'executive_summary': (
            "Implementing a strategy of indexing and table partitioning will resolve the current analytics performance crisis. "
            "This ensures both immediate query speed improvements and long-term database scalability and manageability."
        ),
        'query_performance': query_performance,
        'storage_analysis': storage_analysis,
        'maintenance_benefits': {
            "VACUUM/ANALYZE": "Can be run on smaller, individual partitions, reducing maintenance windows.",
            "Data Archival": "Old data can be removed instantly by dropping a partition instead of a slow DELETE command."
        },
        'recommendations': [
            "Deploy indexes immediately for quick performance gains.",
            "Adopt a phased rollout for the partitioning strategy to minimize risk.",
            "Implement robust monitoring post-deployment to track query latency."
        ]
    }
    
    print("Report generation complete.")
    return report

def print_report(report):
    """Helper function to print the generated report in a readable format."""
    print("\n\n" + "="*50)
    print("      StreamFlix Performance Optimization Report")
    print("="*50)
    print("\n## 1. Executive Summary\n" + report['executive_summary'])
    print("\n## 2. Query Performance (Indexed vs. Partitioned)")
    for name, data in report['query_performance'].items():
        print(f"  - {name}:\n    - Indexed: {data['indexed_ms']} ms\n    - Partitioned: {data['partitioned_ms']} ms\n    - Improvement: {data['improvement_%']}%")
    print("\n## 3. Storage Impact Analysis")
    print(f"  - Monolithic Table Size: {report['storage_analysis']['monolithic_table_size']}")
    print(f"  - Index Size (Monolithic): {report['storage_analysis']['monolithic_indexes_size']}")
    print(f"  - Partitioned Table Size: {report['storage_analysis']['partitioned_table_size']}")
    print("\n## 4. Maintenance Benefits")
    for op, benefit in report['maintenance_benefits'].items():
        print(f"  - {op}: {benefit}")
    print("\n## 5. Recommendations")
    for rec in report['recommendations']:
        print(f"  - {rec}")
    print("\n" + "="*50)

if __name__ == '__main__':
    conn = None
    try:
        conn = connect_db()
        final_report = generate_performance_report(conn)
        print_report(final_report)
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()