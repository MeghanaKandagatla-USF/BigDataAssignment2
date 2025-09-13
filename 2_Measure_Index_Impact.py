import psycopg2
import time
import sys

def connect_db():
    """Connects to the PostgreSQL database."""
    print("Connecting to database...")
    return psycopg2.connect(
        host="localhost",
        database="streamflix",
        user="student",
        password="student"
    )

def measure_index_performance(conn):
    """
    Measures query performance before and after creating indexes.
    """
    
    queries = {
        "daily_active_users": """
            SELECT event_timestamp::date AS day, COUNT(DISTINCT user_id) AS active_users
            FROM viewing_events WHERE event_timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY day ORDER BY day;
        """,
        "top_10_content": """
            SELECT content_id, COUNT(*) AS view_count
            FROM viewing_events WHERE event_timestamp >= NOW() - INTERVAL '24 hours' AND event_type = 'start'
            GROUP BY content_id ORDER BY view_count DESC LIMIT 10;
        """,
        "device_analytics": """
            SELECT device_type, COUNT(*) AS event_count
            FROM viewing_events WHERE event_timestamp >= NOW() - INTERVAL '1 month'
            GROUP BY device_type ORDER BY event_count DESC;
        """
    }
    
    results = {}

    with conn.cursor() as cur:
        # Step 1: Run queries and record times BEFORE creating indexes
        print("\n--- Measuring performance BEFORE indexes ---")
        # Drop any indexes from previous runs to ensure a clean test
        cur.execute("""
            DROP INDEX IF EXISTS idx_viewing_events_timestamp_userid;
            DROP INDEX IF EXISTS idx_viewing_events_timestamp_contentid;
            DROP INDEX IF EXISTS idx_viewing_events_country_timestamp;
            DROP INDEX IF EXISTS idx_viewing_events_device_timestamp;
            DROP INDEX IF EXISTS idx_viewing_events_userid;
        """)
        conn.commit()
        
        for name, query in queries.items():
            start_time = time.perf_counter()
            cur.execute(query)
            cur.fetchall() # Ensure query is fully executed
            end_time = time.perf_counter()
            duration = (end_time - start_time) * 1000  # Convert to milliseconds
            results[name] = {'before_ms': duration}
            print(f"  '{name}' query took: {duration:.2f} ms")

        # Step 2: Create your indexes
        print("\n--- Creating indexes from 2_index_strategy.sql ---")
        try:
            with open('2_index_strategy.sql', 'r') as f:
                sql_script = f.read()
                cur.execute(sql_script)
            conn.commit()
            print("Indexes created successfully.")
        except FileNotFoundError:
            print("ERROR: '2_index_strategy.sql' not found. Please create it in the same directory.")
            return None


        # Step 3: Run queries again AFTER creating indexes
        print("\n--- Measuring performance AFTER indexes ---")
        for name, query in queries.items():
            start_time = time.perf_counter()
            cur.execute(query)
            cur.fetchall() # Ensure query is fully executed
            end_time = time.perf_counter()
            duration = (end_time - start_time) * 1000 # Convert to milliseconds
            results[name]['after_ms'] = duration
            print(f"  '{name}' query took: {duration:.2f} ms")

    # Step 4: Calculate improvement percentages
    print("\n--- Performance Improvement Report ---")
    for name, timings in results.items():
        before = timings['before_ms']
        after = timings['after_ms']
        if after > 0 and before > 0:
            improvement = (before - after) / before * 100
            speedup = before / after
            timings['improvement_%'] = improvement
            print(f"  {name}: {before:.2f}ms -> {after:.2f}ms ({improvement:.2f}% improvement, {speedup:.1f}x faster)")
            
    return results

if __name__ == '__main__':

    output_filename = '2_index_performance_report.txt'
    original_stdout = sys.stdout

    with open(output_filename, 'w') as f:
        sys.stdout = f  

        conn = None
        try:
            conn = connect_db()
            measure_index_performance(conn)
        except psycopg2.Error as e:
            print(f"Database error: {e}")
        finally:
            if conn:
                conn.close()

    sys.stdout = original_stdout  
    print(f"Script finished. Output saved to '{output_filename}'")