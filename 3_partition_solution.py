import psycopg2
import psycopg2.extras
from datetime import date
from dateutil.relativedelta import relativedelta
import time
import sys

class StreamFlixPartitionManager:
    """Automated partition management for StreamFlix viewing events."""

    def __init__(self, conn_params):
        self.conn_params = conn_params
        self.conn = psycopg2.connect(**self.conn_params)

    def create_monthly_partitions(self, start_date_str, num_months):
        """Creates monthly partitions for the specified range."""
        print(f"\n--- Creating {num_months} monthly partitions starting from {start_date_str} ---")
        start_date = date.fromisoformat(start_date_str)
        with self.conn.cursor() as cur:
            for i in range(num_months):
                target_date = start_date + relativedelta(months=i)
                try:
                    cur.execute("SELECT create_partition_and_indexes(%s);", (target_date,))
                    self.conn.commit()
                except psycopg2.Error as e:
                    print(f"Error creating partition for {target_date}: {e}")
                    self.conn.rollback()
                    break
        print("Partition creation process complete.")

    def migrate_data_to_partitioned(self, batch_size=50000):
        """Migrates data from monolithic table to partitioned table in batches."""
        print("\n--- Migrating data to partitioned table ---")
        with self.conn.cursor() as cur:
            
            # --- FIX: Clear the destination table first to prevent duplicate errors ---
            print("  Clearing destination table...")
            cur.execute("TRUNCATE TABLE viewing_events_partitioned;")
            self.conn.commit()

            cur.execute("SELECT count(*) FROM viewing_events;")
            result = cur.fetchone()
            if result is None or result[0] == 0:
                print("Source table is empty. No data to migrate.")
                return
            total_rows = result[0]

            migrated_rows = 0
            while migrated_rows < total_rows:
                query = f"""
                    INSERT INTO viewing_events_partitioned
                    SELECT * FROM viewing_events
                    ORDER BY event_id
                    OFFSET {migrated_rows}
                    LIMIT {batch_size};
                """
                cur.execute(query)
                self.conn.commit()
                
                rows_in_batch = cur.rowcount
                migrated_rows += rows_in_batch
                
                progress = (migrated_rows / total_rows) * 100
                print(f"  Migrated {migrated_rows}/{total_rows} rows ({progress:.2f}%)")
                
                if rows_in_batch == 0:
                    break

            # Verify data integrity after migration
            cur.execute("SELECT count(*) FROM viewing_events_partitioned;")
            partitioned_result = cur.fetchone()
            partitioned_count = partitioned_result[0] if partitioned_result else 0
            print(f"\nData integrity check: Monolithic count = {total_rows}, Partitioned count = {partitioned_count}")
            if total_rows == partitioned_count:
                print("Verification successful: Row counts match.")
            else:
                print("Verification failed: Row counts do not match.")

    def analyze_partition_performance(self):
        """Compares performance between monolithic and partitioned tables."""
        print("\n--- Analyzing performance: Monolithic vs. Partitioned ---")
        queries = {
            "daily_active_users (last 7 days)": """
                SELECT COUNT(DISTINCT user_id)
                FROM {table_name} WHERE event_timestamp >= NOW() - INTERVAL '7 days';
            """,
            "top_10_content (last 24 hours)": """
                SELECT content_id, COUNT(*) AS view_count
                FROM {table_name} WHERE event_timestamp >= NOW() - INTERVAL '24 hours' AND event_type = 'start'
                GROUP BY content_id ORDER BY view_count DESC LIMIT 10;
            """
        }
        report = {}
        for name, query_template in queries.items():
            print(f"\nBenchmarking query: '{name}'")
            timings = {}
            with self.conn.cursor() as cur:
                start_time = time.perf_counter()
                cur.execute(query_template.format(table_name='viewing_events'))
                cur.fetchall()
                end_time = time.perf_counter()
                timings['monolithic_ms'] = (end_time - start_time) * 1000
                print(f"  Monolithic table took: {timings['monolithic_ms']:.2f} ms")

            with self.conn.cursor() as cur:
                start_time = time.perf_counter()
                cur.execute(query_template.format(table_name='viewing_events_partitioned'))
                cur.fetchall()
                end_time = time.perf_counter()
                timings['partitioned_ms'] = (end_time - start_time) * 1000
                print(f"  Partitioned table took: {timings['partitioned_ms']:.2f} ms")
            
            report[name] = timings
        
        print("\n--- Checking for Partition Pruning ---")
        with self.conn.cursor() as cur:
            cur.execute("EXPLAIN SELECT COUNT(*) FROM viewing_events_partitioned WHERE event_timestamp >= NOW() - INTERVAL '1 day';")
            print("EXPLAIN plan for a query on the partitioned table:")
            for row in cur.fetchall():
                print(f"  {row[0]}")
        
        return report

    def close(self):
        self.conn.close()
        print("\nDatabase connection closed.")

if __name__ == '__main__':
    output_filename = '3_partition_solution_output.txt'
    original_stdout = sys.stdout

    with open(output_filename, 'w') as f:
        sys.stdout = f
        
        conn_params = {
            "host": "localhost",
            "dbname": "streamflix",
            "user": "student",
            "password": "student"
        }
        manager = StreamFlixPartitionManager(conn_params)
        
        print("This script will create partitions and migrate data.")

        three_months_ago = date.today() - relativedelta(months=2)
        manager.create_monthly_partitions(three_months_ago.strftime('%Y-%m-01'), 3)
        manager.migrate_data_to_partitioned()
        manager.close()
        
        print("\nPartitioning and migration complete.")

    sys.stdout = original_stdout
    print(f"Script finished. Output saved to '{output_filename}'")