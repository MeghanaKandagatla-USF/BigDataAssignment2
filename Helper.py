import psycopg2
import time

def connect_db():
    """Connects to the PostgreSQL database."""
    return psycopg2.connect(
        host="localhost",
        dbname="streamflix",
        user="student",
        password="student"
    )

def test_solution():
    """Test harness for your optimization solution."""
    
    conn = connect_db()
    passed_tests = 0
    total_tests = 4
    
    print("--- Starting Final Solution Verification ---")
    
    with conn.cursor() as cur:
        # Test 1: Verify indexes exist and are used
        print("\n--- Test 1: Verifying index usage... ---")
        try:
            # Check if a key index exists
            cur.execute("""
                SELECT 1 FROM pg_indexes 
                WHERE tablename = 'viewing_events' AND indexname = 'idx_viewing_events_timestamp_userid';
            """)
            assert cur.fetchone() is not None, "Index 'idx_viewing_events_timestamp_userid' does not exist."
            
            # Check if the query planner uses an index
            cur.execute("EXPLAIN SELECT * FROM viewing_events WHERE event_timestamp > NOW() - INTERVAL '1 day';")
            plan = "".join([row[0] for row in cur.fetchall()])
            assert "Index Scan" in plan or "Bitmap Heap Scan" in plan, "Query plan is not using an index (found Seq Scan)."
            
            print("  [PASS] Indexes exist and are used by the query planner.")
            passed_tests += 1
        except AssertionError as e:
            print(f"  [FAIL] {e}")

        # Test 2: Verify partitions are created correctly
        print("\n--- Test 2: Verifying partition structure... ---")
        try:
            cur.execute("""
                SELECT count(*) FROM pg_inherits 
                WHERE inhparent = 'viewing_events_partitioned'::regclass;
            """)
            result = cur.fetchone()
            partition_count = result[0] if result is not None else 0
            assert partition_count > 0, "No child partitions found for 'viewing_events_partitioned'."
            print(f"  [PASS] Found {partition_count} partitions attached to the main table.")
            passed_tests += 1
        except AssertionError as e:
            print(f"  [FAIL] {e}")

        # Test 3: Verify performance improvements
        print("\n--- Test 3: Verifying query performance... ---")
        try:
            query = "SELECT COUNT(DISTINCT user_id) FROM {} WHERE event_timestamp >= NOW() - INTERVAL '7 days';"
            
            # Time query on original (but indexed) table
            start_time = time.perf_counter()
            cur.execute(query.format('viewing_events'))
            cur.fetchone()
            before_time = time.perf_counter() - start_time

            # Time query on new partitioned table
            start_time = time.perf_counter()
            cur.execute(query.format('viewing_events_partitioned'))
            cur.fetchone()
            after_time = time.perf_counter() - start_time
            
            speedup = before_time / after_time if after_time > 0 else float('inf')
            print(f"  - Indexed Table Time: {before_time*1000:.2f} ms")
            print(f"  - Partitioned Table Time: {after_time*1000:.2f} ms")
            print(f"  - Speedup Factor: {speedup:.2f}x")
            
            assert speedup > 10, f"Performance improvement is {speedup:.2f}x, which is less than the required >10x."
            print("  [PASS] Performance improvement achieves >10x speedup.")
            passed_tests += 1
        except AssertionError as e:
            print(f"  [FAIL] {e}")

        # Test 4: Verify data integrity
        print("\n--- Test 4: Verifying data integrity... ---")
        try:
            cur.execute("SELECT count(*) FROM viewing_events;")
            result = cur.fetchone()
            original_count = result[0] if result is not None else 0
            
            cur.execute("SELECT count(*) FROM viewing_events_partitioned;")
            result = cur.fetchone()
            partitioned_count = result[0] if result is not None else 0
            
            print(f"  - Original Table Count: {original_count}")
            print(f"  - Partitioned Table Count: {partitioned_count}")
            
            assert original_count == partitioned_count and original_count > 0, "Row counts do not match or are zero."
            print("  [PASS] Data integrity verified. Row counts match.")
            passed_tests += 1
        except AssertionError as e:
            print(f"  [FAIL] {e}")

    # Generate final score
    print("\n\n" + ("-" * 40))
    print("Optimization Score:")
    print(f"  PASSED {passed_tests} / {total_tests} TESTS")
    print("-" * 40)
    
    conn.close()


if __name__ == '__main__':
    test_solution()