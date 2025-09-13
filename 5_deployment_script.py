import psycopg2
import logging

# --- Basic Logger Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def deploy_optimizations(conn, dry_run=True):
    """
    Creates a safe deployment script for production.
    """
    
    logging.info(f"--- Starting optimization deployment. DRY RUN: {dry_run} ---")
    
    with conn.cursor() as cur:
        try:
            # 1. Check prerequisites (disk space, permissions)
            logging.info("Step 1: Checking prerequisites...")
            cur.execute("SELECT current_user, usesuper FROM pg_user WHERE usename = current_user;")
            user, is_superuser = cur.fetchone()
            if not is_superuser:
                logging.error(f"User '{user}' is not a superuser. Aborting.")
                return {'status': 'failed', 'reason': 'Insufficient permissions'}
            logging.info("  - User permissions check: PASSED")
            logging.info("  - Disk space check (simulated): PASSED")

            # 2. Create backup points (simulated)
            logging.info("Step 2: Creating database backup point (simulated)...")
            backup_command = "pg_dump -U student -d streamflix > streamflix_backup.sql"
            logging.info(f"  - Would run command: {backup_command}")
            if not dry_run:
                # In a real script, you would use subprocess to run this command.
                logging.info("  - (Skipping actual backup for this exercise)")
            
            # 3. Implement changes with minimal downtime
            logging.info("Step 3: Applying optimizations...")

            # Apply indexes concurrently to avoid locking the main table
            index_sql = "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_viewing_events_userid ON viewing_events (user_id);"
            logging.info("  - Applying index concurrently to monolithic table...")
            logging.info(f"    - EXECUTING: {index_sql}")
            if not dry_run:
                cur.execute(index_sql)
                conn.commit()
            
            # Run the hybrid strategy script
            logging.info("  - Creating hybrid partitioned table and supporting objects...")
            try:
                with open('5_optimisation_strategy.sql', 'r') as f:
                    hybrid_script = f.read()
                    logging.info("    - EXECUTING: 5_optimisation_strategy.sql")
                    if not dry_run:
                        cur.execute(hybrid_script)
                        conn.commit()
            except FileNotFoundError:
                logging.error("  - '5_optimisation_strategy.sql' not found. Aborting.")
                return {'status': 'failed', 'reason': 'Missing SQL script'}
                
            # 4. Verify each step before proceeding
            logging.info("Step 4: Verification...")
            cur.execute("SELECT to_regclass('public.viewing_events_hybrid');")
            if cur.fetchone()[0]:
                logging.info("  - Verified: 'viewing_events_hybrid' table exists.")
            else:
                raise ValueError("Verification failed: 'viewing_events_hybrid' not created.")

            cur.execute("SELECT to_regclass('public.daily_active_users_mv');")
            if cur.fetchone()[0]:
                logging.info("  - Verified: 'daily_active_users_mv' materialized view exists.")
            else:
                raise ValueError("Verification failed: Materialized view not created.")

            logging.info("--- Deployment script completed successfully! ---")
            return {'status': 'success', 'dry_run': dry_run}

        except Exception as e:
            # 5. Rollback capability if issues detected
            logging.error(f"An error occurred during deployment: {e}")
            logging.info("--- Rolling back changes ---")
            if not dry_run:
                conn.rollback() # Roll back the transaction
            return {'status': 'failed', 'reason': str(e)}

if __name__ == '__main__':
    
    conn = None
    try:
        conn = psycopg2.connect(
        host="localhost",
        database="streamflix",
        user="student",
        password="student"
    )
    
        # It's always best to run in dry_run mode first to see what will happen
        print("--- Running deployment script in DRY RUN mode ---")
        deploy_optimizations(conn, dry_run=True)
    
        # To actually apply the changes, you would uncomment the following lines:
        print("\n--- Running deployment script in EXECUTE mode ---")
        deploy_optimizations(conn, dry_run=False)
    
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
    finally:
        if conn:
            conn.close()