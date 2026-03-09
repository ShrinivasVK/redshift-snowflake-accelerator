# scripts/deploy.py
# ─────────────────────────────────────────────────────────────
# This script:
#   1. Detects which SQL files changed in the latest Git push
#   2. Connects to Snowflake using key-pair authentication
#   3. Runs each changed SQL file in order
#   4. Logs success or failure for each file
# ─────────────────────────────────────────────────────────────

import os
import subprocess
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from pathlib import Path

# ── 1. Detect changed SQL files ────────────────────────────
def get_changed_sql_files():
    """Returns a sorted list of SQL files changed in the last commit."""
    result = subprocess.run(
        ["git", "diff", "--name-only", "HEAD~1", "HEAD"],
        capture_output=True, text=True
    )
    all_changed = result.stdout.strip().split("\n")
    
    # Only care about .sql files inside the snowflake/ folder
    sql_files = sorted([
        f for f in all_changed
        if f.endswith(".sql") and f.startswith("snowflake/")
    ])
    
    print(f"📋 Changed SQL files detected: {len(sql_files)}")
    for f in sql_files:
        print(f"   → {f}")
    
    return sql_files


# ── 2. Connect to Snowflake ─────────────────────────────────
def get_snowflake_connection():
    """Creates a Snowflake connection using key-pair auth (no password)."""
    
    # Load private key from temp file written by the workflow
    with open("/tmp/rsa_key.p8", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )
    
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        private_key=private_key_bytes
    )
    
    print("✅ Connected to Snowflake successfully")
    return conn


# ── 3. Run a single SQL file ────────────────────────────────
def run_sql_file(conn, filepath: str):
    """Reads a SQL file and executes each statement inside it."""
    
    print(f"\n▶  Running: {filepath}")
    
    with open(filepath, "r") as f:
        sql_content = f.read()
    
    # Split file into individual statements (separated by semicolons)
    statements = [s.strip() for s in sql_content.split(";") if s.strip()]
    
    cursor = conn.cursor()
    for i, statement in enumerate(statements, 1):
        try:
            cursor.execute(statement)
            print(f"   ✅ Statement {i}/{len(statements)} executed")
        except Exception as e:
            print(f"   ❌ Statement {i}/{len(statements)} FAILED: {e}")
            raise  # Stop deployment if any statement fails
    
    cursor.close()
    print(f"   ✓ {filepath} deployed successfully")


# ── 4. Main orchestrator ────────────────────────────────────
def main():
    print("🚀 Starting Snowflake deployment\n")
    
    sql_files = get_changed_sql_files()
    
    if not sql_files:
        print("ℹ️  No SQL files changed — nothing to deploy")
        return
    
    conn = get_snowflake_connection()
    
    failed = []
    for filepath in sql_files:
        try:
            run_sql_file(conn, filepath)
        except Exception as e:
            failed.append(filepath)
            print(f"❌ FAILED: {filepath}")
    
    conn.close()
    
    print(f"\n{'─'*50}")
    print(f"Deployment complete: {len(sql_files) - len(failed)} succeeded, {len(failed)} failed")
    
    if failed:
        print("\nFailed files:")
        for f in failed:
            print(f"  ✗ {f}")
        exit(1)  # Non-zero exit tells GitHub the deployment failed
    else:
        print("✅ All files deployed successfully")


if __name__ == "__main__":
    main()