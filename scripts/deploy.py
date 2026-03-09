# scripts/deploy.py
import os
import subprocess
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from pathlib import Path


# ── 1. Detect how the workflow was triggered ────────────────
def get_trigger_mode():
    """
    Returns 'manual' if triggered via GitHub UI (workflow_dispatch)
    Returns 'push' if triggered by a git push
    """
    event = os.environ.get("GITHUB_EVENT_NAME", "push")
    print(f"ℹ️  Trigger mode: {event}")
    return event


# ── 2. Get SQL files based on trigger mode ──────────────────
def get_sql_files(mode: str):
    """
    PUSH mode:   Only return SQL files changed in the latest commit
    MANUAL mode: Return ALL SQL files in the snowflake/ folder
    """
    if mode == "workflow_dispatch":
        # Full re-deploy — find all .sql files recursively
        sql_files = sorted(
            [str(p).replace("\\", "/") for p in Path("snowflake").rglob("*.sql")]
        )
        print(f"📋 Full deploy mode — all SQL files found: {len(sql_files)}")
    else:
        # Incremental deploy — only changed files in latest push
        result = subprocess.run(
            ["git", "diff", "--name-only", "HEAD~1", "HEAD"],
            capture_output=True,
            text=True,
        )
        all_changed = result.stdout.strip().split("\n")
        sql_files = sorted(
            [
                f
                for f in all_changed
                if f.endswith(".sql") and f.startswith("snowflake/")
            ]
        )
        print(f"📋 Incremental deploy mode — changed SQL files: {len(sql_files)}")

    for f in sql_files:
        print(f"   → {f}")

    return sql_files


def get_snowflake_connection():
    """Creates a Snowflake connection using base64-encoded key-pair auth."""
    import base64

    # Decode the base64-encoded private key from GitHub Secrets
    # This avoids all Windows/Linux line ending issues entirely
    private_key_b64 = os.environ["SNOWFLAKE_PRIVATE_KEY"].strip()
    private_key_pem = base64.b64decode(private_key_b64)

    private_key = serialization.load_der_private_key(private_key_pem, password=None)

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        private_key=private_key_bytes,
    )

    print("✅ Connected to Snowflake successfully")
    return conn


# ── 4. Run a single SQL file ────────────────────────────────
def run_sql_file(conn, filepath: str):
    """Reads a SQL file and executes each statement inside it."""

    print(f"\n▶  Running: {filepath}")

    with open(filepath, "r") as f:
        sql_content = f.read()

    # Split on semicolons to get individual statements
    statements = [s.strip() for s in sql_content.split(";") if s.strip()]

    cursor = conn.cursor()
    for i, statement in enumerate(statements, 1):
        try:
            cursor.execute(statement)
            print(f"   ✅ Statement {i}/{len(statements)} executed")
        except Exception as e:
            print(f"   ❌ Statement {i}/{len(statements)} FAILED: {e}")
            raise
    cursor.close()
    print(f"   ✓ {filepath} deployed successfully")


# ── 5. Main orchestrator ────────────────────────────────────
def main():
    print("🚀 Starting Snowflake deployment\n")

    mode = get_trigger_mode()
    sql_files = get_sql_files(mode)

    if not sql_files:
        print("ℹ️  No SQL files to deploy — exiting")
        return

    conn = get_snowflake_connection()

    failed = []
    for filepath in sql_files:
        try:
            run_sql_file(conn, filepath)
        except Exception as e:
            failed.append(filepath)
            print(f"❌ FAILED: {filepath} — {e}")

    conn.close()

    print(f"\n{'─'*50}")
    print(
        f"Deployment complete: {len(sql_files) - len(failed)} succeeded, {len(failed)} failed"
    )

    if failed:
        print("\nFailed files:")
        for f in failed:
            print(f"  ✗ {f}")
        exit(1)
    else:
        print("✅ All files deployed successfully")


if __name__ == "__main__":
    main()
