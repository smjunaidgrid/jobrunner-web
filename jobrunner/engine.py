import subprocess
import sqlite3
from pathlib import Path

DB_PATH = Path(".jobrunner/jobs.db")


def run_job(job_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, name, command, retry_count, max_retries
        FROM steps
        WHERE job_id=?
        ORDER BY rowid
        """,
        (job_id,),
    )

    steps = cursor.fetchall()

    for step_id, name, command, retry_count, max_retries in steps:

        attempt = retry_count

        while attempt <= max_retries:

            print(f"Running step: {name} (attempt {attempt + 1})")

            cursor.execute(
                "UPDATE steps SET status=?, started_at=datetime('now') WHERE id=?",
                ("running", step_id),
            )
            conn.commit()

            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
            )

            log_dir = Path(f".jobrunner/logs/{job_id}")
            log_dir.mkdir(parents=True, exist_ok=True)

            log_file = log_dir / f"{name}_attempt{attempt+1}.log"

            with open(log_file, "w") as f:
                f.write(result.stdout)
                f.write(result.stderr)

            if result.returncode == 0:
                cursor.execute(
                    """
                    UPDATE steps
                    SET status='success',
                        completed_at=datetime('now'),
                        retry_count=?
                    WHERE id=?
                    """,
                    (attempt, step_id),
                )
                conn.commit()
                break

            attempt += 1

            cursor.execute(
                "UPDATE steps SET retry_count=? WHERE id=?",
                (attempt, step_id),
            )
            conn.commit()

            if attempt > max_retries:
                print(f"Step failed after retries: {name}")

                cursor.execute(
                    """
                    UPDATE steps
                    SET status='failed',
                        completed_at=datetime('now')
                    WHERE id=?
                    """,
                    (step_id,),
                )

                cursor.execute(
                    """
                    UPDATE jobs
                    SET status='failed',
                        completed_at=datetime('now')
                    WHERE id=?
                    """,
                    (job_id,),
                )

                conn.commit()
                conn.close()
                return

    cursor.execute(
        """
        UPDATE jobs
        SET status='success',
            completed_at=datetime('now')
        WHERE id=?
        """,
        (job_id,),
    )

    conn.commit()
    conn.close()

    print("Job completed successfully")