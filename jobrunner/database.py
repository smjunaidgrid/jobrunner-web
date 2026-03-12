import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

DB_PATH = Path(".jobrunner/jobs.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        name TEXT,
        status TEXT,
        created_at TEXT,
        completed_at TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS steps (
        id TEXT PRIMARY KEY,
        job_id TEXT,
        name TEXT,
        command TEXT,
        status TEXT,
        retry_count INTEGER,
        max_retries INTEGER,
        started_at TEXT,
        completed_at TEXT
    )
    """)

    conn.commit()
    conn.close()


def create_job(name):
    conn = get_connection()
    cursor = conn.cursor()

    job_id = str(uuid.uuid4())

    cursor.execute(
        """
        INSERT INTO jobs (id, name, status, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (job_id, name, "pending", datetime.utcnow().isoformat()),
    )

    conn.commit()
    conn.close()

    return job_id


def create_steps(job_id, steps):
    conn = get_connection()
    cursor = conn.cursor()

    for step in steps:
        step_id = str(uuid.uuid4())

        cursor.execute(
            """
            INSERT INTO steps
            (id, job_id, name, command, status, retry_count, max_retries)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                step_id,
                job_id,
                step["name"],
                step["command"],
                "pending",
                0,
                step["retry"],
            ),
        )

    conn.commit()
    conn.close()

def get_job(job_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, name, status, created_at, completed_at FROM jobs WHERE id=?",
        (job_id,),
    )

    job = cursor.fetchone()

    conn.close()
    return job


def get_steps(job_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT name, status, retry_count, max_retries, started_at, completed_at
        FROM steps
        WHERE job_id=?
        ORDER BY rowid
        """,
        (job_id,),
    )

    steps = cursor.fetchall()

    conn.close()
    return steps

def list_jobs():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, name, status, created_at
        FROM jobs
        ORDER BY created_at DESC
        """
    )

    jobs = cursor.fetchall()

    conn.close()
    return jobs

def get_failed_steps(job_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id
        FROM steps
        WHERE job_id=? AND status='failed'
        """,
        (job_id,),
    )

    steps = cursor.fetchall()

    conn.close()
    return steps

def reset_failed_steps(job_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE steps
        SET status='pending',
            retry_count=0,
            started_at=NULL,
            completed_at=NULL
        WHERE job_id=? AND status='failed'
        """,
        (job_id,),
    )

    cursor.execute(
        """
        UPDATE jobs
        SET status='pending',
            completed_at=NULL
        WHERE id=?
        """,
        (job_id,),
    )

    conn.commit()
    conn.close()

    