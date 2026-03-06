import os
from datetime import datetime
from typing import Optional

import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from contextlib import contextmanager

from app.models import JobDetail

_pool = MySQLConnectionPool(
    pool_name="scraper_pool",
    pool_size=5,
    host=os.getenv("DB_HOST", "mysql"),
    port=int(os.getenv("DB_PORT", 3306)),
    database=os.getenv("DB_NAME", "job_search"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)


@contextmanager
def get_connection():
    conn = _pool.get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_cursor(dictionary: bool = True):
    with get_connection() as conn:
        cursor = conn.cursor(dictionary=dictionary)
        try:
            yield cursor
        finally:
            cursor.close()


def update_job_detail(url_hash: str, detail: JobDetail) -> None:
    """
    Update an existing job row with full detail from Pass 2.
    - 'scraped'  if description was successfully retrieved
    - 'failed'   if Pass 2 completed but returned no description
    Only called for jobs written in Pass 1 with scrape_status = 'pending'.
    """
    desc = detail.description
    scrape_status = "scraped" if desc and desc.strip() else "failed"

    with get_cursor() as cursor:
        cursor.execute(
            """UPDATE jobs SET
                location      = COALESCE(%s, location),
                job_type      = COALESCE(%s, job_type),
                salary        = COALESCE(%s, salary),
                description   = %s,
                scrape_status = %s,
                scraped_at    = %s
               WHERE url_hash = %s""",
            (
                detail.location,
                detail.job_type,
                detail.salary,
                desc,
                scrape_status,
                datetime.utcnow(),
                url_hash,
            ),
        )