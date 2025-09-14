# gets.py (Final Version with Checksum Deduplication and Cleanup)

import hashlib
import json
import logging
import os
import random
import time
from datetime import datetime
import feedparser
import psycopg2
import psycopg2.extras
import requests

# --- Configuration ---
# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FEEDS_FILE = os.path.join(SCRIPT_DIR, 'feeds.json')
REQUEST_TIMEOUT = 10
DB_NAME = 'solene_db'
DB_USER = 'kabir'

# --- Advanced Headers to Impersonate Real Browsers ---
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/121.0',
]
BASE_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- NEW: Checksum Generation Function ---
def generate_checksum(article):
    """Generates a SHA256 checksum for an article's core content."""
    # Ensure consistent string representation, especially for datetime
    published_str = str(article.get('published_at') or '')
    summary_str = article.get('summary', '') or '' # Treat None summary as empty string
    
    # Concatenate core fields
    data_string = (
        article.get('title', '') +
        published_str +
        article.get('link', '') +
        summary_str
    )
    
    # Return the SHA256 hash of the UTF-8 encoded string
    return hashlib.sha256(data_string.encode('utf-8')).hexdigest()

# --- MODIFIED: Database Schema and Cleanup Functions ---
def manage_database_schema(conn):
    """Ensures the 'news' table has the correct schema, including the checksum column and constraint."""
    queries = [
        """
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT,
            link TEXT,
            summary TEXT,
            source TEXT,
            published_at TIMESTAMP,
            fetched_at TIMESTAMP,
            created_at TIMESTAMP,
            category TEXT
        );
        """,
        # Add the checksum column if it doesn't exist
        "ALTER TABLE news ADD COLUMN IF NOT EXISTS content_checksum TEXT;",
        # Drop the old unique link constraint if it exists, as checksum is now primary
        "ALTER TABLE news DROP CONSTRAINT IF EXISTS news_link_key;",
        # Add the new unique constraint on the checksum. This will fail if there are existing duplicates.
        # We will handle this by cleaning up duplicates first.
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'unique_content_checksum'
            ) THEN
                ALTER TABLE news ADD CONSTRAINT unique_content_checksum UNIQUE (content_checksum);
            END IF;
        END;
        $$;
        """
    ]
    with conn.cursor() as cur:
        for query in queries:
            try:
                cur.execute(query)
            except psycopg2.Error as e:
                # This might happen if we try to add a unique constraint on a table with duplicates.
                # We will fix this in the cleanup steps, so we can log it and continue.
                logging.warning(f"Schema management notice: {e}")
                conn.rollback() # Rollback the failed transaction
            else:
                conn.commit() # Commit successful DDL changes
    logging.info("Database schema is up to date.")


def run_deduplication_cleanup(conn):
    """
    Backfills checksums for old rows and deletes duplicates, keeping the most recent entry.
    This prepares the database for the unique constraint on content_checksum.
    """
    logging.info("Starting database cleanup and deduplication process...")
    
    # 1. Backfill checksums for any rows that are missing them
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT id, title, published_at, link, summary FROM news WHERE content_checksum IS NULL;")
        rows_to_update = cur.fetchall()
        
        if rows_to_update:
            logging.info(f"Found {len(rows_to_update)} rows missing checksums. Generating and backfilling...")
            for row in rows_to_update:
                article_data = dict(row)
                checksum = generate_checksum(article_data)
                cur.execute("UPDATE news SET content_checksum = %s WHERE id = %s;", (checksum, article_data['id']))
            conn.commit()
            logging.info("Checksum backfill complete.")

    # 2. Delete duplicate articles, keeping only the most recent one per checksum
    with conn.cursor() as cur:
        cleanup_query = """
        DELETE FROM news
        WHERE id IN (
            SELECT id FROM (
                SELECT id, ROW_NUMBER() OVER (PARTITION BY content_checksum ORDER BY created_at DESC) as rn
                FROM news
                WHERE content_checksum IS NOT NULL
            ) t
            WHERE t.rn > 1
        );
        """
        cur.execute(cleanup_query)
        deleted_count = cur.rowcount
        conn.commit()
        if deleted_count > 0:
            logging.info(f"Deleted {deleted_count} older duplicate articles.")
        else:
            logging.info("No duplicate articles found to delete.")

# --- MODIFIED: Insertion Logic ---
def insert_article(conn, article_data):
    """Inserts a single article, using content_checksum for deduplication."""
    insert_query = """
    INSERT INTO news (title, link, summary, source, published_at, fetched_at, created_at, category, content_checksum)
    VALUES (%(title)s, %(link)s, %(summary)s, %(source)s, %(published_at)s, %(fetched_at)s, %(created_at)s, %(category)s, %(content_checksum)s)
    ON CONFLICT (content_checksum) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, article_data)
        return True
    except psycopg2.Error as e:
        logging.error(f"Database insert error for link {article_data.get('link')}: {e}")
        conn.rollback()
        return False

# --- Main Application Logic (with cleanup integrated) ---
def main():
    logging.info("Starting feed fetching process...")

    # Connect to DB
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER)
        logging.info(f"Successfully connected to database '{DB_NAME}'.")
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to database. Error: {e}")
        return

    # Run schema management FIRST, then cleanup
    manage_database_schema(conn)
    run_deduplication_cleanup(conn)

    # Load feeds
    try:
        with open(FEEDS_FILE, 'r') as f:
            feed_categories = json.load(f)
        logging.info(f"Loaded {len(feed_categories)} categories from {FEEDS_FILE}.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Could not read or parse {FEEDS_FILE}: {e}")
        conn.close()
        return

    total_articles_added = 0
    now = datetime.now()

    for category, feeds_list in feed_categories.items():
        logging.info(f"--- Processing category: {category} ---")
        
        for feed_info in feeds_list:
            source_name = feed_info.get('source')
            feed_url = feed_info.get('url')

            if not all([source_name, feed_url]):
                continue

            logging.info(f"Fetching feed: {source_name}")

            try:
                request_headers = BASE_HEADERS.copy()
                request_headers['User-Agent'] = random.choice(USER_AGENTS)
                response = requests.get(feed_url, timeout=REQUEST_TIMEOUT, headers=request_headers)
                response.raise_for_status()
                feed_content = response.content
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch {feed_url} ({source_name}). Reason: {e}")
                continue

            feed_data = feedparser.parse(feed_content)

            if feed_data.bozo:
                logging.warning(f"Feed from {source_name} might be ill-formed. Error: {feed_data.bozo_exception}")

            articles_from_feed = 0
            for entry in feed_data.entries:
                title = entry.get('title')
                link = entry.get('link')

                if not all([title, link]):
                    continue

                published_dt = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    try:
                        published_dt = datetime.fromtimestamp(time.mktime(entry.published_parsed))
                    except (TypeError, ValueError):
                        pass
                
                article = {
                    'title': title,
                    'link': link,
                    'summary': entry.get('summary', ''),
                    'source': source_name,
                    'published_at': published_dt,
                    'fetched_at': now,
                    'created_at': now,
                    'category': category
                }
                
                # Generate and add the checksum before insertion
                article['content_checksum'] = generate_checksum(article)
                
                if insert_article(conn, article):
                    articles_from_feed += 1

            if articles_from_feed > 0:
                total_articles_added += articles_from_feed
                logging.info(f"Processed {articles_from_feed} new articles from {source_name}.")
                conn.commit()

    logging.info(f"Feed fetching process finished. Total new articles added: {total_articles_added}.")
    conn.close()

if __name__ == '__main__':
    main()