import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta
import sqlite3
import time
import json
import urllib.parse
import os
from dotenv import load_dotenv

load_dotenv()

from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

class SamScraper:
    def __init__(self, base_url: str, page_size: int = 100, max_concurrent_requests: int = 10):
        self.base_url = base_url
        self.page_size = page_size
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.session = None
        self.failed_requests = []
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def construct_slug(self, entry_id: str) -> str:
        return f"https://sam.gov/opp/{entry_id}/view"

    async def fetch_page(self, page: int, date_from: str, date_to: str, cursor=None) -> dict:
        """Fetch single page with date range"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Origin': 'https://sam.gov',
            'Referer': 'https://sam.gov/search',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin'
        }
        
        date_from_formatted = f"{date_from}-05:00"
        date_to_formatted = f"{date_to}-05:00"
        
        params = {
            'random': int(time.time() * 1000), 
            'index': 'opp',
            'page': page,
            'sort': '-modifiedDate',
            'size': self.page_size,
            'mode': 'search',
            'responseType': 'json',
            'q': '',
            'qMode': 'ALL',
            'modified_date.to': date_to_formatted,
            'modified_date.from': date_from_formatted
        }

        url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
        logger.info(f"Requesting URL: {url}")

        async with self.semaphore:
            for attempt in range(3):
                try:
                    async with self.session.get(self.base_url, params=params, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            if '_embedded' in data and 'results' in data['_embedded']:
                                results = data['_embedded']['results']
                                if results:
                                    logger.info(f"Page {page}: Retrieved {len(results)} results")
                            return data
                        else:
                            logger.error(f"Error {response.status} fetching page {page}")
                            if attempt < 2:
                                await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Failed request: date_from={date_from}, date_to={date_to}, page={page}, error={error_msg}")
                    
                    if cursor: 
                        try:
                            cursor.execute('''
                                INSERT INTO failed_requests (date_from, date_to, page, error)
                                VALUES (?, ?, ?, ?)
                            ''', (date_from, date_to, page, error_msg))
                            cursor.connection.commit()  
                        except Exception as db_error:
                            logger.error(f"Failed to log error to database: {str(db_error)}")

                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
            return None
    
    async def retry_failed_requests(self, db_path: str): 
        """Retry failed requests specifically"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT date_from, date_to, page 
                FROM failed_requests 
                WHERE retry_count < 3 
                ORDER BY created_at
            ''')
            
            failed_requests = cursor.fetchall()
            for date_from, date_to, page in failed_requests:
                try:
                    result = await self.fetch_page(page, date_from, date_to, cursor)
                    if result:
                        self._process_and_insert_results([result], cursor)
                        cursor.execute('''
                            DELETE FROM failed_requests 
                            WHERE date_from = ? AND date_to = ? AND page = ?
                        ''', (date_from, date_to, page))
                    else:
                        cursor.execute('''
                            UPDATE failed_requests 
                            SET retry_count = retry_count + 1,
                                last_retry = CURRENT_TIMESTAMP
                            WHERE date_from = ? AND date_to = ? AND page = ?
                        ''', (date_from, date_to, page))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Retry failed for {date_from} to {date_to}, page {page}: {str(e)}")
        finally:
            conn.close()

    async def process_time_chunk(self, date_from: str, date_to: str, db_path: str) -> int:
        """Process a specific time chunk"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            data = await self.fetch_page(0, date_from, date_to, cursor)
            if not data or '_embedded' not in data:
                return 0

            total = data['page']['totalElements']
            logger.info(f"Found {total} entries between {date_from} and {date_to}")

            if total > 10000:
                return -1

            pages = (total + self.page_size - 1) // self.page_size
            tasks = []
            processed = 0

            for page in range(pages):
                tasks.append(self.fetch_page(page, date_from, date_to, cursor))
               
                if len(tasks) >= 10:
                    results = await asyncio.gather(*tasks)
                    processed += self._process_and_insert_results(results, cursor)
                    conn.commit()
                    tasks = []
                    logger.info(f"Processed {processed}/{total} entries for {date_from} to {date_to}")
                    await asyncio.sleep(1)

            if tasks:
                results = await asyncio.gather(*tasks)
                processed += self._process_and_insert_results(results, cursor)
                conn.commit()

            return processed
        finally:
            conn.close()

    def _process_and_insert_results(self, fetch_results: list, cursor) -> int:
        batch_processed = 0
        existing_ids = set()

        ids_to_check = [
            item['_id'] 
            for result in fetch_results 
            if result and '_embedded' in result and 'results' in result['_embedded']
            for item in result['_embedded']['results']
        ]
        
        cursor.execute('SELECT id FROM slugs WHERE id IN ({})'.format(
            ','.join('?' * len(ids_to_check))), ids_to_check)
        existing_ids = {row[0] for row in cursor.fetchall()}

        new_records = []
        update_records = []
        sqlite_inserts = []

        for result in fetch_results:
            if result and '_embedded' in result and 'results' in result['_embedded']:
                for item in result['_embedded']['results']:
                    try:
                        json_obj = json.loads(json.dumps(item))  # validate JSON
                        record = {"id": item["_id"], "metadata": json_obj}
                        
                        if item['_id'] in existing_ids:
                            update_records.append(record)
                        else:
                            new_records.append(record)
                            sqlite_inserts.append((item['_id'], self.construct_slug(item['_id'])))
                        
                        batch_processed += 1
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON parsing error for item {item['_id']}: {str(e)}")
                        continue

        if sqlite_inserts:
            cursor.executemany('INSERT OR IGNORE INTO slugs (id, slug) VALUES (?, ?)', 
                              sqlite_inserts)

        BATCH_SIZE = 100  

        # handle new records
        for i in range(0, len(new_records), BATCH_SIZE):
            batch = new_records[i:i + BATCH_SIZE]
            try:
                supabase.table("sam_data").insert(batch).execute()
                logger.info(f"Inserted batch of {len(batch)} new records")
            except Exception as ex:
                logger.error(f"Batch insert failed: {str(ex)}")
                # individual fallback with insert instead of upsert
                for record in batch:
                    try:
                        supabase.table("sam_data").insert(record).execute()
                    except Exception as row_ex:
                        logger.error(f"Failed to insert {record['id']}: {str(row_ex)}")

        # handle updates
        for i in range(0, len(update_records), BATCH_SIZE):
            batch = update_records[i:i + BATCH_SIZE]
            try:
                supabase.table("sam_data").upsert(batch, on_conflict="id").execute()
                logger.info(f"Updated batch of {len(batch)} records")
            except Exception as ex:
                logger.error(f"Batch update failed: {str(ex)}")
                for record in batch:
                    try:
                        supabase.table("sam_data").upsert(record, on_conflict="id").execute()
                    except Exception as row_ex:
                        logger.error(f"Failed to update {record['id']}: {str(row_ex)}")

        return batch_processed

    async def scrape_all(self, db_path: str):
        """Main scraping method using time chunks"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS slugs (
                id TEXT PRIMARY KEY,
                slug TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS progress (
                last_date TEXT,
                processed_count INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS failed_requests (
                date_from TEXT,
                date_to TEXT,
                page INTEGER,
                error TEXT,
                retry_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_retry TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()

        current_date = datetime.now()
        cutoff_date = datetime(2000, 1, 1)  # adjust for testing
        total_processed = 0

        while current_date > cutoff_date:
            date_to = current_date.strftime("%Y-%m-%d")
            date_from = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")

            logger.info(f"\n=== Processing date range: {date_from} to {date_to} ===")
            result = await self.process_time_chunk(date_from, date_to, db_path)

            if result == -1:  # too many entries split the day
                mid_date = current_date - timedelta(hours=12)
                mid_str = mid_date.strftime("%Y-%m-%d %H:%M:%S")

                logger.info(f"Splitting day into two chunks:")
                logger.info(f"1st half: {date_from} to {mid_str}")
                first_half = await self.process_time_chunk(date_from, mid_str, db_path)

                logger.info(f"2nd half: {mid_str} to {date_to}")
                second_half = await self.process_time_chunk(mid_str, date_to, db_path)

                result = (first_half if first_half > 0 else 0) + (second_half if second_half > 0 else 0)

            if result > 0:
                total_processed += result
                logger.info(f"Day complete - Processed {result:,} entries")
                logger.info(f"Running total: {total_processed:,} entries\n")

                # save progress locally
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute('INSERT OR REPLACE INTO progress (last_date, processed_count) VALUES (?, ?)',
                               (date_from, total_processed))
                conn.commit()
                conn.close()

            current_date = current_date - timedelta(days=1)
            await asyncio.sleep(1) 

        logger.info(f"\n=== Scraping complete! ===")
        logger.info(f"Total entries processed: {total_processed:,}")
        logger.info(f"Date range: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")

async def main():
    base_url = "https://sam.gov/api/prod/sgs/v1/search"
    db_path = "sam_slugs.db"

    async with SamScraper(base_url) as scraper:
        try:
            await scraper.scrape_all(db_path)
        finally:
            # retry failed requests before exiting
            await scraper.retry_failed_requests(db_path)

        # failure report
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM failed_requests')
        failed_count = cursor.fetchone()[0]
        
        if failed_count > 0:
            logger.warning(f"\nThere are {failed_count} failed requests remaining")
            logger.warning("Run the retry_failed_requests() method separately to attempt recovery")

if __name__ == "__main__":
    asyncio.run(main())