import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta
import sqlite3
import time
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SamScraper:
    def __init__(self, base_url: str, page_size: int = 100, max_concurrent_requests: int = 10):
        self.base_url = base_url
        self.page_size = page_size
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def construct_slug(self, entry_id: str) -> str:
        return f"https://sam.gov/opp/{entry_id}/view"

    async def fetch_page(self, page: int, date_from: str, date_to: str) -> dict:
        """Fetch a single page with date range"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'application/hal+json',  # Changed from application/json
            'Origin': 'https://sam.gov',
            'Referer': 'https://sam.gov/search',
            'Content-Type': 'application/json'
        }
        
        params = {
            'page': page,
            'size': self.page_size,
            'sort': '-modifiedDate',
            'dateRangeFrom': date_from,
            'dateRangeTo': date_to,
            'random': str(int(time.time() * 1000)),
            'index': '_all',  # Added index parameter
            'sfm[status][is_active]': 'true',  # Added status filters
            'sfm[status][is_inactive]': 'true'
        }

        async with self.semaphore:
            for attempt in range(3):
                try:
                    async with self.session.get(self.base_url, params=params, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:  # Rate limit
                            wait_time = int(response.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limited, waiting {wait_time} seconds")
                            await asyncio.sleep(wait_time)
                        else:
                            text = await response.text()
                            logger.error(f"Error {response.status} fetching page {page}: {text}")
                            await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"Exception on page {page}: {str(e)}")
                    await asyncio.sleep(2 ** attempt)
            return None

    async def process_time_chunk(self, date_from: str, date_to: str, db_path: str) -> int:
        """Process a specific time chunk"""
        # Get total for this time range
        data = await self.fetch_page(0, date_from, date_to)
        if not data or '_embedded' not in data:
            return 0

        total = data['page']['totalElements']
        logger.info(f"Found {total} entries between {date_from} and {date_to}")

        # If too many entries, need to split
        if total > 10000:
            return -1

        # Process all pages for this chunk
        pages = (total + self.page_size - 1) // self.page_size
        tasks = []
        processed = 0

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for page in range(pages):
            tasks.append(self.fetch_page(page, date_from, date_to))
            if len(tasks) >= 10:  # Process in smaller batches
                results = await asyncio.gather(*tasks)
                for result in results:
                    if result and '_embedded' in result and 'results' in result['_embedded']:
                        entries = [
                            (item['_id'], self.construct_slug(item['_id'])) 
                            for item in result['_embedded']['results']
                        ]
                        cursor.executemany('INSERT OR IGNORE INTO slugs (id, slug) VALUES (?, ?)', entries)
                        processed += len(entries)
                
                conn.commit()
                tasks = []
                logger.info(f"Processed {processed}/{total} entries for {date_from} to {date_to}")
                await asyncio.sleep(1)  # Small delay between batches

        # Process remaining
        if tasks:
            results = await asyncio.gather(*tasks)
            for result in results:
                if result and '_embedded' in result and 'results' in result['_embedded']:
                    entries = [
                        (item['_id'], self.construct_slug(item['_id'])) 
                        for item in result['_embedded']['results']
                    ]
                    cursor.executemany('INSERT OR IGNORE INTO slugs (id, slug) VALUES (?, ?)', entries)
                    processed += len(entries)
            conn.commit()

        conn.close()
        return processed

    async def scrape_all(self, db_path: str):
        """Main scraping method using time chunks"""
        # Initialize database
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
        conn.commit()
        conn.close()

        # Start from today and work backwards
        current_date = datetime.now()
        total_processed = 0

        while True:
            date_to = current_date.strftime("%Y-%m-%d")
            date_from = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
            
            result = await self.process_time_chunk(date_from, date_to, db_path)
            
            if result == -1:  # Too many entries, split the day
                mid_date = current_date - timedelta(hours=12)
                mid_str = mid_date.strftime("%Y-%m-%d %H:%M:%S")
                
                # Process each half-day
                first_half = await self.process_time_chunk(date_from, mid_str, db_path)
                second_half = await self.process_time_chunk(mid_str, date_to, db_path)
                
                result = (first_half if first_half > 0 else 0) + (second_half if second_half > 0 else 0)
            
            if result > 0:
                total_processed += result
                logger.info(f"Total entries processed: {total_processed}")
                
                # Save progress
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute('INSERT OR REPLACE INTO progress (last_date, processed_count) VALUES (?, ?)',
                             (date_from, total_processed))
                conn.commit()
                conn.close()
            
            current_date = current_date - timedelta(days=1)
            await asyncio.sleep(1)  # Delay between days

async def main():
    base_url = "https://sam.gov/api/prod/sgs/v1/search"
    db_path = "sam_slugs.db"
    
    async with SamScraper(base_url) as scraper:
        await scraper.scrape_all(db_path)

if __name__ == "__main__":
    asyncio.run(main())