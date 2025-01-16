import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta
import sqlite3
import time
import json
import urllib.parse


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
        
        # Convert dates to the format seen in the API request
        date_from_formatted = f"{date_from}-05:00"
        date_to_formatted = f"{date_to}-05:00"
        
        params = {
            'random': int(time.time() * 1000),  # Timestamp in milliseconds
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

        # Log the exact request URL and parameters
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
                                    first_date = results[0].get('modifiedDate')
                                    last_date = results[-1].get('modifiedDate')
                                    logger.info(f"Results date range: {first_date} to {last_date}")
                                    logger.info(f"Number of results in this page: {len(results)}")
                                    logger.info(f"Sample result ID: {results[0].get('_id', 'No ID')}")
                            return data
                        else:
                            text = await response.text()
                            logger.error(f"Error {response.status} fetching page {page}: {text}")
                            logger.error(f"Failed URL: {url}")
                            if attempt < 2:
                                await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"Exception on page {page}: {str(e)}")
                    if attempt < 2:
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

        
        current_date = datetime.now()
        cutoff_date = datetime(2000, 1, 1) ## change this for testing

        total_processed = 0

        while current_date > cutoff_date:
            date_to = current_date.strftime("%Y-%m-%d")
            date_from = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
            
            logger.info(f"\n=== Processing date range: {date_from} to {date_to} ===")
            result = await self.process_time_chunk(date_from, date_to, db_path)
            
            if result == -1:  # Too many entries, split the day
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
                
                # Save progress
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute('INSERT OR REPLACE INTO progress (last_date, processed_count) VALUES (?, ?)',
                             (date_from, total_processed))
                conn.commit()
                conn.close()
            
            current_date = current_date - timedelta(days=1)
            await asyncio.sleep(1)  # Delay between days

        logger.info(f"\n=== Scraping complete! ===")
        logger.info(f"Total entries processed: {total_processed:,}")
        logger.info(f"Date range: {cutoff_date.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")

async def main():
    base_url = "https://sam.gov/api/prod/sgs/v1/search"
    db_path = "sam_slugs.db"
    
    async with SamScraper(base_url) as scraper:
        await scraper.scrape_all(db_path)

if __name__ == "__main__":
    asyncio.run(main())