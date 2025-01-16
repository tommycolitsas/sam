import aiohttp
import asyncio
from typing import List, Dict
import logging
import json
from datetime import datetime
import sqlite3
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SamEntry:
    id: str
    slug: str
    publish_date: str
    title: str
    type: str
    solicitation_number: str

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

    async def fetch_page(self, page: int) -> Dict:
        """Fetch a single page of results with error handling and retries"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"macOS"',
            'Origin': 'https://sam.gov',
            'Referer': 'https://sam.gov/search'
        }
        
        params = {
            'random': str(int(time.time() * 1000)),
            'index': '_all',
            'page': page,
            'mode': 'search',
            'sort': '-modifiedDate',
            'size': self.page_size,
            'mfe': 'true',
            'q': '',
            'qMode': 'ALL'
        }
        # Add a small delay between requests
        await asyncio.sleep(1)
        
        async with self.semaphore:
            for attempt in range(3):  # Retry up to 3 times
                try:
                    async with self.session.get(self.base_url, params=params, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:  # Rate limit
                            wait_time = int(response.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limited, waiting {wait_time} seconds")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"Error fetching page {page}: Status {response.status}")
                            logger.error(f"Response text: {await response.text()}")
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                except Exception as e:
                    logger.error(f"Exception fetching page {page}: {str(e)}")
                    await asyncio.sleep(2 ** attempt)
            return None

    def process_entries(self, data: Dict) -> List[SamEntry]:
        """Process raw JSON data into SamEntry objects"""
        entries = []
        for result in data["_embedded"]["results"]:
            entry = SamEntry(
                id=result["_id"],
                slug=self.construct_slug(result["_id"]),
                publish_date=result["publishDate"],
                title=result["title"],
                type=result["type"]["value"],
                solicitation_number=result.get("solicitationNumber", "")  # Use get() with default empty string
            )
            entries.append(entry)
        return entries

    async def scrape_all(self, db_path: str):
        """Main method to scrape all pages and save to database"""
        # First page to get total count
        data = await self.fetch_page(0)
        if not data:
            logger.error("Failed to fetch first page")
            return

        total_entries = data["page"]["totalElements"]
        total_pages = (total_entries + self.page_size - 1) // self.page_size
        logger.info(f"Found {total_entries} total entries across {total_pages} pages")

        # Initialize database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sam_entries (
                id TEXT PRIMARY KEY,
                slug TEXT,
                publish_date TEXT,
                title TEXT,
                type TEXT,
                solicitation_number TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

        entries_processed = 0
        
        # Process pages concurrently
        tasks = []
        for page in range(total_pages):
            tasks.append(self.fetch_page(page))
            if len(tasks) >= 50:  # Process in batches of 50 pages
                results = await asyncio.gather(*tasks)
                for result in results:
                    if result:
                        entries = self.process_entries(result)
                        entries_processed += len(entries)
                        # Batch insert entries
                        cursor.executemany(
                            'INSERT OR IGNORE INTO sam_entries (id, slug, publish_date, title, type, solicitation_number) VALUES (?, ?, ?, ?, ?, ?)',
                            [(e.id, e.slug, e.publish_date, e.title, e.type, e.solicitation_number) for e in entries]
                        )
                        conn.commit()
                tasks = []
                logger.info(f"Processed page {page}/{total_pages} - Total entries: {entries_processed:,}/{total_entries:,} ({(entries_processed/total_entries)*100:.2f}%)")

        # Process any remaining tasks
        if tasks:
            results = await asyncio.gather(*tasks)
            for result in results:
                if result:
                    entries = self.process_entries(result)
                    cursor.executemany(
                        'INSERT OR IGNORE INTO sam_entries (id, slug, publish_date, title, type, solicitation_number) VALUES (?, ?, ?, ?, ?, ?)',
                        [(e.id, e.slug, e.publish_date, e.title, e.type, e.solicitation_number) for e in entries]
                    )
                    conn.commit()

        conn.close()
        logger.info("Completed scraping all pages")

async def main():
    base_url = "https://sam.gov/api/prod/sgs/v1/search"
    db_path = "sam_entries.db"
    
    async with SamScraper(base_url, page_size=100) as scraper:
        await scraper.scrape_all(db_path)

if __name__ == "__main__":
    asyncio.run(main())