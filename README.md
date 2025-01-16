# SAM.gov Entries Scraper

Collect all contract data from [SAM.gov](https://sam.gov/)

## Prerequisites
- Python **3.12+**
- **Supabase** account and project
- Required Python packages (see Installation)

---

## Installation

### Clone the repository:
```bash
git clone https://github.com/tommycolitsas/sam.git
cd sam
```

### Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Install dependencies:
```bash
pip install aiohttp python-dotenv supabase asyncio
```

### Create a `.env` file in the project root with your Supabase credentials:
```ini
SUPABASE_URL=your_supabase_project_url
SUPABASE_ANON_KEY=your_supabase_anon_key
```

### Set up the **Supabase table**:
Run this SQL query in your Supabase dashboard:
```sql
CREATE TABLE sam_data (
    id TEXT PRIMARY KEY,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::TEXT, now()) NOT NULL
);
```

---

## Usage

Run the script:
```bash
python main.py
```

The script will:
1. Start from the current date and work backwards to **`cuttoff_date`** (configurable)
2. Process opportunities **day by day**
3. Store basic data (IDs and slugs) in a **local SQLite database**
4. Store **full entry data** in **Supabase**
5. **Track progress** in the local database

---

## Data Storage

### Local SQLite (`sam_slugs.db`)
- **`slugs` table**: Stores entry IDs and their SAM.gov URLs
- **`progress` table**: Tracks scraping progress by date

### Supabase
- **`sam_data` table**: Stores full entry data as JSON (jsonb)

---

## Configuration

Key parameters in `main.py`:

| Parameter | Description | Default |
|-----------|------------|---------|
| `page_size` | Number of results per page | **100** |
| `max_concurrent_requests` | Rate limiting for API calls | **10** |
| `cutoff_date` | How far back to scrape | **Jan 01, 2000** |

---
