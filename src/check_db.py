import sqlite3
from datetime import datetime
import json

def analyze_database():
    conn = sqlite3.connect('sam_slugs.db')
    cursor = conn.cursor()
    
    print("\n=== Database Analysis ===")
    
    # Check tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("\nTables in database:", [table[0] for table in tables])
    
    # Check slugs table
    try:
        cursor.execute('SELECT COUNT(*) FROM slugs')
        total_slugs = cursor.fetchone()[0]
        print(f"\nTotal entries in slugs table: {total_slugs:,}")
        
        cursor.execute('SELECT * FROM slugs ORDER BY created_at DESC LIMIT 5')
        recent_entries = cursor.fetchall()
        print("\nMost recent entries:")
        for entry in recent_entries:
            print(f"ID: {entry[0]}")
            print(f"Slug: {entry[1]}")
            print(f"Created: {entry[2]}")
            print("---")
    except sqlite3.OperationalError as e:
        print(f"Error accessing slugs table: {e}")
    
    # Check progress table
    try:
        cursor.execute('SELECT * FROM progress ORDER BY updated_at DESC LIMIT 5')
        progress_entries = cursor.fetchall()
        print("\nMost recent progress entries:")
        for entry in progress_entries:
            print(f"Last Date: {entry[0]}")
            print(f"Processed Count: {entry[1]:,}")
            print(f"Updated At: {entry[2]}")
            print("---")
    except sqlite3.OperationalError as e:
        print(f"Error accessing progress table: {e}")
    
    conn.close()

if __name__ == "__main__":
    analyze_database()