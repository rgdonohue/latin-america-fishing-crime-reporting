#!/usr/bin/env python3
import csv
import requests
import time
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse
import os
import logging
from tqdm import tqdm
import random
import concurrent.futures
from functools import partial

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/citation_scraping.log'),
        logging.StreamHandler()
    ]
)

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Ensure output directory exists
os.makedirs('output', exist_ok=True)

# Output file for the scraped data
OUTPUT_FILE = 'output/citation_content.csv'

# User agent rotation to avoid blocking
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
]

# Configuration
MAX_WORKERS = 10  # Number of parallel workers
BATCH_SIZE = 50   # Process in batches and save results
VERIFY_SSL = False  # Skip SSL verification to handle problematic certificates

def get_random_user_agent():
    """Return a random user agent from the list."""
    return random.choice(USER_AGENTS)

def scrape_url(url, index=None, total=None):
    """
    Scrape the content from a URL.
    
    Args:
        url (str): The URL to scrape
        index (int, optional): Current index for logging
        total (int, optional): Total count for logging
        
    Returns:
        str: The scraped content as text, or an error message
    """
    if index is not None and total is not None:
        print(f"\nProcessing row {index+1} of {total}")
        logging.info(f"Processing URL {index+1}/{total}: {url}")
    
    try:
        headers = {
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Make the request with a timeout
        logging.info(f"Making request to {url}")
        response = requests.get(
            url, 
            headers=headers, 
            timeout=30, 
            allow_redirects=True,
            verify=VERIFY_SSL  # Skip SSL verification if needed
        )
        
        # Check if the request was successful
        if response.status_code == 200:
            logging.info(f"Successfully retrieved content from {url}")
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script and style elements
            for script in soup(['script', 'style']):
                script.extract()
                
            # Get text
            text = soup.get_text(separator=' ')
            
            # Clean the text a bit
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            domain = urlparse(response.url).netloc
            return f"[Source: {domain}] {text[:5000]}"  # Limiting text to 5000 chars to avoid huge CSV rows
        else:
            logging.warning(f"Failed to retrieve {url}: HTTP {response.status_code}")
            return f"Error: HTTP {response.status_code}"
    
    except requests.exceptions.Timeout:
        logging.error(f"Request to {url} timed out")
        return "Error: Request timed out"
    except requests.exceptions.TooManyRedirects:
        logging.error(f"Too many redirects for {url}")
        return "Error: Too many redirects"
    except requests.exceptions.SSLError as e:
        logging.error(f"SSL error for {url}: {str(e)}")
        return f"Error: SSL verification failed"
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception for {url}: {str(e)}")
        return f"Error: {str(e)}"
    except Exception as e:
        logging.error(f"Unexpected error for {url}: {str(e)}")
        return f"Unexpected error: {str(e)}"

def process_row(row, index, total):
    """Process a single row from the DataFrame."""
    pdf_path = row['pdf_path']
    url = row['url']
    
    # Scrape the URL
    content = scrape_url(url, index, total)
    
    # Add a random delay to avoid rate limiting (between 0.5 and 2 seconds)
    delay = random.uniform(0.5, 2)
    logging.info(f"Worker sleeping for {delay:.2f} seconds to avoid rate limiting")
    time.sleep(delay)
    
    return {
        'pdf_path': pdf_path,
        'url': url,
        'content': content
    }

def save_batch_results(results, batch_num):
    """Save a batch of results to a CSV file."""
    batch_df = pd.DataFrame(results)
    batch_file = f"output/citation_content_batch_{batch_num}.csv"
    
    try:
        batch_df.to_csv(batch_file, index=False, quoting=csv.QUOTE_ALL, escapechar='\\')
        logging.info(f"Batch {batch_num} saved to {batch_file}")
        return True
    except Exception as e:
        logging.error(f"Error saving batch {batch_num}: {e}")
        return False

def main():
    """Main function to read the CSV, scrape URLs, and save results."""
    
    # Read the input CSV file
    try:
        df = pd.read_csv('output/citation_urls.csv', header=None, names=['pdf_path', 'url'])
        logging.info(f"Loaded {len(df)} URLs from citation_urls.csv")
        
        # For testing, uncomment the next line to limit rows
        # df = df.head(20)
        
    except Exception as e:
        logging.error(f"Error reading input file: {e}")
        return
    
    # Create a list to store all results
    all_results = []
    
    # Process in batches
    total_rows = len(df)
    batch_count = 0
    
    for batch_start in range(0, total_rows, BATCH_SIZE):
        batch_count += 1
        batch_end = min(batch_start + BATCH_SIZE, total_rows)
        batch_df = df.iloc[batch_start:batch_end]
        
        logging.info(f"Processing batch {batch_count}: rows {batch_start+1} to {batch_end} (of {total_rows})")
        
        batch_results = []
        
        # Process the batch in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Create a partial function with fixed total parameter
            process_func = partial(process_row, total=total_rows)
            
            # Submit all tasks and process as they complete
            future_to_index = {
                executor.submit(process_func, row, batch_start + i): i 
                for i, row in enumerate(batch_df.to_dict('records'))
            }
            
            # Process results as they complete
            for future in tqdm(
                concurrent.futures.as_completed(future_to_index), 
                total=len(future_to_index),
                desc=f"Batch {batch_count}"
            ):
                try:
                    result = future.result()
                    batch_results.append(result)
                except Exception as e:
                    logging.error(f"Error processing task: {e}")
        
        # Save batch results
        save_batch_results(batch_results, batch_count)
        
        # Add to overall results
        all_results.extend(batch_results)
        
        logging.info(f"Completed batch {batch_count}. Processed {batch_end} of {total_rows} rows.")
    
    # Create a DataFrame from all results
    results_df = pd.DataFrame(all_results)
    
    # Save to final CSV
    try:
        results_df.to_csv(OUTPUT_FILE, index=False, quoting=csv.QUOTE_ALL, escapechar='\\')
        logging.info(f"All results saved to {OUTPUT_FILE}")
    except Exception as e:
        logging.error(f"Error saving final results: {e}")

if __name__ == "__main__":
    logging.info("Starting citation content scraping")
    main()
    logging.info("Citation content scraping completed")
