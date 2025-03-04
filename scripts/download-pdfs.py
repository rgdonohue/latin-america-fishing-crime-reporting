import os
import csv
import argparse
import requests
from urllib.parse import urlparse
from pathlib import Path

def download_pdf(url, output_dir):
    """
    Download a PDF from the given URL and save it to the output directory.
    Returns the path to the downloaded file or None if download failed.
    """
    try:
        # Get the filename from the URL
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        
        # Create output path
        output_path = os.path.join(output_dir, filename)
        
        # Check if file already exists
        if os.path.exists(output_path):
            print(f"File already exists: {output_path}")
            return output_path
        
        # Download the file
        print(f"Downloading {url}...")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Save the file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Downloaded: {output_path}")
        return output_path
    
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def download_pdfs_from_csv(csv_file, output_dir):
    """
    Download PDFs from URLs in the given CSV file.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Count successful and failed downloads
    successful = 0
    failed = 0
    
    # Read the CSV file
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        # Skip header row if it exists
        next(reader, None)
        
        # Download each PDF
        for row in reader:
            if not row:
                continue
                
            url = row[0].strip()
            if url:
                result = download_pdf(url, output_dir)
                if result:
                    successful += 1
                else:
                    failed += 1
    
    # Print summary
    print(f"\nDownload Summary:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total: {successful + failed}")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Download PDFs from URLs listed in a CSV file.')
    parser.add_argument('csv_file', help='Path to the CSV file containing URLs')
    parser.add_argument('--output-dir', default='output/pdfs', 
                        help='Directory to save downloaded PDFs (default: output/pdfs)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Download PDFs
    download_pdfs_from_csv(args.csv_file, args.output_dir)

if __name__ == "__main__":
    main()
