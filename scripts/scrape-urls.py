import os
import csv
import argparse
from bs4 import BeautifulSoup

def extract_pdf_urls(html_file):
    """
    Extract Spanish PDF URLs from the HTML file that start with 
    'https://www.dicapi.mil.pe/storage/ifc-documents/' and exclude English versions
    """
    # Read HTML file
    with open(html_file, 'r', encoding='utf-8') as file:
        html_content = file.read()
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all <a> tags with href attributes
    links = soup.find_all('a', href=True)
    
    # Filter links that point to Spanish PDF documents
    pdf_urls = []
    base_url = 'https://www.dicapi.mil.pe/storage/ifc-documents/'
    
    for link in links:
        href = link.get('href')
        # Only include PDFs that are not in the /en/ directory (Spanish versions only)
        if href and href.startswith(base_url) and href.endswith('.pdf') and "/en/" not in href:
            pdf_urls.append(href)
    
    return pdf_urls

def save_to_csv(urls, output_file):
    """Save URLs to a CSV file"""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["URL"])  # Header
        for url in urls:
            writer.writerow([url])
    
    print(f"Extracted {len(urls)} Spanish PDF URLs and saved to {output_file}")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Extract PDF URLs from an HTML file.')
    parser.add_argument('html_file', help='Path to the HTML file to scrape')
    parser.add_argument('--output', '-o', default='output/pdf_urls.csv',
                        help='Path to the output CSV file (default: output/pdf_urls.csv)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Extract PDF URLs
    pdf_urls = extract_pdf_urls(args.html_file)
    
    # Save to CSV
    save_to_csv(pdf_urls, args.output)

if __name__ == "__main__":
    main()
