import os
import re
import json
import PyPDF2
from datetime import datetime
from pathlib import Path

def extract_urls_from_pdf(pdf_path):
    """
    Extract URLs that follow 'Enlace de la noticia/Fuente de información:' in a PDF file.
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        List of extracted URLs
    """
    urls = []
    
    try:
        # Open the PDF file
        with open(pdf_path, 'rb') as file:
            # Create a PDF reader object
            reader = PyPDF2.PdfReader(file)
            
            # Get the text from all pages
            text = ""
            for page in reader.pages:
                text += page.extract_text()
            
            # Find all occurrences of the pattern and extract URLs
            pattern = r'Enlace de la noticia/Fuente de información:\s*(https?://[^\s,]+)'
            matches = re.findall(pattern, text)
            
            # Add the found URLs to the list
            urls.extend(matches)
            
            if not urls:
                # Try a more general URL pattern if the specific pattern doesn't find any URLs
                general_pattern = r'https?://[^\s,)"]+'
                general_matches = re.findall(general_pattern, text)
                urls.extend(general_matches)
                
    except Exception as e:
        print(f"Error processing {pdf_path}: {e}")
    
    return urls

def find_pdf_files(directory):
    """
    Recursively find all PDF files in the given directory.
    
    Args:
        directory: Directory to search for PDF files
        
    Returns:
        List of paths to PDF files
    """
    pdf_files = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    
    return pdf_files

def main():
    # Define the output directory
    output_dir = 'output'
    
    # Find all PDF files
    pdf_files = find_pdf_files(output_dir)
    print(f"Found {len(pdf_files)} PDF files.")
    
    # Dictionary to store file paths and their extracted URLs
    results = {}
    
    # Process each PDF file
    for pdf_file in pdf_files:
        print(f"Processing {pdf_file}...")
        urls = extract_urls_from_pdf(pdf_file)
        results[pdf_file] = urls
        print(f"  Found {len(urls)} URLs.")
    
    # Create a timestamp for the output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Write the results to a JSON file
    output_file = logs_dir / f"pdf_urls_{timestamp}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
    
    print(f"\nResults saved to {output_file}")
    
    # Create a CSV file with all URLs
    csv_file = Path(output_dir) / f"pdf_urls_{timestamp[:4]}.csv"
    with open(csv_file, 'w', encoding='utf-8') as f:
        f.write("PDF File,URL\n")
        for pdf_file, urls in results.items():
            for url in urls:
                f.write(f"{pdf_file},{url}\n")
    
    print(f"CSV file saved to {csv_file}")
    
    # Print total number of URLs found
    total_urls = sum(len(urls) for urls in results.values())
    print(f"\nTotal URLs found: {total_urls}")

if __name__ == "__main__":
    main()
