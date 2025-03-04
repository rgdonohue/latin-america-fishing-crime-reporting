# **Latin America Crime Reports Scraper**

This project scrapes crime reports from the Maritime Information Fusion Centre (Peru) to identify mentions of specific ships, plants, and topics of interest. The system extracts links from PDF reports, processes the content, and compiles findings into a structured spreadsheet.

## **Project Overview**

The Maritime Information Fusion Centre publishes weekly crime reports for several Latin American countries. These reports (52 per year) contain summaries of crimes with links to original news sources. This project aims to:

- Extract links from PDF reports
- Search through the linked content for mentions of specific ships, plants, and topics
- Compile findings into a structured spreadsheet

## **Methodology**

### **Data Collection**

1. **HTML Source Collection:**
   - The HTML source files in the source directory were manually copied from the webpage
   - This was done by loading the webpage for each year (2023, 2024, 2025) and using "load more" repeatedly to access all available links
   - The complete HTML for each year was saved to `source/source-{year}.html`

2. **PDF URL Extraction:**
   - `scripts/scrape-urls.py` extracts PDF URLs from the saved HTML files
   - URLs are saved to JSON format in the logs directory (e.g., `pdf_urls_20250303_111157.json`)
   - URLs can also be exported to CSV format in the output directory (e.g., `pdf_urls_2024.csv`)

3. **PDF Download:**
   - `scripts/download-pdfs.py` downloads the PDFs from the extracted URLs
   - PDFs are stored for further processing

4. **Citation URL Extraction:**
   - `scripts/scrape-citations.py` extracts citation URLs from the downloaded PDFs
   - The script processes each PDF, identifying and extracting all embedded links to crime reports
   - These citation URLs are saved to `output/citation_urls.csv` for further processing

5. **Web Content Scraping:**
   - The system scrapes the full HTML content from all 3,835 crime report web pages
   - Each page is downloaded and its content stored for analysis
   - This creates a comprehensive database of crime report content

### **Data Processing**

The processing pipeline includes:

1. **Content Analysis:**
   - `scripts/search-content-for-match.py` searches through all scraped content
   - The script identifies mentions of specific entities:
     - FMFO plants (by name)
     - Topics of interest
     - Vessels (by name, IMO, or registration number)
     - Vessel owners

2. **Entity Matching:**
   - Cleaning and normalizing entity names (removing "S.A.", "S.R.L.", etc.)
   - Fuzzy matching to account for spelling variations
   - Contextual analysis to reduce false positives

3. **Result Compilation:**
   - Matches are recorded along with the URL to the original crime report
   - Results are structured by entity type and country

### **Output Generation**

Results are compiled into a comprehensive Excel spreadsheet (`FMFO_Plants_and_Ships_in_Latin_America_Crime_Links_Updated.xlsx`) with tabs for:
- FMFO plants
- Topics
- Vessel Owners
- Vessels in Ecuador
- Vessels in Peru
- Vessels in Chile

Each entry includes links to relevant crime reports where the entity is mentioned, allowing for easy reference and verification.

## **Requirements**

- Python 3.8+
- Required packages (to be listed in requirements.txt)

## **Final Deliverable**

The final deliverable is the `FMFO_Plants_and_Ships_in_Latin_America_Crime_Links_Updated.xlsx` file, which contains:

1. Comprehensive listing of all matched entities
2. Direct links to crime reports mentioning each entity
3. Organized tabs by entity type and country
4. Metadata about the matches for further analysis

## **Notes**

- The crime blurbs in the PDFs are not structured, requiring careful text processing
- Entity names may need cleaning (removing corporate suffixes, etc.)
- For ships, searching by name, IMO, and National Registration Number is recommended
- The full scraping process involves handling approximately 3,835 web pages, requiring robust error handling and rate limiting

## **Next Steps**

Complete the PDF text extraction functionality

Implement entity matching against the provided list

Create the final spreadsheet output with all required tabs

Document the complete process and results

