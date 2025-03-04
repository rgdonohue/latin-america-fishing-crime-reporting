import pandas as pd
import re
import os

def clean_company_name(name):
    """
    Clean company name by removing legal entity suffixes and standardizing format.
    
    Args:
        name (str): Company name to clean
        
    Returns:
        str: Cleaned company name
    """
    if pd.isna(name) or not name:
        return ""
    
    # Convert to string and lowercase
    name = str(name).lower().strip()
    
    # Remove common legal entity suffixes
    suffixes = [
        r'\bs\.a\.$', r'\bs\.a\b', 
        r'\bs\.a\.c\.$', r'\bs\.a\.c\b',
        r'\bs\.a\. de c\.v\.$', r'\bs\.a\. de c\.v\b',
        r'\bs\.r\.l\.$', r'\bs\.r\.l\b',
        r'\bltda\.$', r'\bltda\b',
        r'\bs\.a\.p\.i\. de c\.v\.$', r'\bs\.a\.p\.i\. de c\.v\b',
        r'\bcia\. ltda\.$', r'\bcia\. ltda\b',
        r'\bs\. de r\.l\. de c\.v\.$', r'\bs\. de r\.l\. de c\.v\b',
        r'\bsa de cv$', r'\bsa de cv\b'
    ]
    
    for suffix in suffixes:
        name = re.sub(suffix, '', name)
    
    # Remove parentheses and their contents
    name = re.sub(r'\([^)]*\)', '', name)
    
    # Remove quotes
    name = name.replace('"', '').replace("'", '')
    
    # Remove leading/trailing whitespace and commas
    name = name.strip(', ')
    
    return name

def search_plants_in_content(plants_df, citation_content_df):
    """
    Search for plant names in citation content and add matching URLs to the plants dataframe.
    
    Args:
        plants_df (DataFrame): DataFrame containing plant names to search for
        citation_content_df (DataFrame): DataFrame containing citation content to search in
        
    Returns:
        DataFrame: Updated plants DataFrame with matching URLs
    """
    # Make a copy of the plants DataFrame to avoid modifying the original
    updated_plants_df = plants_df.copy()
    
    # Initialize the 'Crime Report Links' column with empty strings if it doesn't exist
    if 'Crime Report Links' not in updated_plants_df.columns:
        updated_plants_df['Crime Report Links'] = ''
    
    # For each plant, search in the citation content
    for idx, row in updated_plants_df.iterrows():
        company_name = clean_company_name(row['Company name'])
        matching_urls = []
        
        # Skip empty company names
        if not company_name:
            continue
        
        # Search for the company name in each citation content
        for _, citation_row in citation_content_df.iterrows():
            content = str(citation_row['content']).lower()
            url = citation_row['url']
            
            # Check if the company name is in the content (as a whole word)
            if re.search(r'\b' + re.escape(company_name) + r'\b', content):
                matching_urls.append(url)
        
        # Update the 'Crime Report Links' column with the matching URLs
        if matching_urls:
            # If the cell already has content, append to it
            current_links = str(row['Crime Report Links']) if not pd.isna(row['Crime Report Links']) else ''
            if current_links and current_links.lower() != 'nan':
                # Check if any of the new URLs are already in the current links
                new_urls = [url for url in matching_urls if url not in current_links]
                if new_urls:
                    updated_plants_df.at[idx, 'Crime Report Links'] = current_links + ', ' + ', '.join(new_urls)
            else:
                updated_plants_df.at[idx, 'Crime Report Links'] = ', '.join(matching_urls)
    
    return updated_plants_df

def main():
    # Define file paths
    plants_file = 'data-output/FMFO Plants in Latin America.csv'
    citation_content_file = 'data-output/citation_content_all_batches.csv'
    output_file = 'data-output/FMFO Plants in Latin America - Updated.csv'
    
    # Check if files exist
    if not os.path.exists(plants_file):
        print(f"Error: Plants file not found at {plants_file}")
        return
    
    if not os.path.exists(citation_content_file):
        print(f"Error: Citation content file not found at {citation_content_file}")
        return
    
    # Load data
    print(f"Loading plants data from {plants_file}...")
    plants_df = pd.read_csv(plants_file)
    
    print(f"Loading citation content from {citation_content_file}...")
    citation_content_df = pd.read_csv(citation_content_file)
    
    # Search for plant names in citation content
    print("Searching for plant names in citation content...")
    updated_plants_df = search_plants_in_content(plants_df, citation_content_df)
    
    # Save updated plants DataFrame
    print(f"Saving updated plants data to {output_file}...")
    updated_plants_df.to_csv(output_file, index=False)
    
    print("Done!")
    
    # Print summary
    total_plants = len(plants_df)
    plants_with_links = len(updated_plants_df[updated_plants_df['Crime Report Links'].str.strip() != ''])
    print(f"Summary: Found matching content for {plants_with_links} out of {total_plants} plants.")

if __name__ == "__main__":
    main() 