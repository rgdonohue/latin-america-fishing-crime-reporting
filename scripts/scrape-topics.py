import pandas as pd
import re
import os
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("topic_scraping.log"),
        logging.StreamHandler()
    ]
)

def main():
    # Check if files exist
    topics_path = 'data-output/Topics.csv'
    citations_path = 'data-output/citation_content_all_batches.csv'
    
    if not os.path.exists(topics_path) or not os.path.exists(citations_path):
        logging.error(f"Required files not found. Please check paths: {topics_path}, {citations_path}")
        return
    
    # Load the data
    logging.info("Loading data files...")
    try:
        topics_df = pd.read_csv(topics_path)
        citations_df = pd.read_csv(citations_path)
    except Exception as e:
        logging.error(f"Error loading data files: {e}")
        return
    
    # Create a dictionary of topics and their Spanish equivalents
    topic_translations = {
        'IUU': ['IUU', 'pesca INDNR', 'pesca ilegal', 'ilegal', 'no declarada', 'no reglamentada', 'illegal fishing'],
        'fishmeal': ['fishmeal', 'harina de pescado', 'fish meal', 'fish flour'],
        'Trafficking': ['trafficking', 'tráfico', 'trata', 'contrabando', 'human trafficking', 'drug trafficking'],
        'Fishing': ['fishing', 'pesca', 'pesquero', 'pesquera', 'fishery', 'fisheries'],
        'ship': ['ship', 'barco', 'embarcación', 'navío', 'buque', 'nave', 'vessel', 'boat']
    }
    
    # Create a dictionary to store URLs by topic for faster lookup
    topic_urls = {}
    for i, row in topics_df.iterrows():
        topic = row['Topic']
        if isinstance(row['Crime Report Links'], str) and row['Crime Report Links'].strip():
            topic_urls[topic] = set(url.strip() for url in row['Crime Report Links'].split(','))
        else:
            topic_urls[topic] = set()
    
    # Process citations in batches to improve memory usage
    logging.info("Processing citations to find topic matches...")
    batch_size = 500
    total_batches = (len(citations_df) + batch_size - 1) // batch_size
    
    for batch_idx in tqdm(range(total_batches), desc="Processing citation batches"):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, len(citations_df))
        batch_df = citations_df.iloc[start_idx:end_idx]
        
        for _, citation in batch_df.iterrows():
            content = citation['content']
            url = citation['url']
            
            # Skip if URL is invalid
            if not isinstance(url, str) or 'Error: HTTP' in url:
                continue
            
            # Skip if content is not a string
            if not isinstance(content, str):
                continue
            
            # Check each topic
            for topic, translations in topic_translations.items():
                # Skip if this URL is already associated with this topic
                if url in topic_urls[topic]:
                    continue
                
                # Check if any translation appears in the content
                for translation in translations:
                    pattern = re.compile(r'\b' + re.escape(translation) + r'\b', re.IGNORECASE)
                    if pattern.search(content):
                        topic_urls[topic].add(url)
                        break
    
    # Update the DataFrame with new URLs
    logging.info("Updating topics with new URLs...")
    for i, row in topics_df.iterrows():
        topic = row['Topic']
        if topic in topic_urls:
            topics_df.at[i, 'Crime Report Links'] = ', '.join(topic_urls[topic]) if topic_urls[topic] else ""
    
    # Save the updated DataFrame
    output_path = 'data-output/Topics_updated.csv'
    try:
        topics_df.to_csv(output_path, index=False)
        logging.info(f"Topics updated and saved to {output_path}")
    except Exception as e:
        logging.error(f"Error saving updated topics: {e}")
    
    # Print summary of results
    for topic, urls in topic_urls.items():
        logging.info(f"Topic '{topic}': Found {len(urls)} URLs")

if __name__ == "__main__":
    main()

# Note: For plant names functionality, we would need a similar approach
# with a dataset of plant names, removing suffixes like "S.A.", "S.A.C.", etc.
