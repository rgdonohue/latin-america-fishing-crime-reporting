import pandas as pd
import os
import re

def load_excel_sheets(file_path):
    """
    Load all sheets from an Excel file into separate pandas DataFrames.
    
    Args:
        file_path (str): Path to the Excel file
        
    Returns:
        dict: Dictionary with sheet names as keys and DataFrames as values
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel file not found at: {file_path}")
    
    # Load the Excel file
    excel_file = pd.ExcelFile(file_path)
    
    # Dictionary to store DataFrames
    dfs = {}
    
    # Load each sheet into a DataFrame
    for sheet_name in excel_file.sheet_names:
        dfs[sheet_name] = pd.read_excel(excel_file, sheet_name=sheet_name)
    
    return dfs

def search_topics_in_content(topics_df, citation_content_df):
    """
    Search for topics in citation content and add matching URLs to the topics dataframe.
    
    Args:
        topics_df (DataFrame): DataFrame containing topics to search for
        citation_content_df (DataFrame): DataFrame containing citation content to search in
        
    Returns:
        DataFrame: Updated topics DataFrame with matching URLs
    """
    # Make a copy of the topics DataFrame to avoid modifying the original
    updated_topics_df = topics_df.copy()
    
    # Initialize the 'Crime Report Links' column with empty strings if it doesn't exist
    if 'Crime Report Links' not in updated_topics_df.columns:
        updated_topics_df['Crime Report Links'] = ''
    
    # Ensure citation_content_df has the expected columns
    if not all(col in citation_content_df.columns for col in ['pdf_path', 'url', 'content']):
        # Try to determine the column names based on the data
        if len(citation_content_df.columns) >= 3:
            # Assume the second column is URL and third is content
            citation_content_df.columns = ['pdf_path', 'url', 'content'] + list(citation_content_df.columns[3:])
        else:
            raise ValueError("Citation content DataFrame doesn't have the expected columns")
    
    # For each topic, search in the citation content
    for idx, row in updated_topics_df.iterrows():
        topic = row['Topic'].lower()
        matching_urls = []
        
        # Skip empty topics
        if not topic or pd.isna(topic):
            continue
        
        # Search for the topic in each citation content
        for _, citation_row in citation_content_df.iterrows():
            content = str(citation_row['content']).lower()
            url = citation_row['url']
            
            # Check if the topic is in the content
            if re.search(r'\b' + re.escape(topic) + r'\b', content):
                matching_urls.append(url)
        
        # Update the 'Crime Report Links' column with the matching URLs
        if matching_urls:
            # If the cell already has content, append to it
            current_links = str(row['Crime Report Links']) if not pd.isna(row['Crime Report Links']) else ''
            if current_links:
                # Check if any of the new URLs are already in the current links
                new_urls = [url for url in matching_urls if url not in current_links]
                if new_urls:
                    updated_topics_df.at[idx, 'Crime Report Links'] = current_links + ', ' + ', '.join(new_urls)
            else:
                updated_topics_df.at[idx, 'Crime Report Links'] = ', '.join(matching_urls)
    
    return updated_topics_df

def search_vessels_in_content(vessels_df, citation_content_df):
    """
    Search for vessel names in citation content and add matching URLs to the vessels dataframe.
    
    Args:
        vessels_df (DataFrame): DataFrame containing vessel names to search for
        citation_content_df (DataFrame): DataFrame containing citation content to search in
        
    Returns:
        DataFrame: Updated vessels DataFrame with matching URLs
    """
    # Make a copy of the vessels DataFrame to avoid modifying the original
    updated_vessels_df = vessels_df.copy()
    
    # Initialize the 'Crime Report Links' column with empty strings if it doesn't exist
    if 'Crime Report Links' not in updated_vessels_df.columns:
        updated_vessels_df['Crime Report Links'] = ''
    
    # For each vessel, search in the citation content
    for idx, row in updated_vessels_df.iterrows():
        vessel_name = str(row['Vessel name']).lower()
        matching_urls = []
        
        # Skip empty vessel names
        if not vessel_name or pd.isna(vessel_name) or vessel_name == 'nan':
            continue
        
        # Search for the vessel name in each citation content
        for _, citation_row in citation_content_df.iterrows():
            content = str(citation_row['content']).lower()
            url = citation_row['url']
            
            # Check if the vessel name is in the content (as a whole word)
            if re.search(r'\b' + re.escape(vessel_name) + r'\b', content):
                matching_urls.append(url)
        
        # Update the 'Crime Report Links' column with the matching URLs
        if matching_urls:
            # If the cell already has content, append to it
            current_links = str(row['Crime Report Links']) if not pd.isna(row['Crime Report Links']) else ''
            if current_links and current_links.lower() != 'nan':
                # Check if any of the new URLs are already in the current links
                new_urls = [url for url in matching_urls if url not in current_links]
                if new_urls:
                    updated_vessels_df.at[idx, 'Crime Report Links'] = current_links + ', ' + ', '.join(new_urls)
            else:
                updated_vessels_df.at[idx, 'Crime Report Links'] = ', '.join(matching_urls)
    
    return updated_vessels_df

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
        company_name = str(row['Company name']).lower()
        matching_urls = []
        
        # Skip empty company names
        if not company_name or pd.isna(company_name) or company_name == 'nan':
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

def search_vessel_owners_in_content(owners_df, citation_content_df):
    """
    Search for vessel owner names in citation content and add matching URLs to the owners dataframe.
    
    Args:
        owners_df (DataFrame): DataFrame containing owner names to search for
        citation_content_df (DataFrame): DataFrame containing citation content to search in
        
    Returns:
        DataFrame: Updated owners DataFrame with matching URLs
    """
    # Make a copy of the owners DataFrame to avoid modifying the original
    updated_owners_df = owners_df.copy()
    
    # Initialize the 'Crime Report Links' column with empty strings if it doesn't exist
    if 'Crime Report Links' not in updated_owners_df.columns:
        updated_owners_df['Crime Report Links'] = ''
    
    # For each owner, search in the citation content
    for idx, row in updated_owners_df.iterrows():
        owner_name = str(row['Owner Name']).lower() if 'Owner Name' in updated_owners_df.columns else ''
        matching_urls = []
        
        # Skip empty owner names
        if not owner_name or pd.isna(owner_name) or owner_name == 'nan':
            continue
        
        # Search for the owner name in each citation content
        for _, citation_row in citation_content_df.iterrows():
            content = str(citation_row['content']).lower()
            url = citation_row['url']
            
            # Check if the owner name is in the content (as a whole word)
            if re.search(r'\b' + re.escape(owner_name) + r'\b', content):
                matching_urls.append(url)
        
        # Update the 'Crime Report Links' column with the matching URLs
        if matching_urls:
            # If the cell already has content, append to it
            current_links = str(row['Crime Report Links']) if not pd.isna(row['Crime Report Links']) else ''
            if current_links and current_links.lower() != 'nan':
                # Check if any of the new URLs are already in the current links
                new_urls = [url for url in matching_urls if url not in current_links]
                if new_urls:
                    updated_owners_df.at[idx, 'Crime Report Links'] = current_links + ', ' + ', '.join(new_urls)
            else:
                updated_owners_df.at[idx, 'Crime Report Links'] = ', '.join(matching_urls)
    
    return updated_owners_df

def main():
    try:
        # Define file paths
        topics_path = 'output/Topics.csv'
        chile_vessels_path = 'output/FMFO Vessels in Chile.csv'
        peru_vessels_path = 'output/FMFO Vessels in Peru.csv'
        ecuador_vessels_path = 'output/FMFO Vessels in Ecuador.csv'
        plants_path = 'output/FMFO Plants in Latin America.csv'
        vessel_owners_path = 'output/Vessel Owners.csv'
        citation_content_path = 'output/citation_content_all_batches.csv'
        output_excel_path = 'output/All_Updated_Data.xlsx'
        
        # Load the Topics CSV
        if os.path.exists(topics_path):
            topics_df = pd.read_csv(topics_path)
            print(f"Loaded Topics.csv with {len(topics_df)} rows")
        else:
            print(f"Warning: {topics_path} not found. Creating empty DataFrame.")
            topics_df = pd.DataFrame(columns=['Topic', 'Crime Report Links'])
        
        # Load the Chile Vessels CSV
        if os.path.exists(chile_vessels_path):
            chile_vessels_df = pd.read_csv(chile_vessels_path)
            print(f"Loaded Chile vessels CSV with {len(chile_vessels_df)} rows")
        else:
            print(f"Warning: {chile_vessels_path} not found. Skipping Chile vessels search.")
            chile_vessels_df = None
        
        # Load the Peru Vessels CSV
        if os.path.exists(peru_vessels_path):
            peru_vessels_df = pd.read_csv(peru_vessels_path)
            print(f"Loaded Peru vessels CSV with {len(peru_vessels_df)} rows")
        else:
            print(f"Warning: {peru_vessels_path} not found. Skipping Peru vessels search.")
            peru_vessels_df = None
        
        # Load the Ecuador Vessels CSV
        if os.path.exists(ecuador_vessels_path):
            ecuador_vessels_df = pd.read_csv(ecuador_vessels_path)
            print(f"Loaded Ecuador vessels CSV with {len(ecuador_vessels_df)} rows")
        else:
            print(f"Warning: {ecuador_vessels_path} not found. Skipping Ecuador vessels search.")
            ecuador_vessels_df = None
        
        # Load the Plants CSV
        if os.path.exists(plants_path):
            plants_df = pd.read_csv(plants_path)
            print(f"Loaded Plants CSV with {len(plants_df)} rows")
        else:
            print(f"Warning: {plants_path} not found. Skipping plants search.")
            plants_df = None
        
        # Load the Vessel Owners CSV
        if os.path.exists(vessel_owners_path):
            vessel_owners_df = pd.read_csv(vessel_owners_path)
            print(f"Loaded Vessel Owners CSV with {len(vessel_owners_df)} rows")
        else:
            print(f"Warning: {vessel_owners_path} not found. Skipping vessel owners search.")
            vessel_owners_df = None
        
        # Load the citation content CSV
        if os.path.exists(citation_content_path):
            try:
                citation_content_df = pd.read_csv(citation_content_path)
            except pd.errors.ParserError:
                # If that fails, try with different settings
                citation_content_df = pd.read_csv(citation_content_path, escapechar='\\', quotechar='"')
            
            print(f"Loaded citation content with {len(citation_content_df)} rows")
            
            # If the DataFrame doesn't have column names, assign them
            if citation_content_df.columns[0].startswith('"output/pdfs'):
                citation_content_df.columns = ['pdf_path', 'url', 'content']
        else:
            print(f"Warning: {citation_content_path} not found. Skipping content search.")
            citation_content_df = None
        
        # Search for topics in citation content if both DataFrames are available
        if citation_content_df is not None and not topics_df.empty:
            updated_topics_df = search_topics_in_content(topics_df, citation_content_df)
            
            # Save the updated Topics DataFrame
            updated_topics_df.to_csv(topics_path, index=False)
            print(f"Updated and saved Topics.csv with matching URLs")
            
            # Display the updated Topics DataFrame
            print("\nUpdated Topics DataFrame:")
            print(updated_topics_df)
        else:
            updated_topics_df = topics_df
        
        # Search for vessel names in citation content if both DataFrames are available
        updated_vessels = {}
        
        if citation_content_df is not None and chile_vessels_df is not None:
            updated_chile_vessels_df = search_vessels_in_content(chile_vessels_df, citation_content_df)
            
            # Save the updated Chile Vessels DataFrame
            updated_chile_vessels_df.to_csv(chile_vessels_path, index=False)
            print(f"Updated and saved Chile vessels CSV with matching URLs")
            
            # Display the number of vessels with matches
            matches = (updated_chile_vessels_df['Crime Report Links'] != '') & (~updated_chile_vessels_df['Crime Report Links'].isna())
            print(f"\nFound matches for {matches.sum()} vessels in Chile")
            
            # Display a few examples of matches
            if matches.sum() > 0:
                print("\nExamples of vessels with matches:")
                print(updated_chile_vessels_df[matches].head())
            
            updated_vessels['Chile'] = updated_chile_vessels_df
        else:
            updated_vessels['Chile'] = chile_vessels_df
        
        if citation_content_df is not None and peru_vessels_df is not None:
            updated_peru_vessels_df = search_vessels_in_content(peru_vessels_df, citation_content_df)
            
            # Save the updated Peru Vessels DataFrame
            updated_peru_vessels_df.to_csv(peru_vessels_path, index=False)
            print(f"Updated and saved Peru vessels CSV with matching URLs")
            
            # Display the number of vessels with matches
            matches = (updated_peru_vessels_df['Crime Report Links'] != '') & (~updated_peru_vessels_df['Crime Report Links'].isna())
            print(f"\nFound matches for {matches.sum()} vessels in Peru")
            
            # Display a few examples of matches
            if matches.sum() > 0:
                print("\nExamples of vessels with matches:")
                print(updated_peru_vessels_df[matches].head())
            
            updated_vessels['Peru'] = updated_peru_vessels_df
        else:
            updated_vessels['Peru'] = peru_vessels_df
        
        if citation_content_df is not None and ecuador_vessels_df is not None:
            updated_ecuador_vessels_df = search_vessels_in_content(ecuador_vessels_df, citation_content_df)
            
            # Save the updated Ecuador Vessels DataFrame
            updated_ecuador_vessels_df.to_csv(ecuador_vessels_path, index=False)
            print(f"Updated and saved Ecuador vessels CSV with matching URLs")
            
            # Display the number of vessels with matches
            matches = (updated_ecuador_vessels_df['Crime Report Links'] != '') & (~updated_ecuador_vessels_df['Crime Report Links'].isna())
            print(f"\nFound matches for {matches.sum()} vessels in Ecuador")
            
            # Display a few examples of matches
            if matches.sum() > 0:
                print("\nExamples of vessels with matches:")
                print(updated_ecuador_vessels_df[matches].head())
            
            updated_vessels['Ecuador'] = updated_ecuador_vessels_df
        else:
            updated_vessels['Ecuador'] = ecuador_vessels_df
        
        # Search for plant names in citation content if both DataFrames are available
        if citation_content_df is not None and plants_df is not None:
            updated_plants_df = search_plants_in_content(plants_df, citation_content_df)
            
            # Save the updated Plants DataFrame
            updated_plants_df.to_csv(plants_path, index=False)
            print(f"Updated and saved Plants CSV with matching URLs")
            
            # Display the number of plants with matches
            matches = (updated_plants_df['Crime Report Links'] != '') & (~updated_plants_df['Crime Report Links'].isna())
            print(f"\nFound matches for {matches.sum()} plants")
            
            # Display a few examples of matches
            if matches.sum() > 0:
                print("\nExamples of plants with matches:")
                print(updated_plants_df[matches].head())
        else:
            updated_plants_df = plants_df
        
        # Search for vessel owner names in citation content if both DataFrames are available
        if citation_content_df is not None and vessel_owners_df is not None:
            updated_vessel_owners_df = search_vessel_owners_in_content(vessel_owners_df, citation_content_df)
            
            # Save the updated Vessel Owners DataFrame
            updated_vessel_owners_df.to_csv(vessel_owners_path, index=False)
            print(f"Updated and saved Vessel Owners CSV with matching URLs")
            
            # Display the number of vessel owners with matches
            matches = (updated_vessel_owners_df['Crime Report Links'] != '') & (~updated_vessel_owners_df['Crime Report Links'].isna())
            print(f"\nFound matches for {matches.sum()} vessel owners")
            
            # Display a few examples of matches
            if matches.sum() > 0:
                print("\nExamples of vessel owners with matches:")
                print(updated_vessel_owners_df[matches].head())
        else:
            updated_vessel_owners_df = vessel_owners_df
        
        # Save all updated DataFrames to a single Excel file
        with pd.ExcelWriter(output_excel_path) as writer:
            updated_topics_df.to_excel(writer, sheet_name='Topics', index=False)
            
            if updated_vessels['Chile'] is not None:
                updated_vessels['Chile'].to_excel(writer, sheet_name='Chile Vessels', index=False)
            
            if updated_vessels['Peru'] is not None:
                updated_vessels['Peru'].to_excel(writer, sheet_name='Peru Vessels', index=False)
            
            if updated_vessels['Ecuador'] is not None:
                updated_vessels['Ecuador'].to_excel(writer, sheet_name='Ecuador Vessels', index=False)
            
            if updated_plants_df is not None:
                updated_plants_df.to_excel(writer, sheet_name='Plants', index=False)
            
            if updated_vessel_owners_df is not None:
                updated_vessel_owners_df.to_excel(writer, sheet_name='Vessel Owners', index=False)
        
        print(f"\nAll updated data saved to {output_excel_path}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
