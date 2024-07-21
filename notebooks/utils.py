import pandas as pd

def show_new_items(df_existing, df_latest):
    #Remove duplicates from df_existing
    df_existing = df_existing.drop_duplicates(subset=['BNF_CODE', 'BNF_DESCRIPTION'])

    # Merge the dataframes with an indicator
    merged_df = df_latest.merge(df_existing, on='BNF_CODE', how='left', suffixes=('_latest', '_existing'))

    # Filter rows where BNF_DESCRIPTIONs do not match
    updated_codes = merged_df[merged_df['BNF_CODE_latest'] != merged_df['BNF_CODE_existing']]

    # Select the relevant columns and rename them
    updated_codes = updated_codes[['BNF_CODE', 'BNF_DESCRIPTION_latest']]
    updated_codes = updated_codes.rename(columns={'BNF_DESCRIPTION_latest': 'BNF_DESCRIPTION'})

    # Sort the dataframe by BNF_CODE
    updated_codes = sort_by_bnf_code(updated_codes)

    return updated_codes

def find_bnf_code_only_in_latest(df_existing, df_latest):
    # Find BNF_CODEs that are only in df_latest
    latest_codes = set(df_latest['BNF_CODE'])
    existing_codes = set(df_existing['BNF_CODE'])
    unique_codes = latest_codes - existing_codes
    
    # Filter df_latest based on unique BNF_CODEs
    result = df_latest[df_latest['BNF_CODE'].isin(unique_codes)]
    result=sort_by_bnf_code(result)
    return result

def find_bnf_description_only_in_latest(df_existing, df_latest):
    # Find BNF_DESCRIPTIONs that are only in df_latest
    latest_descriptions = set(df_latest['BNF_DESCRIPTION'])
    existing_descriptions = set(df_existing['BNF_DESCRIPTION'])
    unique_descriptions = latest_descriptions - existing_descriptions
    
    # Filter df_latest based on unique BNF_DESCRIPTIONs
    result = df_latest[df_latest['BNF_DESCRIPTION'].isin(unique_descriptions)]
    result=sort_by_bnf_code(result)
    return result

def find_chemical_substance_bnf_descr_only_in_latest(df_existing, df_latest):
    # Find CHEMICAL_SUBSTANCE_BNF_DESCRs that are only in df_latest
    latest_substances = set(df_latest['CHEMICAL_SUBSTANCE_BNF_DESCR'])
    existing_substances = set(df_existing['CHEMICAL_SUBSTANCE_BNF_DESCR'])
    unique_substances = latest_substances - existing_substances
    
    # Filter df_latest based on unique CHEMICAL_SUBSTANCE_BNF_DESCRs
    result = df_latest[df_latest['CHEMICAL_SUBSTANCE_BNF_DESCR'].isin(unique_substances)]
    result=sort_by_bnf_code(result)
    return result

def sort_by_bnf_code(df):
    # Make a copy of the dataframe to avoid SettingWithCopyWarning
    df = df.copy()

    # Add column BNF_CHAPTER which is the first 2 characters of BNF_CODE
    df.loc[:, 'BNF_CHAPTER'] = df['BNF_CODE'].str[:2]
    # Add column BNF_SECTION which is the 3rd and 4th characters of BNF_CODE
    df.loc[:, 'BNF_SECTION'] = df['BNF_CODE'].str[2:4]
    # Add column BNF_PARAGRAPH which is the 5th and 6th characters of BNF_CODE
    df.loc[:, 'BNF_PARAGRAPH'] = df['BNF_CODE'].str[4:6]
    # Add column BNF_SUBPARAGRAPH which is the 7th character of BNF_CODE
    df.loc[:, 'BNF_SUBPARAGRAPH'] = df['BNF_CODE'].str[6]

    # Sort the dataframe by BNF_CHAPTER Ascending, BNF_SECTION Ascending, BNF_PARAGRAPH Ascending, BNF_SUBPARAGRAPH Ascending
    df = df.sort_values(by=['BNF_CHAPTER', 'BNF_SECTION', 'BNF_PARAGRAPH', 'BNF_SUBPARAGRAPH'])

    # Drop the intermediate columns
    df = df.drop(columns=['BNF_CHAPTER', 'BNF_SECTION', 'BNF_PARAGRAPH', 'BNF_SUBPARAGRAPH'])

    # Reset the index
    df = df.reset_index(drop=True)

    return df