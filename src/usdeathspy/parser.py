import pandas as pd
# Import the internal enrichment function from your local module
from .enrich_causes import _enrich_causes 

def parse_cdc_data(data_path, meta_path, year=None, nrows=None):
    """
    Parses fixed-width CDC data using a metadata map and enriches it 
    with human-readable cause of death descriptions.
    """
    meta_df = pd.read_csv(meta_path)
    
    widths = (meta_df['end'] - meta_df['start'] + 1).tolist()
    names = meta_df['name'].tolist()

    # Ensure categorical/ID columns are read as strings to preserve padding
    str_cols = [
    'month_of_death', 'education', 'sex', 'marital_status',
    'icr61', 'ucodr130', 'ucodr113', 'cause_recode_72' # Add these!
]
    dtype_settings = {col: str for col in str_cols if col in names}

    try:
        df = pd.read_fwf(
            data_path, 
            widths=widths, 
            names=names, 
            nrows=nrows, 
            encoding='latin-1',
            dtype=dtype_settings  
        )
        
        # Check if data was actually captured before enriching
        if not df.empty:
            # Call the internal router to map ICD/NCHS codes to descriptions
            df = _enrich_causes(df, year=year)
            print(f"Success! Captured and enriched {len(df)} rows.")
        
        return df

    except Exception as e:
        print(f"Parsing error: {e}")
        return pd.DataFrame()