import pandas as pd

def parse_cdc_data(data_path, meta_path, nrows=None):
    """
    Parses fixed-width CDC data using a metadata map.
    """
    meta_df = pd.read_csv(meta_path)
    
    widths = (meta_df['end'] - meta_df['start'] + 1).tolist()
    names = meta_df['name'].tolist()

    str_cols = ['month_of_death', 'education', 'sex', 'marital_status']
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
        
        print(f"Success! Captured {len(df)} rows.")
        return df

    except Exception as e:
        print(f"Parsing error: {e}")
        return pd.DataFrame() 