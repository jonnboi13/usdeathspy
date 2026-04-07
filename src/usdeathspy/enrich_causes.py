import pandas as pd
import numpy as np
from importlib.resources import files

def _enrich_causes(df, year=None):
    """
    Main enrichment entry point. Identifies the ICD revision, detects the 
    appropriate cause column, and maps codes to human-readable descriptions.
    """
    # 1. Determine Year and Revision first to guide column detection
    all_cols_lower = [c.lower() for c in df.columns]
    
    if year is not None:
        target_year = year
    elif "year" in all_cols_lower:
        year_idx = all_cols_lower.index("year")
        years = df.iloc[:, year_idx].dropna().unique()
        target_year = years[0] if len(years) > 0 else 1999
    else:
        target_year = 1999 

    revision = "icd10" if target_year >= 1999 else "icd9"

    # 2. Identify the cause column based on the revision context
    cause_col = _detect_cause_col(df, revision)
    
    if not cause_col:
        return df

    # 3. Fetch the corresponding internal lookup table
    lookup_df = _get_lookup(revision, cause_col)

    # 4. Data Engineering: Standardize keys for the join
    def force_clean_code(series):
        """Forces numeric conversion to strip decimals (.0) and pads strings."""
        nums = pd.to_numeric(series, errors='coerce')
        return (
            nums.fillna(0)
            .astype(int)
            .astype(str)
            .str.strip()
            .str.zfill(3)
            .replace('000', np.nan)
        )

    # Clean both the user data and the lookup table to ensure a perfect match
    df[cause_col] = force_clean_code(df[cause_col])
    lookup_df["code"] = force_clean_code(lookup_df["code"])
    
    # Prepare a clean, deduplicated mapping
    lookup_clean = (
        lookup_df[["code", "description"]]
        .drop_duplicates(subset=["code"])
        .reset_index(drop=True)
    )

    # 5. Execute the join (Left Join keeps all original rows)
    df = df.merge(lookup_clean, left_on=cause_col, right_on="code", how="left")
    
    # Finalize columns
    df = df.rename(columns={"description": "cause_description"})
    df["cause_description"] = df["cause_description"].fillna("Other/Unknown")
    
    # Drop the temporary join key
    if "code" in df.columns:
        df = df.drop(columns=["code"])
        
    return df

def _detect_cause_col(df, revision):
    """
    Identifies the best cause column available in the dataframe.
    Priority is shifted based on whether the data is ICD-9 or ICD-10.
    """
    if revision == "icd10":
        candidates = ["ucodr130", "cause_recode_130", "ucodr113", "cause_recode_113", "ucod"]
    else:
        # ICD-9 Priority (Infant 61 -> General 72 -> Raw)
        candidates = ["icr61", "cause_recode_61_infant", "ucodr72", "cause_recode_72", "ucod"]
    
    current_cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in current_cols:
            return current_cols[cand]
    return None

def _get_lookup(revision, cause_col):
    """
    Routes the request to the correct Parquet asset within the package data.
    """
    data_path = files("usdeathspy.data") 
    col_lower = cause_col.lower()
    
    # ICD-9 Assets
    if revision == "icd9":
        if any(k in col_lower for k in ["61", "icr61"]):
            return pd.read_parquet(data_path.joinpath("infant_cause_61.parquet"))
        if any(k in col_lower for k in ["72", "ucodr72"]):
            return pd.read_parquet(data_path.joinpath("cause_72.parquet"))
        return pd.read_parquet(data_path.joinpath("icd9_codes.parquet"))
    
    # ICD-10 Assets
    else:
        if any(k in col_lower for k in ["130", "infant_cause_130"]):
            return pd.read_parquet(data_path.joinpath("infant_cause_130.parquet"))
        if any(k in col_lower for k in ["113", "cause_113"]):
            return pd.read_parquet(data_path.joinpath("cause_113.parquet"))
        return pd.read_parquet(data_path.joinpath("icd10_codes.parquet"))