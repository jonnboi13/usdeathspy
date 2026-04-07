import pandas as pd
import requests
import zipfile
import io
import os
import re
from pathlib import Path

# --- Utility Functions ---

def download_recode_text(url):
    """Replaces R's download_recode_text function."""
    response = requests.get(url)
    response.raise_for_status()
    return response.text.splitlines()

def parse_recode_text(lines, max_code):
    """
    Translates R's parse_recode_text using Regex.
    Parses '001 = Label' style lines from NCHS/ResDAC text files.
    """
    entries = []
    current = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Check for the start of a new code entry: '001 ='
        if re.match(r"^[0-9]{3}\s*=", line):
            if current:
                entries.append(current)
            current = line
        elif current:
            current += " " + line
            
    if current:
        entries.append(current)

    data = []
    for entry in entries:
        # Regex to split '001 = Description (Extra Info)'
        match = re.match(r"^([0-9]{3})\s*=\s*(.*)$", entry)
        if match:
            code_str, desc = match.groups()
            # Clean up description (remove trailing parentheses and extra whitespace)
            desc = re.sub(r"\s*\([^\)]*\)\s*$", "", desc).strip()
            
            if int(code_str) <= max_code:
                data.append({"code": code_str, "description": desc})
    
    return pd.DataFrame(data).drop_duplicates("code")

def expand_recode(df, min_code, max_code, unknown_prefix):
    """Replaces R's expand_recode to ensure full coverage of code ranges."""
    full_range = [str(i).zfill(3) for i in range(min_code, max_code + 1)]
    full_df = pd.DataFrame({"code": full_range})
    
    merged = pd.merge(full_df, df, on="code", how="left")
    
    # Fill missing descriptions with the custom prefix + the code
    merged["description"] = merged["description"].fillna(
        merged["code"].apply(lambda x: f"{unknown_prefix} {x}")
    )
    return merged

# --- Main Build Logic ---

def build_assets(output_dir="src/usdeathspy/data"):
    """Orchestrates the download and conversion of all lookup tables."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 1. ICD-10-CM 2026 Master List
    print("Downloading ICD-10-CM 2026 Master...")
    icd10_url = "https://ftp.cdc.gov/pub/health_statistics/nchs/publications/ICD10CM/2026/icd10cm-Code%20Descriptions-2026.zip"
    r = requests.get(icd10_url)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # Find the main text file in the zip
        txt_file = [f for f in z.namelist() if f.endswith('.txt') and 'order' not in f.lower()][0]
        with z.open(txt_file) as f:
            lines = f.read().decode('latin-1').splitlines()
            # FWF parsing: Code is chars 6-13, Description starts at 77
            icd10_data = [
                {
                    "code": l[6:13].strip().replace(".", ""), 
                    "description": l[77:500].strip()
                } 
                for l in lines if len(l) > 77
            ]
            pd.DataFrame(icd10_data).to_parquet(os.path.join(output_dir, "icd10_codes.parquet"))

    # 2. Infant Cause Recode 130 (ICD-10 Era)
    print("Building Infant Recode 130...")
    i130_url = "https://resdac.org/sites/datadocumentation.resdac.org/files/130%20ICD-10%20Cause%20of%20Death%20Recodes%20Code%20Table%20%28MBSF-NDI%29.txt"
    i130_raw = parse_recode_text(download_recode_text(i130_url), 158)
    expand_recode(i130_raw, 1, 158, "Infant cause recode").to_parquet(
        os.path.join(output_dir, "infant_cause_130.parquet")
    )

    # 3. General Cause Recode 113 (ICD-10 Era)
    print("Building General Recode 113...")
    c113_url = "https://resdac.org/sites/datadocumentation.resdac.org/files/113%20ICD-10%20Cause%20of%20Death%20Recodes%20Code%20Table%20%28MBSF-NDI%29.txt"
    c113_raw = parse_recode_text(download_recode_text(c113_url), 135)
    expand_recode(c113_raw, 1, 135, "Cause recode").to_parquet(
        os.path.join(output_dir, "cause_113.parquet")
    )

    # 4. Vintage ICD-9 Recodes (Manual Definitions)
    print("Building Vintage ICD-9 Recodes...")
    
    # Infant 61
    i61_data = [
        ["010", "Infectious and parasitic diseases"], ["020", "Septicemia"],
        ["160", "Anencephaly"], ["170", "Spina bifida"], ["200", "Heart anomalies"],
        ["310", "Respiratory distress syndrome"], ["380", "Short gestation/Low birth weight"],
        ["450", "Sudden Infant Death Syndrome (SIDS)"], ["610", "All other causes"]
    ]
    i61 = expand_recode(pd.DataFrame(i61_data, columns=["code", "description"]), 10, 610, "Infant recode (ICD-9)")
    i61.to_parquet(os.path.join(output_dir, "infant_cause_61.parquet"))

    # General 72 (Truncated example - add your full list here)
    c72_data = [["010", "Septicemia"], ["080", "Malignant neoplasms"], ["840", "All other external causes"]]
    c72 = expand_recode(pd.DataFrame(c72_data, columns=["code", "description"]), 10, 840, "Cause recode (ICD-9)")
    c72.to_parquet(os.path.join(output_dir, "cause_72.parquet"))

    # 5. Legacy ICD-9 Fallback
    pd.concat([i61, c72]).drop_duplicates("code").to_parquet(
        os.path.join(output_dir, "icd9_codes.parquet")
    )

if __name__ == "__main__":
    # Path relative to your Project Root
    build_assets(output_dir="src/usdeathspy/data")
    print("\nSUCCESS: All Parquet assets built in src/usdeathspy/data/")