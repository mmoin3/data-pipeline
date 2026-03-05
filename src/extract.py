import os
import sys

import pandas as pd

# Ensure project root is on sys.path regardless of the current working directory.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config.settings import DATA_DIR
from src.parsers.inav_bskt import INAVBskt

def load_into_bronze(df12):    
    combined_holdings = pd.DataFrame()
    combined_metadata = pd.DataFrame()
    for idx, fund in enumerate(df12):
        metadata_df = fund['fund_metadata']
        holdings_df = fund['fund_holdings'].copy()

        if metadata_df.empty:
            continue

        # Broadcast scalar metadata values to every row in the holdings block.
        trade_date = metadata_df['TRADE_DATE'].iat[0] if 'TRADE_DATE' in metadata_df.columns else pd.NA
        ss_long_code = metadata_df['SS_LONG_CODE'].iat[0] if 'SS_LONG_CODE' in metadata_df.columns else pd.NA

        holdings_df['TRADE_DATE'] = trade_date
        holdings_df['SS_LONG_CODE'] = ss_long_code
        
        combined_holdings = pd.concat([combined_holdings, holdings_df], ignore_index=True)
        combined_metadata = pd.concat([combined_metadata, metadata_df], ignore_index=True)
    
    combined_holdings.to_csv(os.path.join(DATA_DIR, 'bronze_holdings.csv'), index=False)
    combined_metadata.to_csv(os.path.join(DATA_DIR, 'bronze_metadata.csv'), index=False)
    return

