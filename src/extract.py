import os
import sys
from sqlalchemy import create_engine, text

import pandas as pd

# Ensure project root is on sys.path regardless of the current working directory.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config.settings import DATA_DIR
from src.parsers.inav_bskt import INAVBskt

def load_into_bronze(df12):    
    pass