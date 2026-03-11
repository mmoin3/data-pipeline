"""State Street MFT client for downloading portfolio data via SFTP."""
import requests
import urllib3
import logging
from pathlib import Path
from config import MFT_URL, MFT_USERNAME, MFT_PASSWORD, MFT_CERT_PATH, MFT_KEY_PATH, RAW_DATA_DIR

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

DL_FOLDER = Path(RAW_DATA_DIR)
FOLDERS = [
    "/ETFGlobalHarvest/fromSSC",
    "/Harvestportf"
]

def login():
    """Create authenticated session with MFT.
    
    Returns:
        requests.Session: Authenticated session with cert and credentials.
    
    Raises:
        Exception: If login fails.
    """
    session = requests.Session()
    session.cert = (MFT_CERT_PATH, MFT_KEY_PATH)
    
    r = session.post(
        MFT_URL,
        data={"username": MFT_USERNAME, "password": MFT_PASSWORD},
        timeout=10,
        verify=False
    )
    if r.status_code != 200:
        raise Exception(f"Login failed: {r.status_code}")
    
    logger.info("Logged in")
    return session


def list_files(session, folder):
    """List files in MFT folder.
    
    Args:
        session: Authenticated requests session.
        folder: Folder path (e.g., '/ETFGlobalHarvest/fromSSC').
    
    Returns:
        list: File metadata dictionaries.
    """
    r = session.get(
        f"{MFT_URL}/files{folder}",
        params={
            "spcmd": "splist",
            "sort": "filename",
            "direction": "ASC",
            "page": 0,
            "start": 0,
            "limit": 100
        },
        verify=False,
        timeout=10
    )
    r.raise_for_status()
    files = r.json().get("files", [])
    logger.info(f"Found {len(files)} items in {folder}")
    return files


def download_file(session, folder, filename):
    """Download file from MFT if not already present.
    
    Args:
        session: Authenticated requests session.
        folder: MFT folder path.
        filename: File to download.
    
    Returns:
        Path: Path to downloaded file.
    """
    DL_FOLDER.mkdir(parents=True, exist_ok=True)
    path = DL_FOLDER / filename
    
    if path.exists():
        logger.info(f"Skipped (exists): {filename}")
        return path
    
    r = session.get(
        f"{MFT_URL}/files{folder}/{filename}",
        verify=False,
        timeout=30,
        stream=True
    )
    r.raise_for_status()
    
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    
    logger.info(f"Downloaded: {filename}")
    return path


def run():
    """Download all files from configured MFT folders."""
    session = login()
    
    for folder in FOLDERS:
        logger.info(f"Processing: {folder}")
        files = list_files(session, folder)
        
        for file in files:
            if not file.get("directory"):
                download_file(session, folder, file.get("filename"))
    
    logger.info("Done")


if __name__ == "__main__":
    run()