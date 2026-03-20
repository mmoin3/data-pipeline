# State Street MFT client for file transfers.

import logging
from pathlib import Path
import requests
import urllib3

from config import MFT_BASE_URL, MFT_USERNAME, MFT_PASSWORD, MFT_CERT_PATH, MFT_KEY_PATH, RAW_DATA_DIR

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StateStreetMFTClient:
    """Client for downloading and uploading files to State Street MFT."""

    def __init__(self, download_dir: Path = None):
        self.download_dir = Path(download_dir or RAW_DATA_DIR)
        self.session = None

    def __enter__(self) -> "StateStreetMFTClient":
        self.session = self._login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()

    def _login(self) -> requests.Session:
        session = requests.Session()
        session.cert = (str(MFT_CERT_PATH), str(MFT_KEY_PATH))
        session.verify = False

        response = session.post(
            f"{MFT_BASE_URL}/auth/login",
            data={"username": MFT_USERNAME, "password": MFT_PASSWORD},
            timeout=10,
        )

        if response.status_code != 200:
            raise ConnectionError(f"MFT login failed: {response.status_code}")

        logger.info("Authenticated with MFT")
        return session

    def download(self, remote_path: str, local_filename: str = None, skip_existing: bool = False) -> Path:
        """Download file from MFT.

        Args:
            remote_path: Remote file path (e.g., '/ETFGlobalHarvest/fromSSC/file.csv').
            local_filename: Local filename. Defaults to remote filename.
            skip_existing: If True, skip download when local file already exists.
        """
        filename = local_filename or Path(remote_path).name
        file_path = self.download_dir / filename

        if skip_existing and file_path.exists():
            logger.info(f"Skipped (exists): {filename}")
            return file_path

        logger.info(f"Downloading: {filename}")
        response = self.session.get(f"{MFT_BASE_URL}/files{remote_path}?attachment=", timeout=30, stream=True)
        response.raise_for_status()

        self.download_dir.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Downloaded: {filename}")
        return file_path

    def list_files(self, remote_folder: str) -> list:
        """List files in MFT folder.

        Args:
            remote_folder: Folder path (e.g., '/ETFGlobalHarvest/fromSSC').
        """
        response = self.session.get(f"{MFT_BASE_URL}/files{remote_folder}", timeout=30)
        response.raise_for_status()
        return response.json().get("files", [])

    def upload(self, local_file: Path, remote_path: str) -> bool:
        """Upload file to MFT.

        Args:
            local_file: Path to local file.
            remote_path: Remote destination path (e.g., '/uploads/file.csv').
        """
        with open(local_file, "rb") as f:
            response = self.session.post(f"{MFT_BASE_URL}/files{remote_path}", files={"file": f}, timeout=30)

        response.raise_for_status()
        logger.info(f"Uploaded: {local_file.name}")
        return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with StateStreetMFTClient() as client:
        files = client.list_files("/ETFGlobalHarvest/fromSSC")
        for f in files:
            if not f.get("directory"):
                logger.info(f"  {f.get('filename')} ({f.get('attributes', {}).get('FSR_FILE_SYS_MD.FILE_SIZE', '?')} bytes)")

        client.download("/ETFGlobalHarvest/fromSSC/Harvest_Preburst_INKIND_ALL.20260317.TXT")