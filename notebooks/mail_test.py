
import imaplib
import email
from pathlib import Path
import os
import re
import sys
sys.path.append(str(Path("..").resolve()))
from config import *
from email.header import decode_header
from dotenv import load_dotenv
import win32com.client
from datetime import datetime
load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
SAVE_FOLDER   = ROOT_DIR/"data/raw/all_positions"
# Outlook folder path — top-level inbox name is usually your email address
OUTLOOK_ACCOUNT  = "mmoin@harvestetfs.com"
SUBFOLDER_NAME   = ""
 
# Processed in order — most recent first, older as best-effort
DATE_RANGES = [
    (datetime(2024, 1, 1), None,                  "Priority: 2024 to present"),
    (datetime(2019, 1, 1), datetime(2023, 12, 31), "Best-effort: 2019-2023"),
]
 
FILENAME_PATTERN = re.compile(r"All_Positions\d{8}\.csv", re.IGNORECASE)
 
# ──────────────────────────────────────────────────────────────────────────────
 
 
def get_folder(outlook):
    """Navigate to the All Positions subfolder inside Inbox."""
    namespace = outlook.GetNamespace("MAPI")
 
    # Find the right account in case multiple are set up in Outlook
    inbox = None
    for store in namespace.Stores:
        if OUTLOOK_ACCOUNT.lower() in store.DisplayName.lower():
            inbox = store.GetRootFolder().Folders["Inbox"].Folders[SUBFOLDER_NAME]
            break
 
    if inbox is None:
        # Fallback to default inbox
        inbox = namespace.GetDefaultFolder(6)  # 6 = olFolderInbox
 
    return inbox.Folders[SUBFOLDER_NAME]
 
 
def process_range(folder, since, before, label, save_folder):
    print(f"\n── {label} ──")
    os.makedirs(save_folder, exist_ok=True)
 
    # Build a filter string in Outlook's format
    if before:
        filter_str = (
            f"[ReceivedTime] >= '{since.strftime('%m/%d/%Y')}' AND "
            f"[ReceivedTime] < '{before.strftime('%m/%d/%Y')}'"
        )
    else:
        filter_str = f"[ReceivedTime] >= '{since.strftime('%m/%d/%Y')}'"
 
    items = folder.Items
    items.Sort("[ReceivedTime]", True)   # True = descending, so newest first
    filtered = items.Restrict(filter_str)
 
    count = filtered.Count
    if count == 0:
        print("  No emails found in this range.")
        return 0, 0
 
    print(f"  {count} emails found — downloading newest first...")
    downloaded = already_have = 0
 
    for i in range(1, count + 1):  # COM collections are 1-indexed
        try:
            msg = filtered.Item(i)
        except Exception:
            continue
 
        for j in range(1, msg.Attachments.Count + 1):
            try:
                att = msg.Attachments.Item(j)
            except Exception:
                continue
 
            filename = att.FileName
            if not FILENAME_PATTERN.match(filename):
                continue
 
            save_path = os.path.join(save_folder, filename)
            if os.path.exists(save_path):
                already_have += 1
                continue
 
            att.SaveAsFile(save_path)
            print(f"  ✓ Saved: {filename}")
            downloaded += 1
 
    return downloaded, already_have
 
 
def main():
    print("Connecting to Outlook...")
    outlook = win32com.client.Dispatch("Outlook.Application")
 
    print(f"Opening folder: Inbox / {SUBFOLDER_NAME}")
    folder = get_folder(outlook)
 
    total_dl = total_have = 0
 
    for since, before, label in DATE_RANGES:
        try:
            dl, have = process_range(folder, since, before, label, SAVE_FOLDER)
            total_dl   += dl
            total_have += have
        except KeyboardInterrupt:
            print("\n\nStopped — all files downloaded so far are safely saved.")
            break
        except Exception as e:
            print(f"\n  Error during '{label}': {e}")
            print("  Skipping to next range...")
 
    print(f"\nDone. {total_dl} new file(s) downloaded, {total_have} already existed.")
    print(f"Saved to: {SAVE_FOLDER}")

if __name__ == "__main__":
    main()