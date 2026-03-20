import win32com.client

outlook = win32com.client.Dispatch("Outlook.Application")
namespace = outlook.GetNamespace("MAPI")

for store in namespace.Stores:
    print(f"\nAccount: {store.DisplayName}")
    try:
        root = store.GetRootFolder()
        for folder in root.Folders:
            print(f"  {folder.Name}")
            for subfolder in folder.Folders:
                print(f"    └─ {subfolder.Name}")
    except Exception as e:
        print(f"  (error: {e})")