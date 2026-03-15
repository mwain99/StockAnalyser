"""
setup_windows.py
----------------
One-time setup script for Windows. Downloads winutils.exe which is
required by PySpark to read/write files on Windows.

Usage:
    python setup_windows.py
"""
import os
import sys
import urllib.request
from pathlib import Path

if sys.platform != "win32":
    print("This script is only needed on Windows. Nothing to do.")
    sys.exit(0)

WINUTILS_URL = (
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
)
HADOOP_DLL_URL = (
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"
)

project_root = Path(__file__).parent
hadoop_bin = project_root / "hadoop" / "bin"
hadoop_bin.mkdir(parents=True, exist_ok=True)

winutils_path = hadoop_bin / "winutils.exe"
hadoop_dll_path = hadoop_bin / "hadoop.dll"

def download(url: str, dest: Path) -> None:
    if dest.exists():
        print(f"  Already exists: {dest}")
        return
    print(f"  Downloading {dest.name} ...")
    urllib.request.urlretrieve(url, dest)
    print(f"  Saved to {dest}")

print("Setting up winutils for PySpark on Windows...")
download(WINUTILS_URL, winutils_path)
download(HADOOP_DLL_URL, hadoop_dll_path)

print(f"\n✅ Done. hadoop/bin is ready at: {hadoop_bin.resolve()}")
print("You can now run the pipeline normally.")