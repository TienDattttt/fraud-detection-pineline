"""
Download PaySim dataset from Kaggle.

Usage:
    pip install kaggle
    export KAGGLE_USERNAME=your_username
    export KAGGLE_KEY=your_api_key
    python scripts/download_dataset.py

Alternative (manual):
    1. Go to https://www.kaggle.com/datasets/ealaxi/paysim1
    2. Download 'PS_20174392719_1491204439457_log.csv'
    3. Rename to 'paysim_transactions.csv'
    4. Place in 'data/' folder
"""
import os
import sys
import zipfile
import shutil


DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data"
)
FINAL_NAME = "paysim_transactions.csv"
FINAL_PATH = os.path.join(DATA_DIR, FINAL_NAME)
KAGGLE_DATASET = "ealaxi/paysim1"


def download_with_kaggle():
    """Download using Kaggle API."""
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        print("❌ kaggle package not installed.")
        print("   Run: pip install kaggle")
        sys.exit(1)

    api = KaggleApi()
    api.authenticate()

    print(f"📥 Downloading {KAGGLE_DATASET}...")
    api.dataset_download_files(KAGGLE_DATASET, path=DATA_DIR, unzip=False)

    zip_path = os.path.join(DATA_DIR, "paysim1.zip")
    if os.path.exists(zip_path):
        print("📦 Extracting...")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(DATA_DIR)
        os.remove(zip_path)

    # Rename the extracted file
    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv") and f != FINAL_NAME:
            src = os.path.join(DATA_DIR, f)
            shutil.move(src, FINAL_PATH)
            print(f"✅ Renamed {f} → {FINAL_NAME}")
            break


def verify_dataset():
    """Verify downloaded dataset."""
    if not os.path.exists(FINAL_PATH):
        print(f"❌ File not found: {FINAL_PATH}")
        return False

    size_mb = os.path.getsize(FINAL_PATH) / (1024 * 1024)
    print(f"✅ Dataset ready: {FINAL_PATH}")
    print(f"   Size: {size_mb:.1f} MB")

    with open(FINAL_PATH, "r") as f:
        header = f.readline().strip()
        first_row = f.readline().strip()
    print(f"   Header: {header[:80]}...")
    print(f"   Sample: {first_row[:80]}...")

    # Count rows
    with open(FINAL_PATH, "r") as f:
        row_count = sum(1 for _ in f) - 1
    print(f"   Rows: {row_count:,}")
    return True


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(FINAL_PATH):
        print(f"⚡ Dataset already exists at {FINAL_PATH}")
        verify_dataset()
        sys.exit(0)

    print("=" * 60)
    print("PaySim Dataset Downloader")
    print("=" * 60)
    print()
    print("Option 1: Automatic (requires Kaggle API key)")
    print("Option 2: Manual download")
    print()

    choice = input("Try automatic download? [y/N]: ").strip().lower()
    if choice == "y":
        download_with_kaggle()
    else:
        print()
        print("📋 Manual download steps:")
        print("   1. Go to: https://www.kaggle.com/datasets/ealaxi/paysim1")
        print("   2. Click 'Download' (need Kaggle account)")
        print(f"   3. Extract CSV to: {DATA_DIR}/")
        print(f"   4. Rename to: {FINAL_NAME}")
        print()
        input("Press Enter after placing the file...")

    if verify_dataset():
        print("\n🎉 Ready to use!")
    else:
        print("\n❌ Dataset not found. Please download manually.")
        sys.exit(1)
