import os
import glob
import shutil
import pandas as pd
from datetime import datetime

def ensure_fresh_directory(directory:str):
    """Ensure the directory is empty and fresh to use."""
    if os.path.exists(directory):
        for file in os.listdir(directory):
            if file.endswith(".csv"):
                os.remove(os.path.join(directory, file))
    else:
        os.makedirs(directory, exist_ok=True)

def download_kaggle_dataset(dataset_slug:str, download_path:str):
    """Download existing kaggle dataset"""
    ensure_fresh_directory(download_path)
    if os.system(f'kaggle datasets download -d {dataset_slug} -p {download_path} --unzip') == 0:
        print("Kaggle dataset download successfull.")
    else:
        print("Failed to download latest kaggle dataset")

def read_csv_files(directory: str):
    """Reads and merges all CSV files in the specified directory."""
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".csv")]
    return pd.concat([pd.read_csv(file) for file in files], ignore_index=True) if files else pd.DataFrame()

def merge_and_upload_to_kaggle(data_folder: str, dataset_slug: str, download_path: str):
    """Merges scraped data with existing Kaggle dataset and uploads the updated dataset."""
    download_kaggle_dataset(dataset_slug, download_path)
    download_kaggle_file_path = glob.glob(download_path + "*.csv")[0]
    
    existing_data = pd.read_csv(download_kaggle_file_path) if os.path.exists(download_kaggle_file_path) else pd.DataFrame()
    new_data = read_csv_files(data_folder)
    
    if new_data.empty:
        print("No new data files found.")
        return
    
    merged_data = pd.concat([existing_data, new_data], ignore_index=True)

    max_utc = merged_data['created_utc'].max()
    max_datetime = datetime.strftime(datetime.utcfromtimestamp(max_utc), "%Y-%m-%d %H:%m:%S")
    print("Latest file timestamp: ", max_datetime)

    version_name = f"reddit_coffee_scraper_till_{int(max_utc)}"

    if merged_data.shape[0] > 0:
        ensure_fresh_directory(download_path)
        merged_data.to_csv(f"{download_path}{version_name}.csv", index=False)
    else:
        print("Merge data not succesfull. Breaking.")
        return
    
    print("Final data shape: ", merged_data.shape)

    if merged_data.shape[0] > 0 and os.system(f'kaggle datasets version -p {download_path} -m "New data upload - {version_name}" --dir-mode zip') == 0:
        print("Dataset successfully updated on kaggle.")
        ensure_fresh_directory(download_path)
        ensure_fresh_directory(data_folder)
        print("Scraped data files removed successfully.")
    else:
        print("Failed to update dataset on Kaggle.")
    

if __name__ == "__main__":
    DATA_FOLDER = "data/" # folder for scraped .csvs
    DOWNLOAD_KAGGLE_PATH = "kaggle_downloaded_dataset/"
    DATASET_SLUG = "shahp7575/reddit-posts-with-keyword-coffee/"
    
    merge_and_upload_to_kaggle(data_folder = DATA_FOLDER,
                               dataset_slug = DATASET_SLUG,
                               download_path = DOWNLOAD_KAGGLE_PATH)
