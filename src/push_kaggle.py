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


    
        

# def pull_push_kaggle(dataset_slug: str, download_kaggle_path: str, download_scraped_path: str):

#     # Ensure the download path is fresh
#     if os.path.exists(download_kaggle_path):
#         shutil.rmtree(download_kaggle_path)
#     os.makedirs(download_kaggle_path, exist_ok=True)
    
#     # download existing dataset
#     os.system(f'kaggle datasets download -d {dataset_slug} -p {download_kaggle_path} --unzip')

#     # read downloaded dataset, if exists
#     kaggle_csv = glob.glob(download_kaggle_path + "*.csv")[0]
#     print(kaggle_csv)

#     df_kaggle = pd.read_csv(kaggle_csv)
#     print("Kaggle CSV shape: ", df_kaggle.shape)

#     # read scraped dataset
#     CSV_FILES = glob.glob(download_scraped_path + "*.csv")
#     print("Total CSV files scraped: ", len(CSV_FILES))
#     df_list = []
#     for f in CSV_FILES:
#         df = pd.read_csv(f)
#         df_list.append(df)
#     combined_df = pd.concat(df_list, ignore_index=True)
#     print("Scraped CSV files shape: ", combined_df.shape)

#     # append scraped data to downloaded kaggle data
#     df_final = pd.concat([df_kaggle, combined_df], ignore_index=True)
#     df_final.drop_duplicates(inplace=True)
#     print("Appended data shape: ", df_final.shape)

#     # push to kaggle
#     max_utc = df_final['created_utc'].max()
#     max_datetime = datetime.strftime(datetime.utcfromtimestamp(max_utc), "%Y-%m-%d %H:%m:%S")
#     print("Latest file timestamp: ", max_datetime)

#     version_name = f"reddit_coffee_scraper_till_{max_datetime}"

#     df_final.to_csv(f"{download_kaggle_path}/{version_name}.csv", index=False)
    

# if __name__ == "__main__":
#     DATASET_SLUG = "shahp7575/reddit-posts-with-keyword-coffee/"
#     DOWNLOAD_KAGGLE_PATH = "kaggle_downloaded_dataset/"
#     DOWNLOAD_SCRAPED_PATH = "data/"

#     pull_push_kaggle(dataset_slug=DATASET_SLUG, 
#                      download_kaggle_path=DOWNLOAD_KAGGLE_PATH,
#                      download_scraped_path=DOWNLOAD_SCRAPED_PATH)