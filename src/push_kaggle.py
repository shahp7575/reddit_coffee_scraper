import os
import glob
import shutil
import pandas as pd

def pull_push_kaggle(dataset_slug: str, download_kaggle_path: str, download_scraped_path: str):

    # Ensure the download path is fresh
    if os.path.exists(download_kaggle_path):
        shutil.rmtree(download_kaggle_path)
    os.makedirs(download_kaggle_path, exist_ok=True)
    
    # download existing dataset
    os.system(f'kaggle datasets download -d {dataset_slug} -p {download_kaggle_path} --unzip')

    # read downloaded dataset, if exists
    kaggle_csv = glob.glob(download_kaggle_path + "*.csv")[0]
    print(kaggle_csv)

    df_kaggle = pd.read_csv(kaggle_csv)
    print("Kaggle CSV shape: ", df_kaggle.shape)

    # read scraped dataset
    CSV_FILES = glob.glob(download_scraped_path + "*.csv")
    print("Total CSV files scraped: ", len(CSV_FILES))
    df_list = []
    for f in CSV_FILES:
        df = pd.read_csv(f)
        df_list.append(df)
    combined_df = pd.concat(df_list, ignore_index=True)
    print("Scraped CSV files shape: ", combined_df.shape)

    # append scraped data to downloaded kaggle data
    df_final = pd.concat([df_kaggle, combined_df], ignore_index=True)
    df_final.drop_duplicates(inplace=True)
    print("Appended data shape: ", df_final.shape)
    

if __name__ == "__main__":
    DATASET_SLUG = "shahp7575/reddit-posts-with-keyword-coffee/"
    DOWNLOAD_KAGGLE_PATH = "kaggle_downloaded_dataset/"
    DOWNLOAD_SCRAPED_PATH = "data/"

    pull_push_kaggle(dataset_slug=DATASET_SLUG, 
                     download_kaggle_path=DOWNLOAD_KAGGLE_PATH,
                     download_scraped_path=DOWNLOAD_SCRAPED_PATH)