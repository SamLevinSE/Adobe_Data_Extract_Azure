import os
import sys
import tarfile
import logging
from azure.storage.blob import BlobServiceClient

FORMAT = '\n%(asctime)s \t %(message)s\n'
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.DEBUG)

run_extraction = False


def extract(DOWNLOAD_DIR, DATA_DIR, FILE_NAME_PATTERN):
    files = os.listdir(DOWNLOAD_DIR)
    if not files:
        logging.info('Download directory is not valid!')
        return

    logging.info('In progress...')
    for file in files:
        if FILE_NAME_PATTERN in file:
            src_path = os.path.join(DOWNLOAD_DIR, file)
            target_path = os.path.join(DATA_DIR, os.path.splitext(file)[0])
            with tarfile.open(src_path) as tar_file:
                tar_file.extract('hit_data.tsv', target_path)
                tsv_files = os.listdir(target_path)
                for tsv_file in tsv_files:
                    if 'hit_data.tsv' not in tsv_file:
                        os.remove(f'{target_path}/{tsv_file}')


def download_azure_files(DOWNLOAD_DIR, DATA_DIR, FILE_NAME_PATTERN):
    AZURE_STORAGE_CONNECTION_STRING = '<FILL THIS>'
    CONTAINERNAME = '<FILL THIS>'

    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    blob_client = blob_service_client.get_container_client(
        container=CONTAINERNAME
    )

    for blob in blob_client.list_blobs():
        if FILE_NAME_PATTERN in blob.name:
            download_file_path = os.path.join(DOWNLOAD_DIR, blob.name)
            blob_client_instance = blob_service_client.get_container_client(
                container=CONTAINERNAME
            )

            with open(download_file_path, "wb") as download_file:
                download_file.write(
                    blob_client_instance.download_blob(blob.name).readall()
                )
            logging.info(f'{blob.name} has been downloaded')

    extract(DOWNLOAD_DIR, DATA_DIR, FILE_NAME_PATTERN)


def main(DOWNLOAD_DIR, DATA_DIR, FILE_NAME_PATTERN):
    logging.info('------- Process started!')

    try:
        os.mkdir('DOWNLOADS')
    except:
        pass

    download_azure_files(DOWNLOAD_DIR, DATA_DIR, FILE_NAME_PATTERN)

    logging.info('------- Process finished successfully!')


if __name__ == '__main__':
    DOWNLOAD_DIR = os.path.join(os.path.abspath(''), 'DOWNLOADS/')
    DATA_DIR = os.path.abspath('./DataFeeds/data/')

    if len(sys.argv) < 2:
        logging.error(
            'Please type the file-name pattern as the second argument after mainning the file. E.x., python3 datafeed_file_extraction.py sparebank1prod_2022.'
        )
        exit(1)
    elif len(sys.argv) == 2:
        file_pattern = sys.argv[1]
        main(DOWNLOAD_DIR, DATA_DIR, file_pattern)
    elif len(sys.argv) > 1:
        file_pattern = sys.argv[1]
        target_file = sys.argv[2]
        main(DOWNLOAD_DIR, target_file, file_pattern)
