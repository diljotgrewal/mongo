import argparse
import errno
import fnmatch
import os

import azure.storage.blob as azureblob
import pandas as pd
from azure.common.credentials import ServicePrincipalCredentials
from azure.keyvault import KeyVaultClient, KeyVaultAuthentication
from azure.keyvault.models import KeyVaultErrorException
from pymongo import MongoClient


def get_mongo_client(username, password, url, port=27017):
    connection_string = 'mongodb://{}:{}@{}:{}/'.format(
        username, password, url, port
    )
    client = MongoClient(connection_string)
    return client


def get_collection_client(mongoclient, database, collection):
    db = mongoclient[database]
    collection = db[collection]
    return collection


class UnconfiguredStorageAccountError(Exception):
    pass


def makedirs(directory, isfile=False):
    if isfile:
        directory = os.path.dirname(directory)

    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def get_storage_account_key(
        accountname, client_id, secret_key, tenant_id, keyvault_account
):
    """
    Uses the azure management package and the active directory
    credentials to fetch the authentication key for a storage
    account from azure key vault. The key must be stored in azure
    keyvault for this to work.
    :param str accountname: storage account name
    """

    def auth_callback(server, resource, scope):
        credentials = ServicePrincipalCredentials(
            client_id=client_id,
            secret=secret_key,
            tenant=tenant_id,
            resource="https://vault.azure.net"
        )
        token = credentials.token
        return token['token_type'], token['access_token']

    client = KeyVaultClient(KeyVaultAuthentication(auth_callback))
    keyvault = "https://{}.vault.azure.net/".format(keyvault_account)
    # passing in empty string for version returns latest key
    try:
        secret_bundle = client.get_secret(keyvault, accountname, "")
    except KeyVaultErrorException:
        err_str = "The pipeline is not setup to use the {} account. ".format(accountname)
        err_str += "please add the storage key for the account to {} ".format(keyvault_account)
        err_str += "as a secret. All input/output paths should start with accountname"
        raise UnconfiguredStorageAccountError(err_str)
    account_key = secret_bundle.value

    return account_key


def get_blob_client(storage_account_name):
    storage_account_key = get_storage_account_key(
        storage_account,
        client_id=os.environ["CLIENT_ID"],
        secret_key=os.environ["SECRET_KEY"],
        tenant_id=os.environ["TENANT_ID"],
        keyvault_account=os.environ['AZURE_KEYVAULT_ACCOUNT']
    )

    blob_client = azureblob.BlockBlobService(
        account_name=storage_account_name,
        account_key=storage_account_key)

    return blob_client


def unpack_path(filename):
    if filename.startswith('/'):
        filename = filename[1:]
    filename = filename.split('/')
    storage_account = filename[0]
    container_name = filename[1]
    filename = '/'.join(filename[2:])
    if filename.startswith('/'):
        filename = filename[1:]
    return storage_account, container_name, filename


def get_files_to_load(regex, blobclient):
    files = []

    storage_account, container, filenames = unpack_path(regex)

    filenames_split = filenames.split('/')

    if "*" not in filenames_split[0]:
        match_string = '/'.join(filenames_split[1:])
        print 'downloading: {}'.format(filenames_split[0])
        blobs = blobclient.list_blobs(container, prefix=filenames_split[0])
        for blob in blobs:
            if fnmatch.fnmatch(blob.name, match_string):
                files.append((container, blob.name))
    else:
        match_string = '/'.join(filenames_split[1:])
        blobs = blobclient.list_blobs(container, delimiter='/')
        for blob in blobs:
            if fnmatch.fnmatch(blob.name, filenames_split[0]):
                print 'downloading: {}'.format(blob.name)
                blobs = blobclient.list_blobs(container, prefix=blob.name)
                for blob in blobs:
                    if fnmatch.fnmatch(blob.name, match_string):
                        files.append((container, blob.name))

    return files


def download_blob(blobclient, container_name, blob_name, download_path):
    if os.path.exists(download_path):
        blob = blobclient.get_blob_properties(
            container_name,
            blob_name)
        file_size = os.path.getsize(download_path)
        assert file_size == blob.properties.content_length
        return

    blobclient.get_blob_to_path(container_name,
                                blob_name,
                                download_path)


def download_files(blobclient, files, download_dir):
    for container, filepath in files:
        download_path = os.path.join(download_dir, container, filepath)
        makedirs(download_path, isfile=True)
        download_blob(blobclient, container, filepath, download_path)


def dict_iterator_df(filepath):
    def load_csv_pandas(filepath):
        return pd.read_csv(filepath)

    df = load_csv_pandas(filepath)
    for i, row in df.iterrows():
        yield row.to_dict()


def load_dict_to_mongo(client, document):
    client.insert_one(document)


def load_files(
        files_to_load, download_path, username, password, url, database, collection
):
    client = get_mongo_client(username, password, url)
    client = get_collection_client(client, database, collection)
    for container, filepath in files_to_load:
        file_location = os.path.join(download_path, container, filepath)
        print "loading_file {}".format(file_location)

        for document in dict_iterator_df(file_location):
            load_dict_to_mongo(client, document)


def parse_args():
    parser = argparse.ArgumentParser(
        prog='mongo loader',
        description='load pipeline output from blob to mongo'
    )

    parser.add_argument('--files',
                        required=True,
                        help='regex to detect files to load')

    parser.add_argument('--mongo_ip',
                        required=True,
                        help='mongo db url')

    parser.add_argument('--mongo_username',
                        required=True,
                        help='mongo db user')

    parser.add_argument('--mongo_password',
                        required=True,
                        help='mongo db password')

    parser.add_argument('--mongo_db',
                        required=True,
                        help='mongodb database name')

    parser.add_argument('--mongo_collection',
                        required=True,
                        help='mongo db collection name')

    parser.add_argument('--download_dir',
                        required=True,
                        help='tempdir')

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = parse_args()

    regex = args.files

    storage_account, _, _ = unpack_path(regex)

    blobclient = get_blob_client(storage_account)

    files_to_load = get_files_to_load(regex, blobclient)

    download_files(blobclient, files_to_load, args.download_dir)

    load_files(
        files_to_load, args.download_dir, args.mongo_username,
        args.mongo_password, args.mongo_ip,
        args.mongo_db, args.mongo_collection
    )