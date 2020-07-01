# File: arxiv-s3cmd.py
# Acknowledgement: Original by Brienna Herold, http://briennakh.me/notes/bulk-arxiv-download
# Date: 2020-06-30

import boto3
import configparser
import os
import botocore
import json
import tqdm

from bs4 import BeautifulSoup
from datetime import datetime

# TODO: Remove globals?
s3resource = None


def setup():
    """Creates S3 resource & sets configs to enable download."""

    print('Connecting to Amazon S3...')

    # Securely import configs from private config file
    configs = configparser.ConfigParser()
    configs.read('config.ini')  # TODO: Use the standard .s3cmd file

    # Create S3 resource & set configs
    global s3resource
    s3resource = boto3.resource(
        's3',  # the AWS resource we want to use
        aws_access_key_id=configs['DEFAULT']['ACCESS_KEY'],
        aws_secret_access_key=configs['DEFAULT']['SECRET_KEY'],
        region_name='us-east-1'  # same region arxiv bucket is in
    )  # TODO: Region (location) should be taken from .s3cmd


def download_file(key):
    """
    Downloads given filename from source bucket to destination directory.

    Parameters
    ----------
    key : str
        Name of file to download
    """

    # Ensure pdf directory exists
    if not os.path.isdir('pdf'):  # TODO: Make a command-line parameter (pdf, src)
        os.makedirs('pdf')

    # Download file
    print('\nDownloading s3://arxiv/{} to {}...'.format(key, key))

    try:
        s3resource.meta.client.download_file(
            Bucket='arxiv', 
            Key=key,  # name of file to download from
            Filename=key,  # path to file to download to
            ExtraArgs={'RequestPayer':'requester'})
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print('ERROR: ' + key + " does not exist in arxiv bucket")


def explore_metadata():
    """Explores arxiv bucket metadata."""

    print('\narxiv bucket metadata:')

    with open('pdf/arXiv_pdf_manifest.xml', 'r') as manifest:  # TODO: Parameter (pdf, src)
        soup = BeautifulSoup(manifest, 'xml')

        # Print last time the manifest was edited
        timestamp = soup.arXivPDF.find('timestamp', recursive=False).string
        print('Manifest was last edited on ' + timestamp)

        # Print number of files in bucket
        num_files = len(soup.find_all('file'))
        print('arxiv bucket contains ' + str(num_files) + ' tars')

        # Print total size
        total_size = 0
        for size in soup.find_all('size'):
            total_size = total_size + int(size.string)
        print('Total size: ' + str(total_size/1073741824) + ' GiB')

    print('')


def download():
    """Sets up download of tars from arxiv bucket."""

    print('Beginning tar download & extraction...')

    # Create a reusable Paginator
    paginator = s3resource.meta.client.get_paginator('list_objects_v2')  # TODO: What is this setting? Newer version?

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(
        Bucket='arxiv',
        RequestPayer='requester',
        Prefix='pdf/'
    )

    # TODO: Extraction should be an option
    # Download and extract tars
    num_files = 0
    for page in tqdm.tqdm(page_iterator):  # TODO: The command line UI has to be improved (a la gpustat)
        num_files = num_files + len(page['Contents'])
        for file in tqdm.tqdm(page['Contents']):  # TODO: Many other things here (e.g. MD5 verification)
            key = file['Key']
            # If current file is a tar
            if key.endswith('.tar'):
                download_file(key)  # TODO: Make target directory a cmd line parameter
            
    print('Processed ' + str(num_files - 1) + ' tars')  # -1


# TODO: Useful option pause/resume
# TODO: Update will require digging deeper (Is there a manifest for each tar, with versions of papers?)
if __name__ == '__main__':
    """Runs if script is called on command line"""

    # Create S3 resource & set configs
    setup()

    # Download manifest file to current directory
    download_file('pdf/arXiv_pdf_manifest.xml')

    # Explore bucket metadata 
    explore_metadata()

    # TODO: Create a 'dry-run' option (i.e. w/o actual download)
    # Loop and download
    download()
