import rdflib
from rdflib import URIRef, Namespace
from rdflib.namespace import RDF
import os 
import pandas as pd
from tqdm import tqdm
import configparser
import argparse
import boto3
from concurrent.futures import ThreadPoolExecutor
import requests
import zipfile  
import tarfile 
import subprocess

DATA_BASE = "data/data"
CATALOG_BASE = "data/catalog"
CATALOG_EPUB_URL = f"{CATALOG_BASE}/cache/epub/"
FILE_TYPE = ".txt"


def get_book_url(g, id):
    """ Get the URL link for the book in the FILE_TYPE format
    """
    subject = URIRef(f"http://www.gutenberg.org/ebooks/{id}")
    fmt = URIRef(f"http://purl.org/dc/terms/hasFormat")
    for s, p, o in g.triples((subject, fmt, None)):
        if "http://www.gutenberg.org/files" in o and \
            FILE_TYPE in o:
                url = o
                break

    return url

def get_title(g, id):
    """ Get the title name for the selected book ID
    """

    subject = URIRef(f"http://www.gutenberg.org/ebooks/{id}")
    title_p = URIRef("http://purl.org/dc/terms/title")

    title = next(g.objects(subject, title_p))

    return title


def get_language(g, id):
    """ Get the language for the selected book ID 
    """
    subject = URIRef(f"http://www.gutenberg.org/ebooks/{id}")

    language_p =  URIRef("http://purl.org/dc/terms/language")
    for s, p, o in g.triples((subject, language_p, None)):
        for s, p, o in g.triples((o, RDF.value, None)):
            language = o

    return language


def get_author_info(g, id):
    """ Get author information for the selected book ID
    """
    subject = URIRef(f"http://www.gutenberg.org/ebooks/{id}")

    # Get Author
    creator_p = URIRef("http://purl.org/dc/terms/creator")
    for s, p, o in g.triples((subject, creator_p, None)):
        author_p = URIRef("http://www.gutenberg.org/2009/pgterms/name")
        s = o
        author = next(g.objects(s, author_p))

        create_wiki_ = URIRef("http://www.gutenberg.org/2009/pgterms/webpage")
        wiki = next(g.objects(s, create_wiki_))

    return author, wiki


def get_file_info(id):
    """ Gather all inforamtion about the book from the RDF file
    """

    rdf = f"{CATALOG_EPUB_URL}/{id}/pg{id}.rdf"

    g = rdflib.Graph()
    g.load(rdf)

    title = get_title(g, id)
    language = get_language(g, id)
    author, wiki = get_author_info(g, id)

    url = get_book_url(g, id)

    info = {
        "title": str(title),
        "author": str(author),
        "author_wiki": str(wiki),
        "language": str(language),
        "weblink": str(url),
        "id": id
    }
    g.close()

    return info


def generate_catalog(root=CATALOG_BASE):
    """ Crawl the root folder for all ebook files and
    cross reference them with the catalog files to create a 
    readable catalog.

    Takes about 22:49min
    """
    data = pd.DataFrame()

    for root, dirs, files in tqdm(os.walk(root)):
        
        # Get he text files for each book generate
        # catalog information
        for f in files:
            if FILE_TYPE in f:
                if "-" in f:
                    # ignore file such as *-8.txt
                    continue
                else:
                    id = f.split('.')[0]

                    link = os.path.join(root, f)
                    try:     
                        # Only consider files which have a integer name                   
                        info = get_file_info(int(id))
                        info["_url"] = link

                        data = pd.concat([data, pd.DataFrame([info])])
                    except Exception as e:                        
                        # print(e)
                        pass

    data.to_csv(f"{CATALOG_BASE}/catalog.csv", sep='\t', index=False)


def initialize_credentials(credentials_file):
    """ Initiatialize the AWS configuration to use
    """

    CFG = {}

    # Read AWS credentials
    credentials = configparser.ConfigParser()
    credentials.read_file(open(credentials_file))

    CFG["KEY"]                    = credentials.get('AWS','KEY')
    CFG["SECRET"]                 = credentials.get('AWS','SECRET')
    CFG["REGION"]                 = credentials.get('AWS','REGION')

    return CFG

def upload_catalog_to_s3():
    """ Upload catalog to S3 bucket
    """
    CFG = initialize_credentials("aws.cfg")

    s3 = boto3.resource('s3', 
                    aws_access_key_id=CFG["KEY"],
                    aws_secret_access_key=CFG["SECRET"],
                    region_name=CFG["REGION"])

    BUCKET = "narrate-data"
    BASE = "gutenberg-data"
    CATALOG_KEY = f"{CATALOG_BASE}/data/catalog.csv"

    try:
        rsp = s3.Object(BUCKET, CATALOG_KEY).upload_file(CATALOG_BASE+"/catalog.csv")
    except Exception as e:
        print(e)
        raise

    print("Done")


def upload_data_to_s3():
    """ Upload Data to S3 bucket. 
    Uses ThreadPoolExecutor to Execute the upload in parallel.
    """
    BUCKET = "narrate-data"
    BASE = "gutenberg-data"

    CFG = initialize_credentials("aws.cfg")

    s3 = boto3.resource('s3', 
                    aws_access_key_id=CFG["KEY"],
                    aws_secret_access_key=CFG["SECRET"],
                    region_name=CFG["REGION"])

    catalog = pd.read_csv("catalog.csv", sep='\t')


    def s3_upload_file(bucket, key, filename):
        print(f"upload {filename} to {bucket}/{key}")
        rsp = s3.Object(bucket, key).upload_file(filename)

    executor = ThreadPoolExecutor()
    futures = []
    for id, row in catalog.iterrows():
        # Remove character . from the link name
        FILENAME = DATA_BASE + '/' + row["_url"]

        link = row["_url"][1:]
        BOOK_KEY = f"{BASE}{link}"

        future = executor.submit(s3_upload_file, BUCKET, BOOK_KEY, FILENAME )
        futures.append(future)

    for future in futures:
        future.result()

    print("Done")


def download_catalog():
    """ Download the latest gutenberg catalog from the website
        and extract all file to the {CATALOG_BASE} folder
    """

    url = "https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.zip"

    if os.path.exists(CATALOG_BASE) is False:
        os.mkdir(CATALOG_BASE)

    elif os.path.exists(f'{CATALOG_BASE}/rdf-files.tar.zip') is False:
        r = requests.get(url)

        with open(f'{CATALOG_BASE}/rdf-files.tar.zip', 'wb') as f:
            f.write(r.content)

    # Unzip the catalog from the archive
    file_zip = zipfile.ZipFile(f'{CATALOG_BASE}/rdf-files.tar.zip')
    file_zip.extractall(CATALOG_BASE)

    # Untar
    file_tar  = tarfile.TarFile(f'{CATALOG_BASE}/rdf-files.tar')
    file_tar.extractall(CATALOG_BASE)

    print("Done!")


def download_data():

    if os.path.exists(DATA_BASE) is False:
        os.mkdir(DATA_BASE)

    cmd = ["rsync", "-av", "--del", "--include", "*.txt", "--include='*/'",
            "--exclude='*'", "aleph.gutenberg.org::gutenberg",
             DATA_BASE]

    print (cmd)
    subprocess.run(cmd, capture_output=True)

    print("Done!")


def argparser():
    """ Command Line parser for the script
    """
    parser = argparse.ArgumentParser(description='Management utility for Gutenberg Project')
    parser.add_argument('--cmd', 
                        type=str,
                        required=True,
                        choices=["create_catalog",
                                 "upload_data", 
                                 "upload_catalog",
                                 "download_catalog",
                                 "download_data",
                                 "test",
                                ]
                        )

    args = parser.parse_args()

    return args


def main():
    """ Main entrypoint for the Gutenberg script
    """
    args = argparser()
    cmd = args.cmd

    if cmd == "create_catalog":
        data = generate_catalog()
    elif cmd == "upload_data":
        upload_data_to_s3()
    elif cmd == "upload_catalog":
        upload_catalog_to_s3()
    elif cmd == "test":
        print(get_file_info(7422))
    elif cmd == "download_catalog":
        download_catalog()
    elif cmd == "download_data":
        download_data()


if __name__ == "__main__":
    main()
