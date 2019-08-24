import argparse
import logging
import os
from zipfile import ZipFile

import pandas as pd
import requests
from lxml import etree

from scraper.config import DOC_URL, MAIN_URL

parser = argparse.ArgumentParser(description="Download some files.")
parser.add_argument(
    "--years",
    metavar="2019",
    default="2019",
    type=str,
    help="Comma separated list of years",
)

console = logging.StreamHandler()
formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
console.setFormatter(formatter)

log = logging.getLogger("scraper")
log.setLevel(level=logging.DEBUG)
log.addHandler(console)


def parse_xml(xml_bytes):
    root = etree.fromstring(xml_bytes)
    members = root.xpath("//Member")
    objs = []
    for member in members:
        t = {}
        for e in member:
            t[e.tag] = e.text
        objs.append(t)
    df = pd.DataFrame.from_records(objs)
    return df


def download_document(doc_id, year):
    try:
        log.info(f"Downloading {year} - {doc_id}")
        resp = requests.get(DOC_URL.format(year, doc_id))
        return resp.content
    except Exception as e:
        log.error("Uncaught exception", exc_info=e)
        return None


def download_documents(df, base_dir):
    df["StateDst"].fillna("ZZ00", inplace=True)
    for (ind, row) in df.iterrows():
        try:
            state = row["StateDst"][:2]
            year = row["Year"]
            os.makedirs(f"{base_dir}/{state}", exist_ok=True)
            if os.path.exists(f"{base_dir}/{state}/{row['DocID']}.pdf"):
                continue
            check_1 = download_document(row["DocID"], year)
            if check_1 is not None:
                with open(f"{base_dir}/{state}/{row['DocID']}.pdf", "wb") as f:
                    f.write(check_1)
            else:
                log.info("Failed to download")
        except Exception as e:
            log.error("Uncaught exception", exc_info=e)


def download_year(year):
    log.info(f"Downloading {year}")
    base_dir = f"./saves/{year}"
    os.makedirs(base_dir, exist_ok=True)
    try:
        resp = requests.get(MAIN_URL.format(year))
        save_path = f"{base_dir}/{year}FD.zip"
        with open(save_path, "wb") as f:
            f.write(resp.content)
        with ZipFile(save_path, "r") as my_zip:
            xml_path = [x for x in my_zip.namelist() if ".xml" in x]
            with my_zip.open(xml_path[0]) as xml_file:
                index = parse_xml(xml_file.read())
                log.info("{} entries to download".format(len(index)))
                index.to_csv(f"{base_dir}/index.csv", index=False)
                download_documents(index, base_dir)
    except Exception as e:
        log.error("Uncaught exception", exc_info=e)


if __name__ == "__main__":
    args = parser.parse_args()
    print(args)
    if args.years:
        years = args.years.split(",")
        download_year(years[0])
