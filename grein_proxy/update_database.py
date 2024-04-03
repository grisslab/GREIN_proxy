"""Script used to download the complete GREIN datasets and store them in a SQLITE database
"""

import logging
import click
import sqlite3
import os
import sys
import grein_loader
import pickle
import requests
from progressbar import progressbar


_LOGGER = logging.getLogger(__name__)


def create_database(database: str) -> None:
    """Create a new SQLITE database with the respective schema
       and save it at the required path. Note: This file must
       not yet exist.

    :param database: Path to the new file to be created
    :type database: str
    """
    _LOGGER.info(f"Creating new database at {database}")

    if os.path.isfile(database):
        raise Exception("New database file must not exist.")
    
    # connect to the database
    con = sqlite3.connect(database=database)

    # get the cursor to create the schema
    cur = con.cursor()

    cur.execute("CREATE TABLE dataset(accession TEXT PRIMARY KEY, status INTEGER, title TEXT, species TEXT, metadata BLOB, raw_counts BLOB, normalised_counts BLOB)")

    # save the changes
    con.commit()
    con.close()


def get_loaded_datasets(connection: sqlite3.Connection) -> set:
    """Retrieves the accession numbers of all datasets already present in the
       database

    :param connection: The sqlite3 connection to sue
    :type connection: sqlite3.Connection
    :return: All access numbers as a set.
    :rtype: set
    """
    cur = connection.cursor()

    res = cur.execute("SELECT accession FROM dataset")

    entries = set(res.fetchall())

    accession_numbers = set([entry[0] for entry in entries])

    cur.close()

    return accession_numbers


def load_datasets(geo_accessions: list, connection: sqlite3.Connection) -> None:
    """Load the defined datasets from GREIN and store them in the database

    :param geo_accessions: The GEO accessions to load from GREIN
    :type geo_accessions: list
    :param connection: The connection to the sqlite database
    :type connection: sqlite3.Connection
    """
    cur = connection.cursor()
    
    # create a nice progress bar
    for i in progressbar(range(len(geo_accessions))):
        accession = geo_accessions[i]

        # fetch the data from GREIN
        _LOGGER.debug(f"Loading dataset {accession} from GREIN")

        try:
            description, metadata, raw_counts = grein_loader.load_dataset(accession)

            # prepare the data for the database
            binary_meta = pickle.dumps(metadata)
            binary_raw_counts = pickle.dumps(raw_counts)

            data = [
                accession, 1, description["Title"], description["Species"], binary_meta, binary_raw_counts
            ]

            _LOGGER.debug(f"Saving {accession} into database")
            cur.execute("INSERT INTO dataset(accession, status, title, species, metadata, raw_counts) VALUES(?, ?, ?, ?, ?, ?)", data)
        except (requests.exceptions.HTTPError, grein_loader.exceptions.GreinLoaderException) as e:
            _LOGGER.error("Failed to load dataset from GREIN", e)

            # mark the dataset as not available
            cur.execute("INSERT INTO dataset(accession, status) VALUES(?, 0)", [accession])

        # commit after each dataset
        connection.commit()


@click.command()
@click.option("--database", "-d", required=True, type=str, help="Path to the sqlite file to use as a database.")
@click.option("--max_datasets", "-m", required=False, type=int, help="The maximum number of datasets to load from GREIN.", default=1000000)
def main(database, max_datasets):
    """Function to download / update the GREIN repository
    """
    # set up logging
    logging.basicConfig(level=logging.DEBUG, filename="grein_proxy_update_database.log")
    logging.getLogger("urllib3").setLevel(level=logging.INFO)
    logging.getLogger("gein_loader").setLevel(level=logging.INFO)

    # create a new database if it does not yet exist
    if not os.path.isfile(database):
        create_database(database)

    # open the database
    con = sqlite3.connect(database=database)

    # get all already loaded datasets
    loaded_datasets = get_loaded_datasets(con)
    print(f"{len(loaded_datasets)} datasets available in database.")

    # get all GREIN datasets
    # TODO: remove limit after debug
    grein_datasets = grein_loader.load_overview(no_datasets = max_datasets)
    grein_accessions = set([dataset["geo_accession"] for dataset in grein_datasets])

    print(f"{len(grein_datasets)} datasets available in GREIN.")

    # define the datasets to load
    datasets_to_load = list(grein_accessions - loaded_datasets)
    print(f"{len(datasets_to_load)} new datasets to load")

    # load the new datasets
    if len(datasets_to_load) > 0:
        load_datasets(datasets_to_load, con)
    else:
        print("Database up to date.")

    con.close()

if __name__ == "__main__":
    main()
