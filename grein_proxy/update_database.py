"""Script used to download the complete GREIN datasets and store them in a SQLITE database
"""

import logging
import click
import sqlite3
import os
import time
import grein_loader
import pickle
import requests
import urllib3
import progressbar
import concurrent.futures
import typing

from . import logo


_LOGGER = logging.getLogger(__name__)


def print_logo():
    """Print the applications logo to the
       command line
    """
    logo.print_logo()
    
    print("""
                        +-------------------+
                        |  DATABASE UPDATE  |
                        +-------------------+
          
    """)

def load_grein_dataset_with_timeout(accession: str, timeout: int):
    """Execute the respective function and return its result. If it does
       not complete withing the defined timeout, an Exception is thrown.

    :param The accession of the dataset to load
    :param timeout: The timeout in seconds after which to kill the call
    :type timeout: int
    :return: The description, metadata, raw_counts of the dataset
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(grein_loader.load_dataset, accession)

        try:
            # Attempt to get the result within the specified timeout
            _LOGGER.debug(f"Fetching dataset from GREIN within a maximum of {timeout} seconds")
            description, metadata, raw_counts = future.result(timeout=timeout)
            _LOGGER.debug("Data received")
            
            return (description, metadata, raw_counts)
        except concurrent.futures.TimeoutError as e:
            _LOGGER.error(f"Timeout of {timeout} seconds reached. Killing call")
            raise e


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


def load_single_dataset(geo_accession: str, retry_delay: int, timeout: int, max_retries: int=5) -> typing.Tuple:
    """_summary_

    :param geo_accessions: The GEO accessions to load from GREIN
    :type geo_accessions: list
    :param connection: The connection to the sqlite database
    :type connection: sqlite3.Connection
    :param retry_delay: Seconds to wait before a failed request is repeated.
    :type retry_delay: int
    :param timeout: Timeout in seconds after which a loading call to GREIN is killed.
    :type timeout: int  
    :param max_retries: Maximum number of times a dataset is tried to be loaded, defaults to 5
    :type max_retries: int, optional
    :return: The dataset as returned by the GREIN loader plugin. Returns None if the dataset cannot be loaded from GREIN.
             Throws an exception if the maximum number of retries is reached.
    :rtype: typing.Tuple
    """
    current_retry = 0

    while True:
        try:
            grein_data = load_grein_dataset_with_timeout(accession=geo_accession, timeout=timeout)

            return grein_data
        except (requests.exceptions.HTTPError, grein_loader.exceptions.GreinLoaderException) as e:
            _LOGGER.error("Failed to load %s from GREIN. Dataset may not be available", geo_accession)

            return None
        except (requests.exceptions.ConnectionError, concurrent.futures.TimeoutError) as e:
            current_retry += 1

            if current_retry >= max_retries:
                _LOGGER.error("Failed to load GREIN adataset %s. Max retries exceeded.", geo_accession)
                raise Exception("Failed to load GREIN dataset. Max retries exceeded")

            _LOGGER.error("Connection to GREIN failed - Retrying after %d seconds (retry %d / %d)...", timeout, current_retry, max_retries)

            # repeat the request after a 30 sec delay
            time.sleep(retry_delay)

            # try to reset the urllib3 pool manager - although it is unclear whether
            # this has an effect
            urllib3.PoolManager().clear()


def load_datasets(geo_accessions: list, connection: sqlite3.Connection, retry_delay: int, timeout: int, n_threads: int=1) -> None:
    """Load the defined datasets from GREIN and store them in the database

    :param geo_accessions: The GEO accessions to load from GREIN
    :type geo_accessions: list
    :param connection: The connection to the sqlite database
    :type connection: sqlite3.Connection
    :param retry_delay: Seconds to wait before a failed request is repeated.
    :type retry_delay: int
    :param timeout: Timeout in seconds after which a loading call to GREIN is killed.
    :type timeout: int
    :param n_threads: Number of threads to use to fetch the data. Optional, default = 1
    :type n_threads: int
    """
    # create a nice progress bar
    bar = progressbar.ProgressBar(min_value=0, max_value=len(geo_accessions), redirect_stdout=True)

    # call next to show the progress bar
    bar.next()

    # submit all jobs to a queue
    with concurrent.futures.ProcessPoolExecutor(max_workers=n_threads) as executor:
        # submit all tasks
        future_to_accession = {executor.submit(load_single_dataset, geo_accession=accession, retry_delay=retry_delay, timeout=timeout, max_retries=10): accession for accession in geo_accessions}

        # process each task as soon as it is completed
        for future in concurrent.futures.as_completed(future_to_accession):
            accession = future_to_accession[future]

            try:
                grein_data = future.result()

                # check if the dataset is not available
                if grein_data is None:
                    connection.execute("INSERT INTO dataset(accession, status) VALUES(?, 0)", [accession])
                else:
                    description = grein_data[0]
                    binary_meta = pickle.dumps(grein_data[1])
                    binary_raw_counts = pickle.dumps(grein_data[2])

                    data = [
                        accession, 1, description["Title"], description["Species"], binary_meta, binary_raw_counts
                    ]

                    _LOGGER.debug("Saving %s into database", accession)
            
                    connection.execute("INSERT INTO dataset(accession, status, title, species, metadata, raw_counts) VALUES(?, ?, ?, ?, ?, ?)", data)

                # commit after each dataset
                connection.commit()
            except Exception as e:
                _LOGGER.error("Failed to load %s from GREIN", accession)

            bar.next()

    bar.finish()

@click.command()
@click.option("--database", "-d", required=True, type=str, help="Path to the sqlite file to use as a database.")
@click.option("--max_datasets", "-m", required=False, type=int, help="The maximum number of datasets to load from GREIN.", default=1000000)
@click.option("--retry_delay", "-r", required=False, type=int, help="Seconds to wait before a request is repeated to GREIN if GREIN fails.", default = 30, show_default = True)
@click.option("--timeout", "-t", required=False, type=int, help="Timeout after which the GREIN loading function is killed.", default=60, show_default = True)
@click.option("--threads", "-t", required=False, type=int, help="Number of parallel threads to use to retrieve datasets.", default=1, show_default=True)
def main(database, max_datasets, retry_delay, timeout, threads):
    """Function to download / update the GREIN repository
    """
    # set up logging
    logging.basicConfig(level=logging.DEBUG, filename="grein_proxy_update_database.log")
    logging.getLogger("urllib3").setLevel(level=logging.INFO)
    logging.getLogger("grein_loader").setLevel(level=logging.INFO)

    # print the logo
    print_logo()

    # create a new database if it does not yet exist
    if not os.path.isfile(database):
        create_database(database)

    # open the database
    con = sqlite3.connect(database=database)

    # get all already loaded datasets
    loaded_datasets = get_loaded_datasets(con)
    print(f"Datasets in database: {len(loaded_datasets)}")

    # get all GREIN datasets
    # TODO: remove limit after debug
    grein_datasets = grein_loader.load_overview(no_datasets = max_datasets)
    grein_accessions = set([dataset["geo_accession"] for dataset in grein_datasets])

    print(f"Datasets in GREIN:    {len(grein_datasets)}")
 
    # define the datasets to load
    datasets_to_load = list(grein_accessions - loaded_datasets)
    print(f"Datasets to load:     {len(datasets_to_load)}")

    # load the new datasets
    if len(datasets_to_load) > 0:
        _LOGGER.info(f"Starting download of {len(datasets_to_load)} datasets")
        print("\n       >>>>>   Starting update   <<<<<\n\n")
        load_datasets(datasets_to_load, con, retry_delay, timeout, n_threads=threads)
    else:
        print("\n       >>>>>   Database up to date.   <<<<<\n\n")

    con.close()

if __name__ == "__main__":
    main()
