"""Runs the proxy serving the respective web interface
"""

import click
import os
import sys
import logging
from waitress import serve


from . import logo
from . import flask_app


_LOGGER = logging.getLogger(__name__)


def setup_logging(log_file: str, log_level: str):
    """Setup the logging for the application

    :param log_file: The file to log to. If None, the console is used. 
    :type log_file: str
    :param log_level: The log level as a string.
    :type log_level: str
    """
    # show the logo first :-)
    logo.print_logo()

    # get the root logger
    rootLogger = logging.getLogger()

    # set the level
    rootLogger.setLevel(log_level)

    # set the handler
    if log_file:
        handler = logging.FileHandler(filename=log_file)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s')
    else:
        handler = logging.StreamHandler(stream=sys.stdout)
        formatter = logging.Formatter('%(levelname)s: %(message)s')

    handler.setFormatter(formatter)
    rootLogger.addHandler(handler)


@click.command()
@click.option("--database", "-d", required=True, type=str, help="Path to the sqlite file to use as a database.")
@click.option("--port", "-p", required=False, type=int, help="Port to launch the webserver on.", default=80, show_default=True)
@click.option("--log_file", "-l", required=False, type=str, help="If set, the logfile will be written to this location. Otherwise the logging output is written to the command line.")
@click.option("--log_level", "-e", required=False, type=str, help="Log level to use for logging", default="INFO", show_default=True)
def main(database: str, port: int=80, log_file: str=None, log_level: str="info"):
    """Launches the web server to process requests
    """
    # set up the logging
    setup_logging(log_file, log_level=log_level)

    # ensure that the database exists
    if not os.path.isfile(database):
        _LOGGER.error("Failed to find database file at %s", database)
        sys.exit(1)

    # set the database location
    os.environ["GREIN_DB"] = database

    # create the flask app
    _LOGGER.debug("Creating Flask app")
    app = flask_app.create_app()

    # start serving the application
    _LOGGER.info("Serving GREIN Proxy at port %d", port)
    serve(app, port=port) # host = host

if __name__ == "__main__":
    main()
