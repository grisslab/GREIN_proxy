import os
import logging
import sqlite3
import pickle
from flask import Flask, g, current_app, abort, request, Response


_LOGGER = logging.getLogger(__name__)


def get_db():
    """Retruns a connection to the SQLITE database

    :return: _description_
    :rtype: _type_
    """
    if "db" not in g:
        # make sure the database already exists
        db_path = current_app.config["DATABASE"]

        if not os.path.isfile(db_path):
            _LOGGER.error(f"Failed to find database at {db_path}")
            abort(500, "Failed to connecto the database")

        g.db = sqlite3.connect(
            db_path,
            detect_types=sqlite3.PARSE_DECLTYPES
        )

        # parse rows into dicts
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(e=None):
    """Close the connection to the database

    :param e: Parameter not used, defaults to None
    :type e: _type_, optional
    """
    db = g.pop("db", None)

    if db is not None:
        db.close()


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    # close the DB connection at the end
    app.teardown_appcontext(close_db)
    app.config.from_mapping(
        DATABASE=os.environ.get("GREIN_DB", "/tmp/test.db"),
    )
    
    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    @app.route('/<accession>/status', methods=["GET"])
    def status(accession=None):
        if not accession:
            abort(400, "Missing required parameter 'accession'")

        # get the database
        db = get_db()
        cur = db.cursor()

        # prepare the return status
        return_obj = {"accession": accession}

        res = cur.execute("SELECT status, title, species FROM dataset WHERE accession = ?", [accession])
        entries = res.fetchall()

        if len(entries) < 1:
            return_obj["status"] = "Unknown"

        if len(entries) == 1:
            return_obj.update(entries[0])

        # this should never happen
        if len(entries) > 1:
            _LOGGER.error(f"Duplicate entries for {accession}")
            abort(500, f"Duplicate entries for {accession}")

        return(return_obj)
    
    @app.route('/<accession>/metadata.json', methods=["GET"])
    def metadata(accession=None):
        if not accession:
            abort(400, "Missing required parameter 'accession'")

        # get the database
        db = get_db()
        cur = db.cursor()
    
        res = cur.execute("SELECT status, metadata FROM dataset WHERE accession = ?", [accession])
        entries = res.fetchall()

        if len(entries) < 1:
            abort(404, "Unknown identifier passed")

        # this should never happen
        if len(entries) > 1:
            _LOGGER.error(f"Duplicate entries for {accession}")
            abort(500, f"Duplicate entries for {accession}")
        
        if entries[0]["status"] != 1:
            abort(500, "Database not available in GREIN")

        metadata = entries[0]["metadata"]

        return(pickle.loads(metadata))
    
    @app.route('/<accession>/raw_counts.tsv', methods=["GET"])
    def raw_counts(accession=None):
        if not accession:
            abort(400, "Missing required parameter 'accession'")

        # get the database
        db = get_db()
        cur = db.cursor()
    
        res = cur.execute("SELECT status, raw_counts FROM dataset WHERE accession = ?", [accession])
        entries = res.fetchall()

        if len(entries) < 1:
            abort(404, "Unknown identifier passed")

        # this should never happen
        if len(entries) > 1:
            _LOGGER.error(f"Duplicate entries for {accession}")
            abort(500, f"Duplicate entries for {accession}")

        if entries[0]["status"] != 1:
            abort(500, "Database not available in GREIN")

        raw_counts = pickle.loads(entries[0]["raw_counts"])

        # create the response
        response = Response(raw_counts.to_csv(sep="\t", index=False))
        response.headers["Content-Type"] = "text/csv"
        response.headers["Content-Disposition"] = f"attachement; filename={accession}.tsv"

        return(response)

    return app