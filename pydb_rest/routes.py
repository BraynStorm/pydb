from configparser import RawConfigParser
from pathlib import Path
from typing import List
from pydb import db
from pydb_rest.app import app

from flask import request
import base64


@app.route("/api/v1/find/<string:fields>/<string:mode>/<string:code>", methods=["GET"])
def api_find(raw_fields: str, raw_mode: str, raw_code: str):
    raw_fields = raw_fields.translate({ord(" "): "-"}).encode("ascii").decode("ascii")

    fields: List[str] = raw_fields.split(",")

    # TODO:
    #  Ensure this is safe.
    assert all(
        ("/" not in field and "\\" not in field and "." not in field)
        for field in fields
    )

    mode = db.FindMode[raw_mode]
    pydb = db.PyDB(Path("data"))

    code = base64.b64decode(raw_code)
    compile(code, "requested_source", mode="compile")
    pydb.find_keys()
