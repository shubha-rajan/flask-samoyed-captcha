"""Samoyed captcha API

This project was built from this quickstart:
https://cloud.google.com/appengine/docs/standard/python3/quickstart
"""

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import logging
import platform
from pprint import pprint as pp
import random
from typing import Any, Dict, List
import uuid
import base64
import requests

import pdb

from flask import Flask, jsonify, request
import sqlalchemy # type: ignore

from google.cloud import storage  # type: ignore
from google.cloud import automl_v1beta1 as automl # type: ignore

from config import (STORAGE_BUCKET, DB_USER, DB_PWD, DB_NAME, CSQL_CONNECTION,
                    PROJECT_ID, COMPUTE_REGION, MODEL_ID)

STORAGE_CLIENT = storage.Client()
AUTOML_CLIENT = automl.AutoMlClient()
PREDICTION_CLIENT = automl.PredictionServiceClient()

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

def captcha_dict(image: str, label: str) -> dict:
    """Converts an image name to a dict as returned by the API

    Args:
        image: the public_url of a blob (thumbnail image) in the GCS bucket
        label: who to identify (either "jamie" or "alice")

    Returns:
    A dict with these keys, derived from the blob name:
        url: the full public_url
        jamie: whether photo includes Jamie (bool)
        alice: whether photo includes Alice (bool)
    """
    filename: str = image.split("/")[-1].lower()
    return {"url": image, "match": filename.startswith(label)}


def cloudsql_postgres(
    *,
    instance: str,
    username: str,
    password: str,
    database: str,
    driver: str = "postgres+pg8000",
    pool_size: int = 5,
    max_overflow: int = 2,
    pool_timeout: int = 30,
    pool_recycle: int = 1800,
) -> Any:
    """Creates a SQLAlchemy connection for a Cloud SQL Postgres instance.

    Args:
        instance: Cloud SQL instance name (project:region:instance)
        username: database user to connect as
        password: password for username
        database: name of the database within the Cloud SQL instance
        driver: driver name
        poolsize: maximum number of permanent connections
        max_overflow: number of connections to temporarily exceed pool_size
                      if no connections available
        pool_timeout: maximum # seconds to wait for a new connection
        pool_recycle: number of seconds until a connection will be recycled

    Returns:
        A SQLAlchemy connection instance created with create_engine.
        We assume that if this code is running on Windows (for local dev/test)
        then we're connecting to Cloud SQL via the proxy, so need to use
        localhost instead of a Unix socket for the connection.
    """

    if platform.system() == "Windows":
        connection_string = f"{driver}://postgres:{password}@127.0.0.1:5432/{database}"
    else:
        # If not Windows, we assume a Linux-compatible OS.
        unix_socket: Dict[str, str] = {
            "unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(instance)
        }
        connection_string = sqlalchemy.engine.url.URL(
            drivername=driver,
            username=username,
            password=password,
            database=database,
            query=unix_socket,
        )

    return sqlalchemy.create_engine(
        connection_string,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_recycle=pool_recycle,
    )


def list_blobs(bucket_name: str, delimiter: str = "/") -> List[str]:
    """Returns the urls of all blobs in a bucket.

    Args:
        bucket_name: name of GCS bucket
        delimiter: optional delimiter for storage API call

    Returns:
        List of the public_url values for all blobs in the bucket.
    """
    blobs = STORAGE_CLIENT.list_blobs(bucket_name, delimiter=delimiter)  # type: ignore
    return [blob.public_url for blob in blobs]


def pick_images(candidates) -> set:
    """Returns 9 images from a list of image URLs, assuring that the
    returned list includes at least one Jamie and one Alice, and no
    duplicates.
    """
    # First select a random Jamie and a random Alice, so that we have at least
    # one of each.
    jamies = [image for image in candidates if url_to_label(image) == "jamie"]
    alices = [image for image in candidates if url_to_label(image) == "alice"]
    images = set([random.choice(jamies), random.choice(alices)])

    # Next we add 7 more random images, without duplicating any selected images.
    while len(images) < 9:
        images.add(random.choice(candidates))

    # Shuffle the list and return it.
    image_list = list(images)
    random.shuffle(image_list)
    return images


@app.route('/response/<captcha_id>', methods = ['POST'])
def response_handler(captcha_id):
    """Save a user's response to the captcha.

    The data structure POSTed to this endpoint is 9 booleans, each
    indicating whether the user correctly identified the corresponding
    image from the captcha.
    """
    #data = request.form
    data = request.get_json(force=True)

    # create database connection
    db_connection = cloudsql_postgres(
        instance=CSQL_CONNECTION, username=DB_USER, password=DB_PWD, database=DB_NAME
    )

    # save the responses for the individual images
    for image_no in range(1, 10):
        success = data[f"image{image_no}"]
        public_url = get_public_url(captcha_id, image_no, db_connection)
        save_response(captcha_id, public_url, success, db_connection)
        pass

    captcha_handled(captcha_id, db_connection) # set submitted_at

    return f"Here is the data object we received:\n{data}"

def get_public_url(captcha_id, image_no, db_connection):
    """Get the public_url associated with a captcha_id and image_no.
    """
    result = db_connection.execute(
        f'SELECT public_url FROM thumbnail WHERE captcha_id = "{captcha_id}" AND image_no = {image_no}')
    for row in result:
        public_url = row["public_url"]
    db_connection.close()

    return public_url


def save_response(captcha_id, public_url, success, db_connection):
    """Inserts a record in the responses table.
    """
    stmt = sqlalchemy.text(
        "INSERT INTO responses (captcha_id, public_url, label, success)"
        " VALUES (:captcha_id, :public_url, :label, :success)"
    )
    with db_connection.connect() as conn:
        conn.execute(
            stmt,
            captcha_id=captcha_id,
            public_url=public_url,
            label=url_to_label(public_url),
            success=success,
        )


def captcha_handled(captcha_id, db_connection):
    # update captcha.submitted_at for this captcha_id
    stmt = sqlalchemy.text(
        "UPDATE captcha SET submitted_at = :submitted_at WHERE captcha_id = :captcha_id"
    )
    with db_connection.connect() as conn:
        conn.execute(
            stmt,
            submitted_at=datetime.datetime.utcnow(),
            captcha_id=captcha_id,
        )


def save_captcha(data: dict) -> None:
    """Saves a captcha response to the database.

    Args:
        data: a dict returned by captcha_api()

    Returns:
        None. The data is stored in the captcha and thumbnail tables.
    """

    # create database connection
    db_connection = cloudsql_postgres(
        instance=CSQL_CONNECTION, username=DB_USER, password=DB_PWD, database=DB_NAME
    )

    # insert the captcha row
    stmt = sqlalchemy.text(
        "INSERT INTO captcha (created_at, label, captcha_id)"
        " VALUES (:created_at, :label, :captcha_id)"
    )
    with db_connection.connect() as conn:
        conn.execute(
            stmt,
            created_at=datetime.datetime.utcnow(),
            label=data["label"],
            captcha_id=data["captcha_id"],
        )

    # insert the thumbnails rows
    stmt = sqlalchemy.text(
        "INSERT INTO thumbnail (public_url, image_no, captcha_id, label)"
        " VALUES (:public_url, :image_no, :captcha_id, :label)"
    )
    for image_no in range(1, 10):
        image_dict = data[f"image{image_no}"]
        with db_connection.connect() as conn:
            conn.execute(
                stmt,
                public_url=image_dict["url"],
                image_no=image_no,
                captcha_id=data["captcha_id"],
                label=url_to_label(image_dict["url"]),
            )


def get_prediction_from_db(url: str):
    """ Retrieves data from prediction table
        
        Args:
            url: a string representing the public url of a blob

        Returns:
            Dict. A dict containing the labels as keys and the confidence for 
            each label as values.
    """

    # create database connection
    db_connection = cloudsql_postgres(
        instance=CSQL_CONNECTION, username=DB_USER, password=DB_PWD, database=DB_NAME
    )

    stmt = sqlalchemy.text(
        "SELECT (jamie, alice)"
        " FROM predictions WHERE public_url = :url"
        " LIMIT 1"
    )

    with db_connection.connect() as conn:
        result = conn.execute(stmt, url=url)

    if result.rowcount == 0:
        return None
    else:
        return ({
            "url": url,
            "jamie": result.rows[0]['jamie'], 
            "alice": result.rows[0]['alice']
        })


def get_prediction_from_api(url: str):
    """ Retrieves data from prediction table
        
        Args:
            url: a string representing the public url of a blob

        Returns:
            Dict. A dict containing the labels as keys and the confidence for 
            each label as values.
    """

    img_bytes = requests.get(url).content
    
    payload = {"image": {"image_bytes": img_bytes}}
    params = { "score_threshold": "0.0" }

    model_full_id = AUTOML_CLIENT.model_path(
        PROJECT_ID, COMPUTE_REGION, MODEL_ID
    )
    result = {}
    response = PREDICTION_CLIENT.predict(model_full_id, payload, params)
    for label in response.payload:
        result[label.display_name] = label.classification.score

    return ({
        "url": url,
        "jamie": result['jamie'],
        "alice": result['alice'],
    })

def save_prediction(result: dict):
    """ Retrieves data from prediction table
        
        Args:
            result: a dict which maps labels to confidence scores:
            {
                "jamie': <float representing confidence score>,
                "alice": <float representing confidence score>
            }


        Returns:
            None. Writes data to prediction table
    """

    db_connection = cloudsql_postgres(
        instance=CSQL_CONNECTION, username=DB_USER, password=DB_PWD, database=DB_NAME
    )

    
    url = result['url']
    jamie = result['jamie']
    alice = result['alice']
    stmt = sqlalchemy.text(
        "INSERT INTO predictions (label, public_url, jamie, alice)"
        " VALUES (:label, :url, :jamie, :alice)"
    )

    with db_connection.connect() as conn:
        conn.execute(
            stmt,label=url_to_label(url), url=url, jamie=jamie, alice=alice
        )


def thumbnail_name(blobname: str) -> bool:
    """Returns True if the blobname is a valid thumbnail image name

    Args:
        blobname: the name of a GCS blob
    Returns:
        True if the name is a .jpg that starts with "jamie" or "alice",
        False otherwise.
    """
    filename: str = blobname.split("/")[-1].lower()
    return filename.endswith(".jpg") and (
        filename.startswith("jamie") or filename.startswith("alice")
    )


def url_to_label(public_url: str) -> str:
    """Determines the label ("jamie" or "alice" from a GCS public_url).
    Note that we assume the naming scheme of this project, with all images
    named either jamieNNN.jpg or aliceNNN.jpg.
    """
    return public_url.split("/")[-1][:5].lower()


def who_to_identify(images: List[str]) -> str:
    """Determines who should be identified by the user in a list images.

    Args:
        images: a list of public_url values for 9 thumbnail images.
        Each public_url's blobname should begin with "jamie" or "alice".

    Returns:
        The most common type of image as a string - either "jamie" or "alice".
    """
    jamie_count = len(
        [image for image in images if image.split("/")[-1].lower().startswith("jamie")]
    )
    alice_count = len(
        [image for image in images if image.split("/")[-1].lower().startswith("alice")]
    )
    return "jamie" if jamie_count > alice_count else "alice"

@app.route("/predict", methods=["POST"])  # type: ignore
def return_prediction() -> Dict:
    """Route handler for the API.

    Args:
        None (decorated as a Flask route)

    Returns:
        JSON serialization of a dict that maps labels to confidence scores:
        {
            "jamie': "70.96",
            "alice": "29.04"
        }
    """
    url = request.get_json(force=True).get('url')
    result = get_prediction_from_db(url)
    if result:
        return result

    result = get_prediction_from_api(url)

    save_prediction(result)

    resp = jsonify(result)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers['Content-Type'] = 'application/json'
    resp.headers['Access-Control-Allow-Methods'] = 'POST'

    return resp

@app.route("/", methods=["GET"])  # type: ignore
@app.route("/captcha", methods=["GET"])  # type: ignore
def captcha_api() -> Any:
    """Route handler for the API.

    Preferred useage is /captcha endpoint, but we include the root / here
    for quick manual testing.

    Args:
        None (decorated as a Flask route)

    Returns:
        JSON serialization of a random selection of 9 captcha images,
        as a dict with this structure:
        {"captcha_id": "<unique id for this response>",
         "label": "<who to identify in each image; jamie or alice>",
         "image1": {"url": "<public_url of image>", "match": <bool>},
         "image2": {"url": "<public_url of image>", "match": <bool>},
         "image3": {"url": "<public_url of image>", "match": <bool>},
         "image4": {"url": "<public_url of image>", "match": <bool>},
         "image5": {"url": "<public_url of image>", "match": <bool>},
         "image6": {"url": "<public_url of image>", "match": <bool>},
         "image7": {"url": "<public_url of image>", "match": <bool>},
         "image8": {"url": "<public_url of image>", "match": <bool>},
         "image9": {"url": "<public_url of image>", "match": <bool>},
        }
    """

    # create the data structure to be returned
    blobs = list_blobs(STORAGE_BUCKET)
    thumbnails = [blob for blob in blobs if thumbnail_name(blob)]
    images = pick_images(thumbnails)  # 9 random thumbnails
    label = who_to_identify(images)
    image_dicts = [captcha_dict(image, label) for image in images]
    captcha_id = str(uuid.uuid4())  # unique identifier, 36 characters
    data = {
        "captcha_id": captcha_id,
        "label": label,
        "image1": image_dicts[0],
        "image2": image_dicts[1],
        "image3": image_dicts[2],
        "image4": image_dicts[3],
        "image5": image_dicts[4],
        "image6": image_dicts[5],
        "image7": image_dicts[6],
        "image8": image_dicts[7],
        "image9": image_dicts[8],
    }

    save_captcha(data)  # save to database

    resp = jsonify(data)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.errorhandler(500)
def server_error(e):  # type: ignore
    # Log the error and stacktrace.
    logging.exception("An error occurred during a request.")
    return "An internal error occurred.", 500


if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
