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
import random
from typing import Any, Dict, List
import uuid

from flask import Flask, jsonify
import sqlalchemy  # type: ignore

from google.cloud import storage  # type: ignore

from config import STORAGE_BUCKET, DB_USER, DB_PWD, DB_NAME, CSQL_CONNECTION

STORAGE_CLIENT = storage.Client()

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)


def captcha_dict(image: str, identify: str) -> dict:
    """Converts an image name to a dict as returned by the API

    Args:
        image: the public_url of a blob (thumbnail image) in the GCS bucket
        identify: who to identify (either "jamie" or "alice")

    Returns:
    A dict with these keys, derived from the blob name:
        url: the full public_url
        jamie: whether photo includes Jamie (bool)
        alice: whether photo includes Alice (bool)
    """
    filename: str = image.split("/")[-1].lower()
    return {"url": image, "match": filename.startswith(identify)}


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


def pick_images(candidates):
    """Returns 9 images from a list of image URLs, assuring that the
    returned list includes at least one Jamie and one Alice, and no
    duplicates.
    """
    # First select a random Jamie and a random Alice, so that we have at least
    # one of each.
    jamies = [image for image in candidates if "jamie" in image.lower()]
    alices = [image for image in candidates if "alice" in image.lower()]
    images = [random.choice(jamies), random.choice(alices)]

    # Next we add 7 more random images, without duplicating any selected images.
    while len(images) < 9:
        selection = random.choice(candidates)
        if selection not in images:
            images.append(selection)

    # Shuffle the list and return it.
    random.shuffle(images)
    return images


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
        "INSERT INTO captcha (created_at, identify, captcha_id)"
        " VALUES (:created_at, :identify, :captcha_id)"
    )
    with db_connection.connect() as conn:
        conn.execute(
            stmt,
            created_at=datetime.datetime.utcnow(),
            identify=data["identify"],
            captcha_id=data["captcha_id"],
        )

    pass  # /// insert the thumbnails rows


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
         "identify": "<who to identify in each image; jamie or alice>",
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
    identify = who_to_identify(images)
    image_dicts = [captcha_dict(image, identify) for image in images]
    captcha_id = str(uuid.uuid4())  # unique identifier, 36 characters
    data = {
        "captcha_id": captcha_id,
        "identify": identify,
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
