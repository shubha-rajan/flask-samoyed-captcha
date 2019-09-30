"""
Simple test of Samoyed-Captcha API.
"""
from pprint import pprint
import random

import requests

from util import cloudsql_postgres

# root endpoint for deployed API on App Engine
# API_BASE = "https://jamie-alice-classifier-251416.appspot.com"
API_BASE = "http://127.0.0.1:8080"


def get_image_no(captcha_id, public_url, db_connection):
    """
    """
    result = db_connection.execute(
        f"SELECT * FROM thumbnail WHERE captcha_id = '{captcha_id}' AND public_url = '{public_url}'"
    )
    row = result.fetchone()
    return row["image_no"]


def main():
    """Main test script:
    - creates a captcha (with a GET to /captcha)
    - POSTs a response to /respone/<captcha_id>
    - verifies data structure and database updates
    """

    # get a new random captcha
    response = requests.get(f"{API_BASE}/captcha")
    print(f"/captcha API response: {response}")
    captcha = response.json()
    verify_captcha_structure(captcha)

    db_connection = cloudsql_postgres()  # database connection

    # verify the new captcha was stored correctly in the database
    verify_captcha_record(captcha, db_connection)
    verify_captcha_thumbnails(captcha, db_connection)

    captcha_id = captcha["captcha_id"]

    # POST random user response to the captcha
    post_url = f"{API_BASE}/response/{captcha_id}"
    response_data = {
        "image1": random.randint(0, 1) == True,
        "image2": random.randint(0, 1) == True,
        "image3": random.randint(0, 1) == True,
        "image4": random.randint(0, 1) == True,
        "image5": random.randint(0, 1) == True,
        "image6": random.randint(0, 1) == True,
        "image7": random.randint(0, 1) == True,
        "image8": random.randint(0, 1) == True,
        "image9": random.randint(0, 1) == True,
    }
    post_response = requests.post(post_url, json=response_data)
    print(f"/response API response: {post_response}")

    # verify that the response was stored correctly in the database
    verify_response(captcha, response_data, db_connection)


def verify_captcha_record(captcha, db_connection):
    """Verify the presence and contents of record inserted into captcha table.
    """
    captcha_id = captcha["captcha_id"]
    result = db_connection.execute(
        f"SELECT * FROM captcha WHERE captcha_id = '{captcha_id}'"
    )
    row = result.fetchone()
    assert row["captcha_id"] == captcha_id
    assert row["label"] == captcha["label"]


def verify_captcha_structure(captcha):
    """Verifies the structure of the dictionary returned by /captcha.
    """
    for key in [
        "captcha_id",
        "image1",
        "image2",
        "image3",
        "image4",
        "image5",
        "image6",
        "image7",
        "image8",
        "image9",
    ]:
        assert key in captcha.keys()
        assert captcha[key]


def verify_captcha_thumbnails(captcha, db_connection):
    """Verify the presence and contents of records inserted into thumbnail table.
    """
    captcha_id = captcha["captcha_id"]
    result = db_connection.execute(
        f"SELECT * FROM thumbnail WHERE captcha_id = '{captcha_id}'"
    )
    for row in result:
        image_key = f"image{row['image_no']}"
        assert row["public_url"] == captcha[image_key]["url"]
        if captcha[image_key]["match"]:
            assert row["label"] == captcha["label"]
        else:
            assert row["label"] != captcha["label"]


def verify_response(captcha, response_data, db_connection):
    """Verify the presence and contents of records inserted into responses table.
    """
    captcha_id = captcha["captcha_id"]
    result = db_connection.execute(
        f"SELECT * FROM responses WHERE captcha_id = '{captcha_id}'"
    )
    for row in result:
        image_no = get_image_no(captcha_id, row["public_url"], db_connection)
        image_key = f"image{image_no}"
        assert row["public_url"] == captcha[image_key]["url"]
        assert row["success"] == response_data[image_key]
        if captcha[image_key]["match"]:
            assert row["label"] == captcha["label"]
        else:
            assert row["label"] != captcha["label"]


if __name__ == "__main__":
    main()
