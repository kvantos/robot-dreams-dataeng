#!/usr/bin/env python3

from flask import Flask
from flask import typing as flask_typing
from flask import request as flask_request
import api
import storage

app = Flask(__name__)


def is_blank(string):
    if string:
        return False
    return True


@app.route("/apiworker", methods=['POST'])
def api_worker() -> flask_typing.ResponseReturnValue:
    try:
        input_data: dict = flask_request.json
    except Exception as error_message:
        return {'message': f'{error_message}'}, 400

    if is_blank(input_data):
        return {'message': 'Input .json is empty'}, 400

    try:
        api.get_sales(input_data['date'], input_data['raw_dir'])
    except Exception as error_message:
        return {'message': f'Failed to fetch data: {error_message}'}, 422

    return {'message': 'Successfully fetched data from the API'}, 201


@app.route("/storageworker", methods=["POST"])
def storage_worker() -> flask_typing.ResponseReturnValue:
    try:
        input_data: dict = flask_request.json
    except Exception as error_message:
        return {"message": f"{error_message}"}, 400

    if is_blank(input_data):
        return {"message": "Input .json is empty"}, 400

    if 'stg_dir' not in input_data or 'raw_dir' not in input_data:
        return {"message": "Incorrect .json configuration"}, 422

    try:
        storage.save_to_disk(input_data["raw_dir"], input_data["stg_dir"])
    except Exception as error_message:
        return {"message": f"{error_message}"}, 422

    return {"message": "Successfully processed raw data."}, 201


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8081, debug=True)
