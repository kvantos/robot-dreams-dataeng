#!/usr/bin/env python3

import os
import unittest
from main import app

BASEDIR = os.path.expanduser('~/Projects')


class TestJobOne(unittest.TestCase):
    def setUp(self):
        app.testing = True
        self.app = app.test_client()

    def test_controller(self):
        rv = self.app.post('/')
        self.assertEqual(rv.status_code, 400)

        rv = self.app.post('/', json='')
        self.assertEqual(rv.status_code, 400)
        self.assertEqual(rv.text, '{"message":"Input .json is empty"}\n')

    def test_json(self):
        wrong_json = {
            "some_dir": "2022-08-09",
            "raw_dir": "raw/sales/2022-08-09"
            }
        rv = self.app.post('/', json=wrong_json)
        self.assertEqual(rv.status_code, 422)

        wrong_json = {
            "stg_dir": "stg/sales/2022-08-09",
            "raw_dir": "raw/sales/2022-08-01"
            }
        rv = self.app.post('/', json=wrong_json)
        self.assertEqual(rv.status_code, 422)

    def test_files(self):
        ok_json = {
            "stg_dir": "stg/sales/2022-08-09",
            "raw_dir": "raw/sales/2022-08-09"
            }
        rv = self.app.post('/', json=ok_json)
        self.assertEqual(rv.status_code, 201)

        storing_path = os.path.join(
            BASEDIR,
            'robot-dreams-dataeng/lesson2/data',
            'stg/sales/2022-08-09'
            )

        reading_path = os.path.join(
            BASEDIR,
            'robot-dreams-dataeng/lesson2/data',
            'raw/sales/2022-08-09'
            )

        self.assertTrue(os.path.exists(storing_path))
        self.assertEqual(
            len(os.listdir(storing_path)),
            len(os.listdir(reading_path))
            )


if __name__ == "__main__":
    unittest.main()
