#!/usr/bin/env python3

import os
import unittest
from unittest import mock
from main import app

BASEDIR = os.path.expanduser('~/Projects')
CREDS_FILE = 'test/robot-dreams-auth-token.txt'
with open(os.path.join(BASEDIR, CREDS_FILE), 'r') as fh:
    AUTH_TOKEN = fh.read().rstrip('\n')


class TestJobOne(unittest.TestCase):
    def setUp(self):
        app.testing = True
        self.app = app.test_client()

    @mock.patch.dict(os.environ, {"AUTH_TOKEN": AUTH_TOKEN})
    def test_controller(self):
        rv = self.app.post('/')
        self.assertEqual(rv.status_code, 400)

        rv = self.app.post('/', json='')
        self.assertEqual(rv.status_code, 400)
        self.assertEqual(rv.text, '{"message":"Input .json is empty"}\n')

        wrong_json = {"data": "2022-08-09", "raw_dir": "raw/sales/2022-08-09"}
        rv = self.app.post('/', json=wrong_json)
        self.assertEqual(rv.status_code, 422)

        ok_json = {"date": "2022-08-09", "raw_dir": "raw/sales/2022-08-09"}
        rv = self.app.post('/', json=ok_json)
        self.assertEqual(rv.status_code, 201)

    def test_files(self):
        storing_path = os.path.join(
            BASEDIR,
            'robot-dreams-dataeng/lesson2/data',
            'raw/sales/2022-08-09'
            )
        self.assertTrue(os.path.exists(storing_path))
        self.assertNotEqual(len(os.listdir(storing_path)), 0)


if __name__ == "__main__":
    unittest.main()
