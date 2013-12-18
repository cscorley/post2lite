import unittest
import post_to_lite
import sqlite3
from dateutil.parser import parse as date_parse

class TestSuite(unittest.TestCase):
    def test_parse_schema(self):
        tokens = [  "CREATE",
                    "TABLE",
                    "comments",

                    "(",

                    "id",
                    "integer",
                    "NOT",
                    "NULL",
                    ",",

                    "post_id",
                    "integer",
                    ",",

                    "score",
                    "integer",
                    ",",

                    "text",
                    "text",
                    ",",

                    "creation_date",
                    "date",
                    ",",

                    "user_id",
                    "integer",
                    ")"
                    ]
        name, schema = post_to_lite.parse_schema(tokens)

        self.assertEquals(name, "comments")
        expected = [('id', int),
                    ('post_id', int),
                    ('score', int),
                    ('text', str),
                    ('creation_date', date_parse),
                    ('user_id', int)
                    ]
        expected = [int, int, int, str, date_parse, int]

        self.assertEquals(schema, expected)

