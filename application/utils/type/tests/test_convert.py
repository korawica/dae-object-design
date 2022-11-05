import logging
import unittest
import application.utils.type.convert as convert
from typing import List


class ConvertTestCase(unittest.TestCase):
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        format='%(asctime)s %(module)s %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    def setUp(self) -> None:
        self.str_true_lists: List[str] = [
            'true',
            'True',
            '1',
            'Y',
            'y',
            'yes',
            'Yes',
        ]
        self.str_false_lists: List[str] = [
            'false',
            'False',
            '0',
            'N',
            'n',
            'no',
            'No',
        ]
        self.str_raise_lists: List[str] = [
            'x',
            'X',
        ]

    def test_convert_to_bool(self):
        for _string in self.str_true_lists:
            self.assertTrue(convert.convert_str_to_bool(_string))

        for _string in self.str_false_lists:
            self.assertFalse(convert.convert_str_to_bool(_string))

        for _string in self.str_raise_lists:
            self.assertRaises(ValueError, convert.convert_str_to_bool, _string)
