import logging
import unittest
import application.utils.type.merge as merge
from typing import List


class MergeTestCase(unittest.TestCase):
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        format='%(asctime)s %(module)s %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    def setUp(self) -> None:
        self.dicts: List[dict] = [
            {
                'A': 1,
                'B': 2,
            },
            {
                'A': 10,
                'C': 30,
            },
            {
                'C': 300,
                'D': 400,
            }
        ]
        self.dicts_merge_result: dict = {
            'A': 10,
            'B': 2,
            'C': 300,
            'D': 400,
        }
        self.lists: List[list] = [
            [
                'A',
                'B',
                'C',
            ],
            [
                'C',
                'D',
                'E',
            ]
        ]
        self.lists_merge_result: list = [
            'A',
            'B',
            'C',
            'C',
            'D',
            'E',
        ]

    def test_merge_dict(self):
        self.logger.info("Start testing merge dict ...")
        self.assertDictEqual(merge.merge_dict(*self.dicts, mode='chain'), self.dicts_merge_result)
        self.logger.info("Success with mode 'chain'")
        self.assertDictEqual(merge.merge_dict(*self.dicts, mode='update'), self.dicts_merge_result)
        self.logger.info("Success with mode 'update'")
        self.assertDictEqual(merge.merge_dict(*self.dicts, mode='reduce'), self.dicts_merge_result)
        self.logger.info("Success with mode 'reduce'")

    def test_merge_list(self):
        self.assertListEqual(merge.merge_list(*self.lists, mode='extend'), self.lists_merge_result)
        self.assertListEqual(merge.merge_list(*self.lists, mode='reduce'), self.lists_merge_result)

