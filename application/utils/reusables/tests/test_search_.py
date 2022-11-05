import logging
import unittest
from unittest.mock import patch
from typing import List
import application.utils.get_.search_ as search_


class SearchTestCase(unittest.TestCase):
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        format='%(asctime)s %(module)s %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    def setUp(self) -> None:
        self.patcher = patch.dict('os.environ', {'PATH': 'mockup/main/test'})
        self.patcher.start()
        self.env_lists: List[str] = [
            'CONF_PATH="${PATH}/conf"',
            'DATA_PATH="${TEST}/data"',
            "VALUE='${PATH}1234'",
        ]
        self.env_results: List[dict] = [
            {'CONF_PATH': 'mockup/main/test/conf'},
            {'DATA_PATH': '/data'},
            {'VALUE': '${PATH}1234'}
        ]

    def tearDown(self) -> None:
        self.patcher.stop()
        ...

    def test_random_string(self):
        for index, env_string in enumerate(self.env_lists, start=0):
            self.assertDictEqual(search_.search_env(env_string), self.env_results[index])
