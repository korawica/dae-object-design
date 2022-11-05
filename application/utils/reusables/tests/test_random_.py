import logging
import unittest
from unittest.mock import patch
import os
import application.utils.reusables.random_ as random_


def fake_remove(path, *a, **k):
    print("remove done")


class RandomTestCase(unittest.TestCase):
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        format='%(asctime)s %(module)s %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    @patch('os.remove', fake_remove)
    def test(self):
        try:
            os.remove('%$!?&*')
        except OSError as e:
            print(e)
        else:
            print('test success')

    def setUp(self) -> None:
        self.patcher = patch('random.choices', return_value='AA145WQ2')
        self.patcher.start()

    def tearDown(self) -> None:
        self.patcher.stop()

    def test_random_string(self):
        self.assertEqual(random_.random_sting(), 'AA145WQ2')
