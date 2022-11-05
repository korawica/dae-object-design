import unittest
import application.io.baseconf as bc


class BaseConfTest(unittest.TestCase):

    def setUp(self) -> None:
        self.parameters = {
            'conn_db_test': {
                'type': 'Postgres',
                'host': 'host-name',
                'port': 5432,
                'username': 'user-name'
            },
            'conn_file_test': {
                'type': 'CSV',
                'properties': {
                    'header': True
                }
            }
        }

    def test_initialize(self):
        self.bs_conf = bc.BaseConf(parameters=self.parameters)
        self.assertEqual('Postgres', self.bs_conf.conn_db_test.type)
