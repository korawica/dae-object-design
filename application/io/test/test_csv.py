import os
import unittest
import warnings
import application.io.filelibs as file


class CSVTest(unittest.TestCase):

    def setUp(self) -> None:
        warnings.simplefilter('ignore', category=ResourceWarning)
        self.csv_str: str = """Col01|Col02|Col03
A|1|test1
B|2|test2
C|3|test3
"""
        self.csv_data: list = [
            {'Col01': 'A', 'Col02': '1', 'Col03': 'test1'},
            {'Col01': 'B', 'Col02': '2', 'Col03': 'test2'},
            {'Col01': 'C', 'Col02': '3', 'Col03': 'test3'}
        ]
        self.root_path: str = os.path.dirname(os.path.abspath(__file__)).replace(os.sep, '/')
        self.csv_path: str = f'{self.root_path}/test_file.csv'
        self.csv_env_path: str = f'{self.root_path}/test_env_file.csv'
        # with open(self.csv_path, 'w', encoding='utf-8') as f:
        #     f.write(self.csv_str)
        file.CSV(self.csv_path).save(self.csv_data)

    def test_load_yaml(self):
        self.csv_data_from_load = file.CSV(self.csv_path).load()
        self.assertListEqual(self.csv_data, self.csv_data_from_load)

    # def test_load_yaml_env(self):
    #     os.environ['DEMO_ENV_VALUE'] = 'add_new_value'
    #     self.yaml_data_from_load = file.YamlEnv(self.yaml_path).load()
    #     self.assertEqual('test add_new_value value', self.yaml_data_from_load['main_key']['sub_key']['string'])

        # del os.environ['DEMO_ENV_VALUE']

    def tearDown(self) -> None:
        # for path in {self.csv_path, self.yaml_env_path, }:
        # for path in {self.csv_path, }:
        #     if os.path.exists(path):
        #         os.remove(path)
        ...
