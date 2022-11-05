import os
import unittest
import warnings
import application.io.filelibs as file


class YamlTest(unittest.TestCase):

    def setUp(self) -> None:
        warnings.simplefilter('ignore', category=ResourceWarning)
        self.yaml_str: str = """main_key:
    sub_key:
        string: 'test ${DEMO_ENV_VALUE} value'
        int: 0.001
        bool: false
        list: ['i1', 'i2', 'i3']
"""
        self.yaml_data: dict = {
            'main_key': {
                'sub_key': {
                    'string': 'test ${DEMO_ENV_VALUE} value',
                    'int': 0.001,
                    'bool': False,
                    'list': ['i1', 'i2', 'i3']
                }
            }
        }
        self.root_path: str = os.path.dirname(os.path.abspath(__file__)).replace(os.sep, '/')
        self.yaml_path: str = f'{self.root_path}/test_file.yaml'
        self.yaml_env_path: str = f'{self.root_path}/test_env_file.yaml'
        file.Yaml(self.yaml_path).save(self.yaml_data)

    def test_load_yaml(self):
        self.yaml_data_from_load = file.Yaml(self.yaml_path).load()
        self.assertDictEqual(self.yaml_data, self.yaml_data_from_load)

    def test_load_yaml_env(self):
        os.environ['DEMO_ENV_VALUE'] = 'add_new_value'
        self.yaml_data_from_load = file.YamlEnv(self.yaml_path).load()
        self.assertEqual('test add_new_value value', self.yaml_data_from_load['main_key']['sub_key']['string'])

        del os.environ['DEMO_ENV_VALUE']

    def tearDown(self) -> None:
        for path in {self.yaml_path, self.yaml_env_path, }:
            if os.path.exists(path):
                os.remove(self.yaml_path)
