import unittest
import warnings
from unittest.mock import patch, Mock
import application.core.register as register
from application.errors import ConfigArgumentError


class BaseRegisterTest(unittest.TestCase):
    """"""

    @patch('application.core.register.params.engine.config_domain', True)
    @patch('application.core.register.params.engine.config_environment', False)
    def test_base_initialize_with_domain(self):
        regis = register.BaseRegister(name='test_name', domain='test_domain/')
        self.assertEqual('test_domain', regis.domain)
        self.assertEqual('test_domain:test_name', regis.fullname)
        self.assertEqual('tn', regis.shortname)
        self.assertEqual('TEST_NAME', regis.formatter()['name']['%N']['value'])

    @patch('application.core.register.params.engine.config_domain', False)
    def test_base_initialize_without_domain(self):
        with self.assertRaises(ConfigArgumentError):
            register.BaseRegister(name='test_name.sep_with_dot')
            register.BaseRegister(name='test_name', domain='test_domain/')


class StageTest(unittest.TestCase):
    """"""

    @patch.dict(
        'application.core.register.params.stages',
        {
            'staging': {'format': 'test_format'},
            'curated': {'format': 'test_curated_format'},
        }
    )
    def test_stage_base(self):
        self.assertEqual('test_format', register.Stage('staging').format)
        self.assertEqual('curated', register.Stage.final)
        staging = register.Stage('staging')
        curated = register.Stage('curated')
        self.assertTrue(staging < curated)
        self.assertTrue(staging <= curated)
        self.assertTrue(staging != curated)
        self.assertFalse(staging > curated)
        self.assertFalse(staging >= curated)
        staging = staging.refresh()
        print(staging.format)


class RegisterTest(unittest.TestCase):
    """"""

    def setUp(self) -> None:
        warnings.simplefilter('ignore', category=ResourceWarning)

    def test_register_filler(self):
        _regis = register.Register(name='demo:conn_local_file')
        self.assertEqual('CONN_LOCAL_FILE_testing_DM', _regis.filler(value='{name:%N}_testing_{domain:%U}'))
        with self.assertRaises(ConfigArgumentError):
            _regis.parser('staging', 'CONN_LOCAL_FILE.20220101_121314_1_0_0')
