import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
from unittest.mock import patch, mock_open
import importlib.util

# Load dags.configs directly from file to avoid pytest import mocking
spec = importlib.util.spec_from_file_location("dags_configs", os.path.join(os.path.dirname(__file__), '..', 'configs.py'))
configs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(configs)


def test_config_path_ends_with_configs_json():
    assert configs._CONFIG_PATH.endswith(os.path.join('configs', 'configs.json'))


def test_config_path_contains_dags():
    assert 'dags' in configs._CONFIG_PATH


def test_getConfig_returns_parsed_json():
    fake = {'ConnectionString': 'test', 'FtpHost': 'localhost'}
    with patch('builtins.open', mock_open(read_data=json.dumps(fake))):
        result = configs.getConfig()
    assert result == fake
