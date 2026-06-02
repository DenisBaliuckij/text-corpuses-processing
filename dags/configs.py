import json
import os

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'configs', 'configs.json')


def getConfig() -> dict:
    with open(_CONFIG_PATH) as f:
        return json.load(f)
