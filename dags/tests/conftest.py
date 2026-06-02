import sys
import os
from unittest.mock import MagicMock

# Mock the configs module before any DAG modules try to import it
sys.modules['configs'] = MagicMock()
