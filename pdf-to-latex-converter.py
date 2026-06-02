import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'dags'))

import pdfConverter

if __name__ == '__main__':
    converted = pdfConverter.run_conversion()
    print(f"Converted {converted} files")
