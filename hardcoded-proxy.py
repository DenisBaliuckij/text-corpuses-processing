# -*- coding: utf-8 -*-
"""
Created on Mon May 11 07:38:15 2026

@author: denis
"""

# -*- coding: utf-8 -*-
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dags'))

import json, urllib.request
import time
from repositories.proxy_repository import ProxyRepository


ProxyRepository.add_or_update('brd-customer-hl_68e14c58-zone-isp_proxy1:sgpoqre858ru@brd.superproxy.io', 33335, 2094097452, 'http')
