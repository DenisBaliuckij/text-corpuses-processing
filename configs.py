# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 15:24:30 2026

@author: denis
"""
import json

def getConfig():
    with open('configs\\configs.json') as json_data:
        return json.load(json_data)