# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 13:08:42 2026

@author: denis
"""

# pip install pypdf
from pypdf import PdfReader
import ftpConnector
from ftpConnector import ftpConnector
import dbConnector
from dbConnector import databaseConnector
import io

i = 0
while True:   
    i+=1
    urlToConvert = databaseConnector.getPdfToConvertToLatex()
    try:
        file = ftpConnector.getFile(urlToConvert)
        reader = PdfReader(file)
        text = ""
        for page in reader.pages:
             text += page.extract_text()
        result = io.StringIO(text)
        filename = urlToConvert.replace('.pdf', '.tex')
        ftpConnector.storeFile(filename, io.BytesIO(result.read().encode('utf8')), 'Tex')
        print(urlToConvert)
        print(filename)
        databaseConnector.saveLatexFileLocation(urlToConvert, filename)
        if i>500:
            break
    except Exception as e:
        databaseConnector.saveLatexFileLocation(urlToConvert, 'NA')
        



