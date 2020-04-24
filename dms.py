#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk import download
from PyPDF2 import PdfFileReader
from textract import process
from yaml import safe_load
from os import path

download('punkt')
download('stopwords')

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')
filename = 'test.pdf'
configfilename = 'config.yml'


def run():
    config = None
    text = None
    with open(configfilename, 'r') as configfile:
        try:
            config = safe_load(configfile)
        except:
            logging.exception("Cannot load configuration", exc_info=True)
    with open(config['file'], 'rb') as file:
        try:
            text = readTextFromPdf(file)
        except:
            logging.exception("Cannot extract text.", exc_info=True)

    tokenized = extractTokensFromText(text)
    logging.debug(tokenized)


def readTextFromPdf(file):
    pdf = PdfFileReader(file)
    text = ""
    for page in pdf.pages:
        text += page.extractText().strip()

    if text == "":
        text = process(filename, method='tesseract', language='deu')

    return text


def extractTokensFromText(text):
    tokens = word_tokenize(str(text))
    punctuations = ['(', ')', ';', ':', '[', ']', ',']
    stop_words = stopwords.words('german')

    content = list(set(tokens) - set(punctuations) - set(stop_words))
    return content


run()
