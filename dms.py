#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import asyncio

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk import download
from PyPDF2 import PdfFileReader
from textract import process
from yaml import safe_load
from os import path
from glob import glob
from functools import reduce
from aionotify import Watcher, Flags
from re import compile

download("punkt")
download("stopwords")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
filename = "test.pdf"
configfilename = "config.yml"

config = {}
rules = []


def run():
    with open(configfilename, "r") as configfile:
        try:
            config = safe_load(configfile)
        except:
            logging.exception("Cannot load configuration", exc_info=True)

    for rule in config["rules"]:
        rules.append(compile(rule["match"]))

    sources = []
    globs = []
    for source in config["sources"]:
        sources.append(path.abspath(path.expandvars(path.expanduser(source))))
    logging.debug(sources)

    if "import" in config:
        filenames = sourcesToFilenames(sources)
        for filename in filenames:
            load(filename)

    if "watch" in config:
        logging.info("Watching for changes..")
        watch(sources)


def watch(sources):
    async def handleEvents(watcher, loop):
        await watcher.setup(loop)
        while True:
            event = await watcher.get_event()
            filename = event.alias + "/" + event.name
            load(filename)
            logging.debug(event)

    watcher = Watcher()
    for source in sources:
        watcher.watch(path=source,
                      flags=Flags.MODIFY | Flags.CREATE | Flags.MOVED_TO)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handleEvents(watcher, loop))
    loop.stop()
    loop.close()


def sourcesToFilenames(sources):
    files = []
    for source in sources:
        logging.debug(source)
        if path.isdir(source):
            files += glob(source + "/*.pdf")
        else:
            files.append(source)
    return files


def load(file):
    logging.debug("file: " + file)
    text = None
    with open(file, "rb") as filehandle:
        try:
            text = readTextFromPdf(filehandle)
        except:
            logging.exception("Cannot extract text.", exc_info=True)
    tokenized = extractTokensFromText(text)
    rule = applyRules(tokenized)
    logging.debug(rule)


def applyRules(tokens):
    for rule in rules:
        for token in tokens:
            if rule.match(token):
                logging.info("Found match: " + token)
                return rule


def readTextFromPdf(file):
    pdf = PdfFileReader(file)
    text = ""
    for page in pdf.pages:
        text += page.extractText().strip()

    if text == "":
        text = process(filename, method="tesseract", language="deu")

    return text


def extractTokensFromText(text):
    tokens = word_tokenize(str(text))
    punctuations = ["(', ')", ";", ":", "[", "]", ","]
    stop_words = stopwords.words("german")

    content = list(set(tokens) - set(punctuations) - set(stop_words))
    return content


run()
