#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import asyncio

from nltk import download
from PyPDF2 import PdfFileReader
from textract import process
from yaml import safe_load
from os import path
from glob import glob
from functools import reduce
from aionotify import Watcher, Flags
from re import compile

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
        prepared = {
            "config": rule,
            "match": compile(rule["match"]),
            "extract": []
        }
        if "extract" in rule:
            for extractor in rule["extract"]:
                prepared["extract"].append({
                    "compiled":
                    compile(extractor["match"]),
                    "config":
                    extractor
                })
        rules.append(prepared)

    sources = []
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
    logging.info("Loading " + file)
    text = None
    with open(file, "rb") as filehandle:
        try:
            text = readTextFromPdf(filehandle)
        except:
            logging.exception("Cannot extract text.", exc_info=True)
    match = matchRules(text)
    properties = []
    if match:
        properties = extractProperties(text, match["extract"])

    if properties:
        path = constructPath(match["config"]["target"], properties)


def matchRules(text):
    logging.info("Looking for rules..")
    for rule in rules:
        match = rule["match"].match(text)
        if match:
            logging.info("Found rule: " + match.group())
            return rule
    logging.info("No rule applied.")


def extractProperties(text, extract):
    properties = {}
    for extractor in extract:
        match = extractor["compiled"].match(text)
        propertyName = extractor["config"]["key"]
        if match:
            properties[propertyName] = "".join(match.groups())
            logging.info(f"Found {propertyName} {', '.join(match.groups())}")
    return properties


def constructPath(target, properties):
    path = target.format(extract=properties)
    logging.info(path)
    return path


def readTextFromPdf(file):
    pdf = PdfFileReader(file)
    text = ""
    for page in pdf.pages:
        text += page.extractText().strip()

    logging.info("No text found, using ocr.")
    if text == "":
        text = str(process(file.name, method="tesseract", language="deu"))

    return text


run()
