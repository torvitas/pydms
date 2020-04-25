#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import asyncio

from pdftotext import PDF
from ocrmypdf import ocr
from yaml import safe_load
from os import path, rename
from glob import glob
from functools import reduce
from aionotify import Watcher, Flags
from re import compile, MULTILINE

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def run():
    filename = "test.pdf"
    configfilename = "config.yml"
    config = {}
    with open(configfilename, "r") as configfile:
        try:
            config = safe_load(configfile)
        except:
            logging.exception("Cannot load configuration", exc_info=True)

    rules = compileRules(config["rules"])
    sources = []
    for source in config["sources"]:
        sources.append(path.abspath(path.expandvars(path.expanduser(source))))
    logging.debug(sources)

    if "import" in config:
        filenames = sourcesToFilenames(sources)
        for filename in filenames:
            load(filename, rules)

    if "watch" in config:
        logging.info("Watching for changes..")
        watch(sources, rules)


def compileRules(ruleConfigurations):
    compiledRules = []
    for rule in ruleConfigurations:
        prepared = {
            "config": rule,
            "search": compile(rule["search"], MULTILINE),
            "extract": []
        }
        if "extract" in rule:
            for extractor in rule["extract"]:
                prepared["extract"].append({
                    "compiled":
                    compile(extractor["search"], MULTILINE),
                    "config":
                    extractor
                })
        compiledRules.append(prepared)
    return compiledRules


def watch(sources, rules):
    async def handleEvents(watcher, loop):
        await watcher.setup(loop)
        while True:
            event = await watcher.get_event()
            filename = f"{event.alias}/{event.name}"
            load(filename, rules)
            logging.debug(event)

    watcher = Watcher()
    for source in sources:
        if path.exists(source) != True:
            logging.info(f"{source} does not exist, skipping watch.")
            continue
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
            files += glob(f"{source}/*.pdf")
        else:
            files.append(source)
    return files


def load(filename, rules):
    def searchRules(text):
        logging.debug("Looking for rules..")
        for rule in rules:
            logging.info(rule["config"])
            search = rule["search"].search(text)
            if search:
                logging.info(f"Found rule: {rule['config']['search']}")
                return rule

    def extractProperties(text, extract):
        properties = {}
        for extractor in extract:
            match = extractor["compiled"].match(text)
            propertyName = extractor["config"]["key"]
            if match:
                properties[propertyName] = "".join(match.groups())
                logging.info(
                    f"Found {propertyName} {', '.join(match.groups())}")
            else:
                logging.exception(f"{propertyName} not found.")
                raise
        return properties

    def constructPath(target, properties):
        return target.format(extract=properties)

    def readTextFromPdf(filename):
        text = ""
        with open(filename, "rb") as filehandle:
            try:
                # get array of pages, join them into single string,
                # split the string by new lines,
                # join it into a single string, strip whitespaces
                # now we got the whole text in a single line string
                text = " ".join(" ".join(PDF(filehandle)).split()).strip()
            except:
                logging.exception(
                    f"Cannot extract text from {path.basename(filename)}.",
                    exc_info=True)
        if text == "":
            raise Exception("PDF does not contain text.")
        return text

    def extractText(filename):
        outputFilename = f"/tmp/{path.basename(filename)}"
        try:
            ocr(input_file=filename,
                output_file=outputFilename,
                force_ocr=True,
                progress_bar=False)
            return outputFilename
        except:
            logging.exception(f"Cannot ocr {path.basename(filename)}.",
                              exc_info=True)

    if path.isdir(filename):
        logging.info(f"{filename} is a Directory, skipping.")
        return
    if path.exists(filename) != True:
        logging.info(f"{filename} does not exist, skipping.")
        return
    logging.info(f"Loading {filename}")
    text = ""
    try:
        text = readTextFromPdf(filename)
    except:
        logging.info("No text found, using ocr.")
        tmpfilename = extractText(filename)
        try:
            text = readTextFromPdf(tmpfilename)
        except:
            logging.info("No text found after using ocr.")
    logging.debug(text)
    logging.info(f"Read {len(text)} characters from {path.basename(filename)}")

    search = searchRules(text)
    if search == None:
        logging.info("Found no searching rules, skipping.")
        return

    properties = []
    try:
        properties = extractProperties(text, search["extract"])
    except:
        logging.info(
            f"Unable to extract all properties for {filename} when applying {search['config']['search']}, skipping."
        )
        return

    try:
        pathname = constructPath(search["config"]["target"], properties)
        logging.info(f"Constructed target path {pathname}")
    except:
        logging.exception(
            "Cannot construct path from properties, please double check the configuration.",
            exc_info=True)

    try:
        rename(filename, path.abspath(pathname))
    except:
        logging.exception(
            f"Cannot move {filename} to {path.abspath(pathname)}.",
            exc_info=True)


run()
