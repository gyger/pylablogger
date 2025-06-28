#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Entry Point to the lablogger tools

"""
import logging
import pathlib

import click

import pylablogger.log

from pylablogger import __version__, config

__author__ = "SG"
__copyright__ = "SG / gyger.at"
__license__ = "MIT"

_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@click.group()
def cli(configfile):
    # Default Entry Point
    pass

cli.add_command(pylablogger.log.cli)
