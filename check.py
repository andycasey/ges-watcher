#!/opt/ioa/software/python/2.7.8/bin/python

""" Check the status of all WG submissions. """

from __future__ import absolute_import, print_function, with_statement

__author__ = "Andy Casey <arc@ast.cam.ac.uk>"

import fnmatch
import logging
import os
import re
import shutil
import smtplib
import subprocess
import sys
import textwrap
import time
import yaml
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.MIMEBase import MIMEBase
from email import Encoders
from getpass import getuser
from glob import glob


INVENTORY_FILENAME = "/data/arc/codes/ges-watcher/inventory.yaml"


def check_node_submission(list_of_submitted_files):
    """
    Check that at least one of the submitted FITS files has zero INVALID entries
    in their Dropbox folder.

    :param list_of_submitted_files:
        The submitted files, as per read from the inventory file.

    :type list_of_submitted_files:
        list
    """

    results = {}
    any_ok = False

    for filename, created, updated in list_of_submitted_files:

        log_filename = filename[:-5] + "_FITSchecker_REPORT.log"
        with open(log_filename, "r") as fp:
            log_contents = fp.read()
            num_invalids = len(re.findall("INVALID", log_contents))
            num_lines = log_contents.count("\n")

        results[filename] = (num_invalids, num_lines)

        if any_ok is False and num_lines >= 30 and num_invalids == 0:
            return (True, filename)

    if len(list_of_submitted_files) > 0:
        return (False, results)

    return (None, None)


if __name__ == "__main__":

    with open(INVENTORY_FILENAME, "r") as fp:
        inventory = yaml.load(fp)

    # For each folder we want to know if they are:
    # OK_PASSED, SUBMITTED_INVALID, NOT_SUBMITTED
    # True, False, None

    submitted_and_valid = {}
    submitted_and_invalid = {}
    none_submitted = {}

    # Sort the keys.
    folders = sorted(inventory.keys())
    for folder in folders:
        wg, node = folder.split("/")[-2:]
        if node in ("Recommended", "PerSpectra"): continue

        submitted_contents = inventory[folder]
        result, info = check_node_submission(submitted_contents)

        if result == True:
            submitted_and_valid["{0} {1}".format(wg, node)] = info

        elif result is False:
            submitted_and_invalid["{0} {1}".format(wg, node)] = info

        elif result is None:
            none_submitted["{0} {1}".format(wg, node)] = None


    k = sorted(submitted_and_valid.keys())
    print("Nodes that have submitted valid results:")
    for _ in k:
        print("\t{0}: {1}".format(_, submitted_and_valid[_]))

    print("\n\n")

    k = sorted(submitted_and_invalid.keys())
    print("Nodes that have submitted results, but they still have errors:")
    for _ in k:
        print("\t{0}:".format(_))
        for filename, (num_invalids, num_lines) in submitted_and_invalid[_].items():
            print("\t\t{0}: {1} INVALIDs, {2} lines".format(filename, num_invalids,
                num_lines))
        print("\n")
    print("\n\n")

    k = sorted(none_submitted.keys())
    print("Currently missing results from:\n\t{0}".format(
        "\n\t".join(k)))

