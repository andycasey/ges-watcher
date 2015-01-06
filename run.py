#!/usr/bin/env python

""" Watch for new GES FITS files and check them for validity """

from __future__ import absolute_import, print_function, with_statement

__author__ = "Andy Casey <arc@ast.cam.ac.uk>"

import fnmatch
import logging
import os
import smtplib
import sys
import textwrap
import time
import yaml
from email.mime.text import MIMEText
from getpass import getuser
from glob import glob

GES_ADMINISTRATORS = ["arc@ast.cam.ac.uk"]
INVENTORY_FILENAME = "/data/arc/codes/ges-watcher/inventory.yaml"
FOLDERS_TO_WATCH = [
    {
        "path": "/data/arc/research/globular-clusters/",
        "owners": [
            "andycasey@gmail.com"
        ]
    }

]

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG)

def create_inventory(folder, filter_by="*.fits"):
    """
    Create an inventory of a folder and return the filename, created, and last
    modified times.

    :param folder:
        The path of the folder to stock take.

    :type folder:
        str

    :param filter_by: [optional]
        A filename filter to use for the inventory.

    :type filter_by:
        str

    :returns:
        A recursive inventory of the folder contents that match the filter.
    """

    matches = []
    filter_by = filter_by.lower()
    for root, dirnames, filenames in os.walk(folder):
        lower_filenames = map(str.lower, filenames)
        for filename in fnmatch.filter(lower_filenames, filter_by):
            original_filename = filenames[lower_filenames.index(filename)]
            matches.append(os.path.join(root, original_filename))

    inventory = []
    for match in matches:
        created = os.path.getctime(match)
        modified = os.path.getmtime(match)
        inventory.append((match, created, modified))

    return inventory



def new_file_inventory(previous_inventory, current_inventory):
    """
    Returns files that have been added between two inventories.

    :param previous_inventory:
        The previous inventory performed.

    :type previous_inventory:
        list

    :param current_inventory:
        The most recent inventory performed.

    :type current_inventory:
        list
    """

    previous_inventory_paths = [each[0] for each in previous_inventory]
    current_inventory_paths = [each[0] for each in current_inventory]

    # Deal with cases
    sc_previous = map(str.lower, previous_inventory_paths)
    sc_current = map(str.lower, current_inventory_paths)

    # Get new items
    sc_new = set(sc_current).difference(sc_previous)

    # Get indexes of each to match back to original path
    sc_new_indices = [sc_current.index(index) for index in sc_new]

    # Return the new inventory
    return [current_inventory[index] for index in sc_new_indices]
    

def modified_file_inventory(previous_inventory, current_inventory):
    """
    Return files that have been modified between the two inventories.

    :param previous_inventory:
        The previous inventory performed.

    :type previous_inventory:
        list

    :param current_inventory:
        The most recent inventory performed.

    :type current_inventory:
        list
    """

    previous_inventory_paths = [each[0] for each in previous_inventory]
    current_inventory_paths = [each[0] for each in current_inventory]

    # Deal with cases
    sc_previous = map(str.lower, previous_inventory_paths)
    sc_current = map(str.lower, current_inventory_paths)

    # Need to match them to previous
    modified_inventory = []
    for current_index, path in enumerate(sc_current):

        # Find this in the previous inventory
        try:
            previous_index = sc_previous.index(path)

        except ValueError:
            # It's a new file.
            continue

        else:
            # We want it if the previous modified time is greater than the
            # current 
            p_path, p_created, p_modified = previous_inventory[previous_index]
            c_path, c_created, c_modified = current_inventory[current_index]

            if c_created > p_created or c_modified > p_modified:
                modified_inventory.append([c_path, c_created, c_modified])

    return modified_inventory


def email_report(recipients, contents, subject="Automated FITS-checker report"):
    """
    Send the FITS checker report.
    """

    if isinstance(recipients, str):
        recipients = [recipients]

    sender = "{0}@ast.cam.ac.uk".format(getuser())
    message = MIMEText(contents)
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)

    server = smtplib.SMTP("localhost")
    server.sendmail(sender, recipients, message.as_string())
    code, message = server.quit()

    return (code, message)


if __name__ == "__main__":

    # Create an initial inventory if none exists.
    if not os.path.exists(INVENTORY_FILENAME):
        logging.info("No previous inventory file found at {}. Creating one and "
            "exiting the program.".format(INVENTORY_FILENAME))

        full_inventory = {}
        for folder in FOLDERS_TO_WATCH:
            full_inventory[folder["path"]] = create_inventory(folder["path"])

        n_folders = len(full_inventory)
        n_files = sum([len(v) for v in full_inventory.itervalues()])

        with open(INVENTORY_FILENAME, "w") as fp:
            yaml.dump(full_inventory, fp)

        logging.info("Saved inventory with {0} file(s) in {1} folder(s) to {2}."
            .format(n_files, n_folders, INVENTORY_FILENAME))
        sys.exit(0)

    # Load the previous inventory
    with open(INVENTORY_FILENAME, "r") as fp:
        full_inventory = yaml.load(fp)
    logging.info("Loaded inventory from {0}".format(INVENTORY_FILENAME))

    # Check for updates in all folders.
    for folder in FOLDERS_TO_WATCH:

        path = folder["path"]

        if path not in full_inventory:
            logging.warn("A new folder has been added and no inventory exists:"\
                " {0} -- you should have constructed a totally new inventory!"
                .format(path))
            full_inventory[path] = []

        current_inventory = create_inventory(path)
        new_files = new_file_inventory(full_inventory[path], current_inventory)

        modified_files = modified_file_inventory(full_inventory[path],
            current_inventory)

        # Append to some message logger
        logging.info("Found {0} new FITS file(s) and {1} modified file(s) in {2}"
            .format(len(new_files), len(modified_files), path))

        # Run the script(s) on the new/modified files and grab the output.

        # Send an email if there is anything to report.
        if len(new_files) > 0 or len(modified_files) > 0:

            contents = textwrap.dedent(
                """
                Found {0} new FITS file(s) and {1} modified file(s) in {2}:

                New files:
                    {3}

                Modified files:
                    {4}
                """.format(len(new_files), len(modified_files), path,
                    "\n\t".join([e[0] for e in new_files]),
                    "\n\t".join([e[0] for e in modified_files])))
            recipients = folder["owners"] + GES_ADMINISTRATORS
            return_code, return_message = email_report(recipients, contents)
            logging.info("Email return code and message was {0} {1}".format(
                return_code, return_message))

        # Update the existing inventory
        full_inventory[path] = current_inventory

    # Save the updated inventory
    n_folders = len(full_inventory)
    n_files = sum([len(v) for v in full_inventory.values()])
    with open(INVENTORY_FILENAME, "w") as fp:
        yaml.dump(full_inventory, fp)

    logging.info("Updated inventory with {0} file(s) in {1} folder(s) to {2}."
        .format(n_files, n_folders, INVENTORY_FILENAME))