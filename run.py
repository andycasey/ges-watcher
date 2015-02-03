#!/opt/ioa/software/python/2.7.8/bin/python

""" Watch for new GES FITS files and check them for validity """

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


SEND_EMAILS = False
FITSCHECKER = "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/FITSChecker/run_fitschecker.sh"
FITSCHECKER_LOG_FORMAT = "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/FITSChecker"\
    "/Output/{basename}_FITSchecker_REPORT_{date}.log"
GES_ADMINISTRATORS = [
    "Andy Casey <arc@ast.cam.ac.uk>",
    "Clare Worley <ccworley@ast.cam.ac.uk>"
]
INVENTORY_FILENAME = "/data/arc/codes/ges-watcher/inventory.yaml"
FOLDERS_TO_WATCH = [
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG12/Arcetri",
        "owners": [
            "Elena Franciosini <francio@arcetri.astro.it>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/CAUP",
        "owners": [
            "Sergio Sousa <sousasag@astro.up.pt>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG12/CAUP",
        "owners": [
            "Sergio Sousa <sousasag@astro.up.pt>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/Concepcion",
        "owners": [
            "Sandro Villanova <svillanova@astro-udec.cl>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG10/EPINARBO",
        "owners": [
            "Laura Magrini <laura@arcetri.astro.it>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/EPINARBO",
        "owners": [
            "Laura Magrini <laura@arcetri.astro.it>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG10/IAC",
        "owners": [
            "Carlos Allende-Prieto <callende@iac.es>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/IACAIP",
        "owners": [
            "Carlos Allende-Prieto <callende@iac.es>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG10/Lumba",
        "owners": [
            "Karin Lind <karin.lind@physics.uu.se>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/Lumba",
        "owners": [
            "Greg Ruchti <greg@astro.lu.se>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG10/MaxPlanck",
        "owners": [
            "Maria Bergemann <bergemann@mpia-hd.mpg.de>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/MaxPlanck",
        "owners": [
            "Maria Bergemann <bergemann@mpia-hd.mpg.de>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/MyGIsFOS",
        "owners": [
            "Luca Sbordone <lsbordon@lsw.uni-heidelberg.de>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG10/Nice",
        "owners": [
            "Alejandra Recio-Blanco <alejandra.recio-blanco@oca.eu>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/Nice",
        "owners": [
            "Clare Worley <ccworley@ast.cam.ac.uk>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG10/OACT",
        "owners": [
            "Antonio Frasca <antonio.frasca@oact.inaf.it>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/OACT",
        "owners": [
            "Antonio Frasca <antonio.frasca@oact.inaf.it>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG12/OACT",
        "owners": [
            "Alessandro Lanzafame <a.lanzafame@unict.it>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG12/OAPA",
        "owners": [
            "Francesco Damiani <damiani@astropa.unipa.it>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG10/Potsdam",
        "owners": [
            "Marica Valentini <mvalentini@aip.de>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/Potsdam",
        "owners": [
            "Marica Valentini <mvalentini@aip.de>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/UCM",
        "owners": [
            "Hugo Tabernero <htabernero@ucm.es>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG12/UCM",
        "owners": [
            "Hugo Tabernero <htabernero@ucm.es>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG10/ULB",
        "owners": [
            "Sophie VanEck <svaneck@astro.ulb.ac.be>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/ULB",
        "owners": [
            "Sophie VanEck <svaneck@astro.ulb.ac.be>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/Vilnius",
        "owners": [
            "Grazina Tautvaisiene <grazina.tautvaisiene@tfai.vu.lt>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG13/??1",
        "owners": [
            "Fabrice Martins <fabrice.martins@univ-montp2.fr>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG13/??2",
        "owners": [
            "Andrew Tkachenko <Andrew.Tkachenko@ster.kuleuven.be>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG13/IAC",
        "owners": [
            "Artemio Herrero <ahd@iac.es>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG13/Liege",
        "owners": [
            "Thierry Morel <morel@astro.ulg.ac.be>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG13/ROB",
        "owners": [
            "Alex Lobel <alex.lobel@oma.be>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG13/ROBGrid",
        "owners": [
            "Ronny Blomme <Ronny.Blomme@oma.be>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG14/PerSpectra",
        "owners": [
            "Sophie VanEck <svaneck@astro.ulb.ac.be>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG10/Recommended",
        "owners": [
            "Alejandra Recio-Blanco <alejandra.recio-blanco@oca.eu>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG11/Recommended",
        "owners": [
            "Rodolfo Smiljanic <rsmiljanic@ncac.torun.pl>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG12/Recommended",
        "owners": [
            "Alessandro Lanzafame <a.lanzafame@unict.it>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG13/Recommended",
        "owners": [
            "Ronny Blomme <Ronny.Blomme@oma.be>"
        ]
    },
    {
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG14/Recommended",
        "owners": [
            "Sophie VanEck <svaneck@astro.ulb.ac.be>"
        ]
    },
    {
        # [TODO] This folder does not exist!
        "path": "/data/gaia-eso/geswg15/GESIoA/iDR4PA/WG15/WG15/Recommended",
        "owners": [
            "Patrick Francois <patrick.francois@obspm.fr>"
        ]
    },
]


logging.basicConfig(level=logging.DEBUG, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=os.path.join(os.path.dirname(__file__), "iDR4.log"))

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


def email_report(recipients, contents, subject="Automated FITS-checker report",
    attachments=None):
    """
    Send the FITS checker report.
    """

    if SEND_EMAILS:
        return (0, "This is a *dry run* -- no emails actually sent.")

    if isinstance(recipients, str):
        recipients = [recipients]

    if attachments is not None:
        assert isinstance(attachments, (list, tuple))

    to = recipients + GES_ADMINISTRATORS
    sender = "{0}@ast.cam.ac.uk".format(getuser())
    message = MIMEMultipart()
    message["From"] = sender
    message["To"] = ", ".join(to)
    message["Subject"] = subject
    message.attach(MIMEText(contents))

    for filename in attachments or []:
        with open(filename, "r") as fp:
            part = MIMEBase("applicaton", "octet-stream")
            part.set_payload(fp.read())
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="{}.txt"'\
            .format(os.path.splitext(os.path.basename(filename))[0]))
        message.attach(part)

    server = smtplib.SMTP("localhost")
    server.sendmail(sender, to, message.as_string())
    code, message = server.quit()

    return (code, message)


if __name__ == "__main__":

    # Usage: python run.py

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
        if full_inventory is None:
            full_inventory = {}
    logging.info("Loaded inventory from {0}".format(INVENTORY_FILENAME))

    # Check for updates in all folders.
    total_updated_files, num_invalids = 0, 0
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
        if len(new_files) * len(modified_files) > 0:
            logging.info("Found {0} new FITS file(s) and {1} modified file(s) "
                "in {2}".format(len(new_files), len(modified_files), path))

        # Run the script(s) on the new/modified files and grab the output.
        all_updated_files = new_files + modified_files
        total_updated_files += len(all_updated_files)
        fitschecker_log_filenames = []
        fitschecker_error_occurred = False
        for filename, created, modified in all_updated_files:

            if fitschecker_error_occurred:
                logging.debug("Skipping filename {0} because a FITSCHECKER error"
                    " occurred".format(filename))
                continue

            if not os.path.exists(filename):
                logging.warn("Filename {} found but no longer exists. We will "
                    "skip it now and it will be removed in the next inventory "
                    "update".format(filename))
                continue

            # Check if a log file already exists?
            fitschecker_log_filename = FITSCHECKER_LOG_FORMAT.format(
                basename=os.path.splitext(os.path.basename(filename))[0],
                date=datetime.now().strftime("%Y-%m-%d"))
            if os.path.exists(fitschecker_log_filename):
                logging.warn("FITSCHECKER log filename {} already exists!"\
                    .format(fitschecker_log_filename))

            logging.info("Running FITSCHECKER on {0}".format(filename))
            try:
                #result = os.system(FITSCHECKER)
                result = subprocess.Popen(FITSCHECKER,
                    cwd=os.path.dirname(FITSCHECKER),
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    env={
                        "filepath": filename,
                    },
                    shell=True, close_fds=True)

            except Exception as e:
                logging.exception("Exception in running FITSCHECKER on {0}"\
                    .format(filename))

                # Yo, email andy!
                return_code, return_message = email_report(["arc@ast.cam.ac.uk"],
                    "Yo, something went wrong with FITSCHECKER: {}".format(e),
                    subject="Error in FITSCHECKER")
                logging.info("Emailed error to Andy. Return code and message:"
                    "\n{0}: {1}".format(return_code, return_message))

            else:
                logging.info("FITSCHECKER finished on {0} with output:\n{1}"\
                    .format(filename, result.stdout.read()))
                
                
                if os.path.exists(fitschecker_log_filename):
                    logging.info("Changing group ownership to geswg15 for {}"\
                        .format(fitschecker_log_filename))
                    os.system("chown arc:geswg15 {}".format(fitschecker_log_filename))

                    with open(fitschecker_log_filename, "r") as fp:
                        contents = fp.read()
                        _ = len(re.findall("INVALID", fp.read()))
                        logging.warn("FITSCHECKER found {0} 'INVALID's in {1}"\
                            .format(_, fitschecker_log_filename))
                        num_invalids += _
                        num_lines = contents.count("\n")

                    # [TODO] What is the critical number?
                    if num_lines < 30:
                        fitschecker_error_occurred = True
                        logging.warn("FITSCHECKER log at {0} has only {1} lines"
                            " -- something probably went wrong!".format(
                                fitschecker_log_filename, num_lines))

                        # Email the GES administrators and say something went
                        # wrong, then do not send this email to the owner.

                        contents = textwrap.dedent("""\
                            Dear kind overlords,

                            I think something has gone wrong with FITSCHECKER, because there were only {0} lines in the report file at {1} (attached).

                            I have not sent any emails out to the owner, {2}, I have skipped any remaining files in this path, and I have not updated the inventory for this path. I will try again in another hour.

                            Best wishes,
                            Robot.
                            """.format(num_lines, fitschecker_log_filename, ", ".join(folder["owners"])))

                        logging.debug("Sending the following email to {0}:\n{2}\n"
                            "With attachment {3}".format(
                                ", ".join(GES_ADMINISTRATORS),
                                contents, fitschecker_log_filename))

                        return_code, return_message = email_report(
                            GES_ADMINISTRATORS, contents,
                            attachments=[fitschecker_log_filename])
                        logging.info("Email return code and message was {0} {1}"\
                            .format(return_code, return_message))
                        break

                    # Copy this log file to the correct path
                    most_recent_fitschecker_log_filename = os.path.join(
                            os.path.dirname(filename),
                            "_".join(os.path.basename(fitschecker_log_filename).split("_")[:-1]) + ".log")
                    try:
                        shutil.copy(fitschecker_log_filename,
                            most_recent_fitschecker_log_filename)
                    except IOError:
                        logging.exception("Failed to copy {0} to {1}".format(
                            fitschecker_log_filename,
                            most_recent_fitschecker_log_filename))

                    else:
                        logging.info("Copied {0} to {1}".format(
                            fitschecker_log_filename,
                            most_recent_fitschecker_log_filename))
                    fitschecker_log_filenames.append(fitschecker_log_filename)

                else:
                    logging.warn("Could not find FITSCHECKER log file {0}"\
                        .format(fitschecker_log_filename))

        # Send an email if there is anything to report.
        if len(new_files) > 0 or len(modified_files) > 0 \
        and not fitschecker_error_occurred:

            if num_invalids > 0:
                invalid_str = ("There were {} serious errors reported by FITSCH"
                    "ECKER for your file(s). These errors are marked with the w"
                    "ord 'INVALID' in the attached log files, and need to be fi"
                    "xed before your results can be used. Please examine the at"
                    "tached files, identify and correct the errors in your FITS"
                    " file(s), and update the version in your Dropbox.".format(
                        num_invalids))
            else:
                invalid_str = "There were no errors reported by FITSCHECKER f"\
                    "or your file(s). Thanks for following the FITS format."

            contents = textwrap.dedent("""\
                Dear {5},
                
                I have found {0} new and {1} modified FITS file(s) in the {2} Dropbox folder, which is owned by you:

                New files:
                    {3}

                Modified files:
                    {4}

                FITSCHECKER has been run on these files and the logs are attached with this email. {6}

                Best wishes,
                Andy Casey

                """.format(
                    len(new_files), len(modified_files),
                    path.split("/")[-1],
                    "\n                    ".join([e[0][len(path)+1:] for e in new_files]),
                    "\n                    ".join([e[0][len(path)+1:] for e in modified_files]),
                    ", ".join([_.split(" <")[0] for _ in folder["owners"]]),
                    invalid_str))

            logging.debug("Sending the following email to {0} (and {1}):\n{2}\n"
                "With attachments:\n\t{3}".format(
                    ", ".join(folder["owners"]),
                    ", ".join(GES_ADMINISTRATORS),
                    contents,
                    "\n\t".join(fitschecker_log_filenames)))

            return_code, return_message = email_report(folder["owners"], contents,
                attachments=fitschecker_log_filenames)
            logging.info("Email return code and message was {0} {1}".format(
                return_code, return_message))

            # Update the existing inventory
            full_inventory[path] = current_inventory

        if fitschecker_error_occurred:
            logging.warn("Refusing to update inventory on {} because a FITSCHEC"
                "KER problem was detected".format(path))


    logging.info("There were {0} files updated.".format(total_updated_files))

    # Save the updated inventory
    n_folders = len(full_inventory)
    n_files = sum([len(v) for v in full_inventory.values()])
    with open(INVENTORY_FILENAME, "w") as fp:
        yaml.dump(full_inventory, fp)

    logging.info("Updated inventory with {0} file(s) in {1} folder(s) to {2}."
        .format(n_files, n_folders, INVENTORY_FILENAME))
