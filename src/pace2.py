#!/usr/bin/env python
import os, sys, json, pprint
import xml.etree.ElementTree as ET

from typing import Dict, Tuple
from queue import Queue
from datetime import datetime

from candidate_fm import ForceMatchingCandidate
from candidate_manager import CandidateManager


def read_jsons() -> dict:
    """
    Reads in an JSON file whose filepath is supplied via argv 
    and creates an analagous Python dictionary contating the
    user-furnished parameters

    Args:
        None.

    Returns:
        dict: The dictionary analogue of the user-furnished JSON file.
    """

    candidate_specifications_dict = {}

    #
    #   Insert logic to read input JSON here
    #
    #   You will have to read in a JSON filepath via. sys.argv
    #

    ### for now, manually populated config here:
    
    candidate_specifications_dict = {
        "basename"      :   "FFF",
        "candidates"    :   60,
        "num_workers"   :   16,
    }

    return candidate_specifications_dict


def main() -> None:
    """
    Main pace driver.
    """
    # Read from simconfig.json
    candidate_specifications_dict = read_jsons()

    # FOR THARUN: 
    # 
    # To run this driver script:
    # 1) cd /path/to/PACE2
    # 2) python src/pace2.py  
    # 
    # uncomment the below two commented lines to debug your read_json() function.
    #
    # pprint.pprint(candidate_specifications_dict)
    # return

    run_datetime = datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
    session_dir_name = f"session_{candidate_specifications_dict['basename']}_{run_datetime}"

    # Generate all pipelines
    candidates = []

    for cid in range(candidate_specifications_dict['candidates']):
        # Create candidate pipeline
        candidate = ForceMatchingCandidate(candidate_specifications_dict, cid, session_dir_name)
        candidates.append(candidate)

    # Create candidate manager and run
    candidate_manager = CandidateManager(candidates, session_dir_name, num_workers=candidate_specifications_dict['num_workers'])
    candidate_manager.run()


if __name__ == '__main__':
    main()
