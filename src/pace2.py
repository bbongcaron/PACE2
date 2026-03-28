#!/usr/bin/env python
import os, sys, json, pprint
import xml.etree.ElementTree as ET

from typing import Dict, Tuple
from queue import Queue
from datetime import datetime

from candidate_asyncflow import Candidate
from candidate_manager_asyncflow import CandidateManager

def read_jsons() -> dict:
    candidate_specifications_dict = {}

    #
    #   Insert logic to read input JSON here
    #

    ### for now, manually populated config here:
    
    candidate_specifications_dict = {
        "basename"      :   "ACEALAGLY",
        "candidates"    :   2,
        "num_workers"   :   16,
        "tasks"         :   [
            {
                "name"              :   "force-matching",
                "ranks"             :   1,
                "threads"           :   16,
                "gpus"              :   0,
                "candidateFiles"    :   [
                    "topol_fm.tpr",
                    "traj.trr",
                    "settings.xml",
                    "mapping.xml",
                    "water_CG.xml"
                ],
                "priorTaskFiles"    : [],
                "commands": [
                    {
                        "executable": "csg_fmatch",
                        "args": "--top topol_fm.tpr --trj traj.trr --options settings.xml --cg 'mapping.xml;water_CG.xml'"
                    },
                    {
                        "executable": "csg_call",
                        "args": "table integrate ACE-SOL.force ACE-SOL.pot"
                    },
                                {
                        "executable": "csg_call",
                        "args": "table linearop ACE-SOL.pot ACE-SOL.pot -1 0"
                    }
                ]
            }
        ]
    }

    return candidate_specifications_dict


def main() -> None:
    """
    Main pace driver.
    """
    # Read from simconfig.json
    candidate_specifications_dict = read_jsons()

    # FOR THARUN: uncomment the below two commented lines to debug your read_json() function.
    # pprint.pprint(candidate_specifications_dict)
    # return

    run_datetime = datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
    session_dir_name = f"session_{candidate_specifications_dict['basename']}_{run_datetime}"

    # Generate all pipelines
    pipelines = []

    for cid in range(candidate_specifications_dict['candidates']):
        # Create candidate pipeline
        candidate = Candidate(candidate_specifications_dict, cid, session_dir_name)
        candidate.create_candidate_pipeline()
        pipelines.append(candidate.pipeline)

    # Create candidate manager and run
    candidate_manager = CandidateManager(pipelines, session_dir_name)
    candidate_manager.run()


if __name__ == '__main__':
    main()
