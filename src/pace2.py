#!/usr/bin/env python


import os, sys, json
import xml.etree.ElementTree as ET

from candidate import Candidate
from candidate_manager import CandidateManager
from typing import Dict, Tuple
from queue import Queue

def read_jsons() -> Tuple[Dict, Dict]:
    """
    Read from specified resource and simulation json configuration files.
    Returns:
        Tuple[Dict, Dict] specifying the simulation and resource configurations, respectively
    """
    simconfig = sys.argv[1]  # simulation configuration
    resconfig = sys.argv[2]  # resource configuration
    sim_dict = {}

    # Load simulation and resource json files
    with open(simconfig) as simconf:
        simdata = json.load(simconf)

    with open(resconfig) as resconf:
        resdata = json.load(resconf)

    # Assign simulation configs
    sim_dict['basename'] = simdata["basename"]
    sim_dict['candidates'] = simdata["candidates"]
    sim_dict['cycle_max'] = simdata["cycle_max"]
    sim_dict['pipeline_cores'] = simdata["pipeline_cores"]

    # Executables, args and pre exec for all the tasks
    
    sim_dict['md_executable']     = simdata["md_executable"]
    sim_dict['md_args']           = simdata["md_args"].split()
    sim_dict['md_pre_exec']       = simdata["md_pre_exec"]

    sim_dict['pre_md_executable'] = simdata["pre_md_executable"]
    sim_dict['pre_md_args']       = simdata["pre_md_args"].split()
    sim_dict['pre_md_pre_exec']   = simdata["pre_md_pre_exec"]
        
    sim_dict['an_pre_exec']       = simdata["an_pre_exec"]
    sim_dict['an_executable']     = simdata["an_executable"]
    sim_dict['an_args']           = simdata["an_args"].split()

    ## add-on for agnosticity
    
    sim_dict['chead_files']       = simdata["chead_files"].split()
    sim_dict['md_binary']     = simdata["md_binary"]
    sim_dict['structure_in']     = simdata["structure_in"]
    sim_dict['structure_out']     = simdata["structure_out"]

    sim_dict['pilot_cores'] = resdata["cpus"]

    # Assign resource configs when all fields are specified
    try:
        res_dict = {
            "resource": str(resdata["resource"]),
            "walltime": int(resdata["walltime"]),
            "cpus": sim_dict['pilot_cores'],
            "gpus_per_node": int(resdata["gpus_per_node"]),
            "access_schema": str(resdata["access_schema"]),
            "queue": str(resdata["queue"]),
            "project": str(resdata["project"]),
        }

    # Assign resource configs when minimum fields specified
    except:
        res_dict = {
            "resource": str(resdata["resource"]),
            "walltime": int(resdata["walltime"]),
            "cpus": sim_dict['pilot_cores'],

        }
    return sim_dict, res_dict

def get_forcematching_params(pipeline):
    SETTINGS_PATH = "settings.xml"
    basename = pipeline.name.rsplit('.')[0]

    settings_path = os.path.join(basename, pipeline.name, SETTINGS_PATH)
    settings_tree = ET.parse(settings_path)
    settings_root = settings_tree.getroot()

    fmatch_block = settings_root.find('fmatch')
    fpb = fmatch_block.find("frames_per_block").text

    nonbonded_block = settings_root.find('non-bonded')
    interaction_name = nonbonded_block.find('name').text
    bead_type_1 = nonbonded_block.find('type1').text
    bead_type_2 = nonbonded_block.find('type2').text

    nonbonded_fmatch_block = nonbonded_block.find('fmatch')
    min_r = nonbonded_fmatch_block.find('min').text
    step_size = nonbonded_fmatch_block.find('step').text

    hyperparametersDict = {
            'pipeline_name':    pipeline.name,
            'interaction_name': interaction_name,
            'bead_type_1':      bead_type_1,
            'bead_type_2':      bead_type_2,
            'frames_per_block': fpb,
            'min_r':            min_r,
            'step_size':        step_size
    }

    return hyperparametersDict

def main() -> None:
    """
    Main pace driver.
    """
    # Read from simconfig.json
    candidate_specifications_dict, resource_dict = read_jsons()


    # Obtain pre-run environment variables
    if 'RADICAL_ENTK_VERBOSE' in os.environ:
        os.environ['RADICAL_ENTK_REPORT'] = 'True'
    hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
    port = os.environ.get('RMQ_PORT', 32769)
    username = os.environ.get('RMQ_USERNAME')
    password = os.environ.get('RMQ_PASSWORD')

    # Generate all pipelines
    pipelines = []
    hyperparameters = Queue(maxsize=candidate_specifications_dict['candidates'])

    for cid in range(candidate_specifications_dict['candidates']):
        # Create candidate pipeline
        candidate = Candidate(candidate_specifications_dict, cid)
        candidate.create_candidate_pipeline()
        pipelines.append(candidate.pipeline)

    # obtain FM hyperparameters for the candidate
    if candidate_specifications_dict['pre_md_executable'] == "csg_fmatch":
        for pipeline in pipelines:
            hyperparameters.put(get_forcematching_params(pipeline))

    # Create candidate manager and run
    candidate_manager = CandidateManager(hostname, port, username, password, resource_dict, pipelines)
    candidate_manager.run()


if __name__ == '__main__':
    main()
