import asyncio, os, sys, json
from Task import Task
from Pipeline import Pipeline
from candidate_manager_asyncflow import CandidateManager
from typing import Dict, Tuple

class Candidate:
    """
    Defines class to manage candidates.
    """
    def __init__(self, candidate_specifications: Dict, cid: int):
        """
        Initializes pipeline, candidate specifications dictionary, and candidate id
        Args:
            candidate_specifications: dictionary containing simulation configurations
            cid: unique candidate id
        """
        self.pipeline = Pipeline()
        self.candidate_specifications = candidate_specifications
        self.cid = cid
        self.cycle_count = 0

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

def print_dict(dict: Dict):
    for key in dict:
        print(f"{key}:\t{dict[key]}")

def sandbox():
    # Read from simconfig.json
    #candidate_specifications_dict, resource_dict = read_jsons()
    #print_dict(candidate_specifications_dict)
    '''
        In this test example, ACEALAGLY candidate directory has 2 force matching candidates (both proven to work manually)

        Each candidate pipeline consists of:
            1) csg_fmatch to obtain the .force file
            2) csg_call table integrate to integrate the force
            3) csg_call table linearop -1 to multiply the .pot file produced by 2)

        Note: F = - dU/dx
    '''
    candidate_pool = "ACEALAGLY"

    pipelines = []

    # loop through all candidates in the candidate pool
    for i, candidate in enumerate(os.listdir(candidate_pool)):
        # initialize a candidate pipeline
        candidate_pipeline = Pipeline()
        candidate_dir = os.path.join(os.getcwd(), candidate_pool, candidate)

        ## Task 1: perform csg_fmatch
        task1 = Task()
        #task1.executable = "csg_fmatch"
        #task1.args = f"--top {candidate_dir}/topol_fm.tpr --trj {candidate_dir}/traj.trr --options {candidate_dir}/settings.xml --cg '{candidate_dir}/mapping.xml\;{candidate_dir}/water_CG.xml'"
        task1.executable = "echo"
        task1.args = f"this is task 1 of candidate {i}"

        # Task 2: integrate forces w.r.t. position
        task2 = Task()
        #task2.executable = "csg_call"
        #task2.args = f"table integrate ACE-SOL{i+1}.force ACE-SOL{i+1}.pot"
        task2.executable = "echo"
        task2.args = f"this is task 2 of candidate {i}"

        # Task 3: multiply integrated forces by -1, since F = -dU/dx
        task3 = Task()
        #task3.executable = "csg_call"
        #task3.args = f"table linearop ACE-SOL{i+1}.pot ACE-SOL{i+1}.pot -1 0"
        task3.executable = "echo"
        task3.args = f"this is task 3 of candidate {i}"

        candidate_pipeline.add_task(task1)
        candidate_pipeline.add_task(task2)
        candidate_pipeline.add_task(task3)

        pipelines.append(candidate_pipeline)

    candidate_manager = CandidateManager(pipelines)
    candidate_manager.run()

if __name__ == "__main__":
    sandbox()