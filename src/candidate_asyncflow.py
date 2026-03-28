import asyncio, os, sys, json
from datetime import datetime
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

def create_session_directory(candidate_pool_name: str):
    run_datetime = datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
    session_dir_name = f"session_{candidate_pool_name}_{run_datetime}"
    
    os.mkdir(session_dir_name)

    candidate_directories = [name for name in os.listdir(candidate_pool_name) if os.path.isdir(os.path.join(candidate_pool_name, name))]

    for candidate_directory in candidate_directories:
        os.mkdir(os.path.join(session_dir_name, candidate_directory))
    
    return session_dir_name


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
    session_root_dir = create_session_directory(candidate_pool)

    pipelines = []

    # loop through all candidates in the candidate pool
    for i, candidate in enumerate(os.listdir(candidate_pool)):
        # initialize a candidate pipeline
        candidate_pipeline = Pipeline()
        candidate_dir = os.path.join(os.getcwd(), candidate_pool, candidate)

        commands = [
            {
                "executable": "csg_fmatch",
                "args": f"--top {candidate_dir}/topol_fm.tpr --trj {candidate_dir}/traj.trr --options {candidate_dir}/settings.xml --cg '{candidate_dir}/mapping.xml;{candidate_dir}/water_CG.xml'"
            },
            {
                "executable": "csg_call",
                "args": f"table integrate ACE-SOL{i+1}.force ACE-SOL{i+1}.pot"
            },
                        {
                "executable": "csg_call",
                "args": f"table linearop ACE-SOL{i+1}.pot ACE-SOL{i+1}.pot -1 0"
            }
        ]

        inputFiles = [
            f"{candidate_dir}/topol_fm.tpr",
            f"{candidate_dir}/traj.trr",
            f"{candidate_dir}/settings.xml",
            f"{candidate_dir}/mapping.xml",
            f"{candidate_dir}/water_CG.xml"
        ]

        ranks = 1
        threads = 16

        task = Task(name='force matching', 
                    commands=commands, 
                    inputFiles= inputFiles,
                    cwd=os.path.join(os.getcwd(), session_root_dir, candidate), 
                    ranks=ranks, 
                    threads=threads)

        candidate_pipeline.add_task(task)

        pipelines.append(candidate_pipeline)

    candidate_manager = CandidateManager(pipelines)
    candidate_manager.run()

if __name__ == "__main__":
    sandbox()