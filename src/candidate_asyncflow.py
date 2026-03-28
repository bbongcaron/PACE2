import asyncio, os, sys, json, shutil
from datetime import datetime

from Task import Task
from Pipeline import Pipeline
from candidate_manager_asyncflow import CandidateManager

def sandbox():
    '''
        In this test example, ACEALAGLY candidate directory has 2 force matching candidates (both proven to work manually)

        Each candidate pipeline consists of:
            1) csg_fmatch to obtain the .force file
            2) csg_call table integrate to integrate the force
            3) csg_call table linearop -1 to multiply the .pot file produced by 2)

        Note: F = - dU/dx
    '''

    config = {
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

    run_datetime = datetime.now().strftime("%Y-%b-%d_%H-%M-%S")
    session_dir_name = f"session_{config['basename']}_{run_datetime}"

    pipelines = []

    # loop through all candidates in the candidate pool
    for i in range(config["candidates"]):

        # initialize a candidate pipeline
        candidate_pipeline = Pipeline()

        # build candidate dir paths based on config and candidate #
        candidate = f"{config['basename']}.{i}"
        candidate_dir = os.path.join(os.getcwd(), config["basename"], candidate)

        task = Task(name=config["tasks"][0]["name"], 
                    commands=config["tasks"][0]["commands"], 
                    candidateFiles=config["tasks"][0]["candidateFiles"],
                    cwd=os.path.join(os.getcwd(), session_dir_name, candidate, config["tasks"][0]["name"]), 
                    ranks=config["tasks"][0]["ranks"], 
                    threads=config["tasks"][0]["threads"])

        for inputFile in task.candidateFiles:
            full_path = os.path.join(candidate_dir, inputFile)
            destination_path = os.path.join(task.cwd, inputFile)
            shutil.copy2(full_path, destination_path)

        candidate_pipeline.add_task(task)

        pipelines.append(candidate_pipeline)

    candidate_manager = CandidateManager(pipelines, session_dir_name)
    candidate_manager.run()

if __name__ == "__main__":
    sandbox()