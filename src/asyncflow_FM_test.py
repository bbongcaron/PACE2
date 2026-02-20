import asyncio, os
from Task import Task
from Pipeline import Pipeline
from candidate_manager_async import CandidateManagerAsync

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
    candidate_dir = os.path.join(candidate_pool, candidate)

    ## Task 1: perform csg_fmatch
    task1 = Task()
    task1.executable = "csg_fmatch"
    task1.args = f"--top {candidate_dir}/topol_fm.tpr --trj {candidate_dir}/traj.trr --options {candidate_dir}/settings.xml --cg '{candidate_dir}/mapping.xml;{candidate_dir}/water_CG.xml'"

    # Task 2: integrate forces w.r.t. position
    task2 = Task()
    task2.executable = "csg_call"
    task2.args = f"table integrate ACE-SOL{i+1}.force ACE-SOL{i+1}.pot"

    # Task 3: multiply integrated forces by -1, since F = -dU/dx
    task3 = Task()
    task3.executable = "csg_call"
    task3.args = f"table linearop ACE-SOL{i+1}.pot ACE-SOL{i+1}.pot -1 0"
    
    candidate_pipeline.add_task(task1)
    candidate_pipeline.add_task(task2)
    candidate_pipeline.add_task(task3)

    pipelines.append(candidate_pipeline)

candidate_manager = CandidateManagerAsync(pipelines)
candidate_manager.run()