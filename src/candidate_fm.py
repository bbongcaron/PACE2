import asyncio, os, sys, json, shutil
from datetime import datetime

from candidate_base import BaseCandidatePACE2

class ForceMatchingCandidate(BaseCandidatePACE2):
    """
    Defines class to manage candidates.
    """
    def __init__(self, candidate_specifications: dict, cid: int, session_dir_name: str, hyperparmeters: dict):
        """
        Initializes pipeline, candidate specifications dictionary, and candidate id
        Args:
            candidate_specifications: dictionary containing simulation configurations
            cid: unique candidate id
        """

        BaseCandidatePACE2.__init__(self, candidate_specifications, cid, session_dir_name)

        self.topology = "topol_fm.tpr"
        self.trajectory = "traj.trr"
        self.settings = "settings.xml"
        self.mapping = "mapping.xml"
        self.water_CG = "water_CG.xml"

    def create_candidate_pipeline(self) -> Pipeline:
        """
        Creates candidate pipeline with appropriate stages
        Returns:
            Pipeline: pipeline containing appropriate Tasks (scientific processes) for the candidate
        """

        basename = self.candidate_specifications['basename']
        sysname = basename + "." + str(self.cid)
        self.pipeline.name = sysname
        candidate_pool_dir = os.path.join(os.getcwd(), basename, sysname)
        
        #
        #   TO-DO: dynamic creation of Asynchronous Workflow of Tasks based on len(self.candidate_specifications["tasks"])
        #
        task = Task(name=self.candidate_specifications["tasks"][0]["name"], 
                    commands=self.candidate_specifications["tasks"][0]["commands"], 
                    candidateFiles=self.candidate_specifications["tasks"][0]["candidateFiles"],
                    cwd=os.path.join(os.getcwd(), self.session_dir_name, sysname, self.candidate_specifications["tasks"][0]["name"]), 
                    ranks=self.candidate_specifications["tasks"][0]["ranks"], 
                    threads=self.candidate_specifications["tasks"][0]["threads"])
        
        for inputFile in task.candidateFiles:
            full_path = os.path.join(candidate_pool_dir, inputFile)
            destination_path = os.path.join(task.cwd, inputFile)
            shutil.copy2(full_path, destination_path)

        self.pipeline.add_task(task)

        return self.pipeline