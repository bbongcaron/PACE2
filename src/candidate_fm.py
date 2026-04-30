import asyncio, os, sys, json, shutil, time
from datetime import datetime

from candidate_base import BaseCandidatePACE2
from radical.asyncflow import logging

class ForceMatchingCandidate(BaseCandidatePACE2):
    """
    Defines class to manage candidates.
    """
    def __init__(self, candidate_specifications: dict, cid: int, session_dir_name: str):
        """
        Initializes pipeline, candidate specifications dictionary, and candidate id
        Args:
            candidate_specifications: dictionary containing simulation configurations
            cid: unique candidate id
        """

        BaseCandidatePACE2.__init__(self, candidate_specifications, cid, session_dir_name)

        def get_hyperparameters():
            return {}

        self.topology = os.path.join(self.candidate_pool_dir, "topol_fm.tpr")
        self.trajectory = os.path.join(self.candidate_pool_dir, "traj.trr")
        self.settings = os.path.join(self.candidate_pool_dir, "settings.xml")
        self.mapping = os.path.join(self.candidate_pool_dir, "mapping.xml")
        self.water_CG = os.path.join(self.candidate_pool_dir, "water_CG.xml")
        self.force_out = "ACE-SOL.force"
        self.pot_out = "ACE-SOL.pot"
        
        self.hyperparmaeters = get_hyperparameters()

    async def run_composite_workflow(self):

        @self.flow.block
        async def create_composite_workflow():
            self.logger.info(f"[{time.time():.2f}] {self.sysname} workflow block started")

            @self.flow.executable_task
            async def csg_fmatch(*args, task_description={"process_template": {"cwd" :self.get_task_cwd("csg_fmatch")}}):
                return f'''csg_fmatch --top {self.topology} 
                            --trj {self.trajectory} 
                            --options {self.settings} 
                            --cg "{self.mapping};{self.water_CG}"
                        '''

            @self.flow.executable_task
            async def csg_call_integrate(*args, task_description={"process_template": {"cwd" :self.get_task_cwd("csg_call_integrate")}}):
                return f"csg_call table integrate {self.force_out} {self.pot_out}"

            @self.flow.executable_task
            async def csg_call_linearop(*args, task_description={"process_template": {"cwd" :self.get_task_cwd("csg_call_linearop")}}):
                return f"csg_call table linearop {self.pot_out} {self.pot_out} -1 0"

            csg_fmatch_future = csg_fmatch()
            csg_call_integrate_future = csg_call_integrate(csg_fmatch_future)
            csg_call_linearop_future = csg_call_linearop(csg_fmatch_future, csg_call_integrate_future)
            
            await csg_call_linearop_future

            self.logger.info(f"[{time.time():.2f}] {self.sysname} workflow block completed")
        
        return await create_composite_workflow()