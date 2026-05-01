import asyncio, os, sys, json, shutil, time
import xml.etree.ElementTree as ET

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

        self.topology = os.path.join(self.candidate_pool_dir, "topol_fm.tpr")
        self.trajectory = os.path.join(self.candidate_pool_dir, "traj.trr")
        self.settings = os.path.join(self.candidate_pool_dir, "settings.xml")
        self.mapping = os.path.join(self.candidate_pool_dir, "mapping.xml")
        self.water_CG = os.path.join(self.candidate_pool_dir, "water_CG.xml")
        self.force_out = "ACE-SOL.force"
        self.pot_out = "ACE-SOL.pot"
        self.potcheck = os.path.join(os.getcwd(), "fm-utils", "potcheck.py")

        def get_hyperparameters():
            settings_tree = ET.parse(self.settings)
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
                    'candidate_name':   self.sysname,
                    'interaction_name': interaction_name,
                    'bead_type_1':      bead_type_1,
                    'bead_type_2':      bead_type_2,
                    'frames_per_block': fpb,
                    'min_r':            min_r,
                    'step_size':        step_size
            }

            return hyperparametersDict
        
        self.hyperparameters = get_hyperparameters()

    async def run_composite_workflow(self):

        @self.flow.block
        async def create_composite_workflow():

            @self.flow.executable_task
            async def csg_fmatch(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd()}}):
                return f'''csg_fmatch --top {self.topology} 
                            --trj {self.trajectory} 
                            --options {self.settings} 
                            --cg "{self.mapping};{self.water_CG}"
                        '''

            @self.flow.executable_task
            async def csg_call_integrate(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd()}}):
                return f"csg_call table integrate {self.force_out} {self.pot_out}"

            @self.flow.executable_task
            async def csg_call_linearop(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd()}}):
                return f"csg_call table linearop {self.pot_out} {self.pot_out} -1 0"

            @self.flow.executable_task
            async def analyze_potential_curve(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd()}}):
                return f"python {self.potcheck} {os.path.join(self.get_candidate_cwd(), self.pot_out)}"

            self.logger.info(f"[{time.time():.2f}] {self.sysname} workflow block started")

            json.dump(self.hyperparameters, open(os.path.join(self.get_candidate_cwd(), "hyperparameters.json"), "w"))
            csg_fmatch_future = csg_fmatch()
            csg_call_integrate_future = csg_call_integrate(csg_fmatch_future)
            csg_call_linearop_future = csg_call_linearop(csg_call_integrate_future)
            analyze_potential_curve_future = analyze_potential_curve(csg_call_linearop_future)            

            await analyze_potential_curve_future
            self.logger.info(f"[{time.time():.2f}] {self.sysname} [{analyze_potential_curve_future.result()}]")
            
            self.logger.info(f"[{time.time():.2f}] {self.sysname} workflow block completed")
        
        return await create_composite_workflow()