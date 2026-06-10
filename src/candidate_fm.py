import asyncio, os, sys, json, shutil, time, csv, glob, parameter_states
import xml.etree.ElementTree as ET

from datetime import datetime
from candidate_base import BaseCandidatePACE2
from radical.asyncflow import logging
from radical.asyncflow.errors import DependencyFailureError

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
        self.water_CG = os.path.join(os.getcwd(), self.candidate_pool_dir, "water_CG.xml")
        self.potcheck = os.path.join(os.getcwd(), "fm-utils", "potcheck.py")

        self.success_criteria = ["1 1 0", "1 1 1"]
        
        def read_hyperparameters():
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
        
        self.hyperparameters = read_hyperparameters()
        self.force_out = f"{self.hyperparameters['interaction_name']}.force"
        self.pot_out = f"{self.hyperparameters['interaction_name']}.pot"
        self.convert_xvg_input = f"input_{self.hyperparameters['interaction_name']}.pot"
        self.convert_xvg_output = f"table_{self.hyperparameters['interaction_name']}.xvg"

    async def run_composite_workflow(self):

        @self.flow.executable_task
        async def csg_fmatch(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd()}}):
            self.logger.info(f"[{time.time():.2f}] {self.sysname} csg_fmatch task queued")
            return f'''csg_fmatch --top {self.topology} 
                        --trj {self.trajectory} 
                        --options {self.settings} 
                        --cg "{self.mapping};{self.water_CG}" -v 
                    '''

        @self.flow.executable_task
        async def csg_call_integrate(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd() }}):
            self.logger.info(f"[{time.time():.2f}] {self.sysname} csg_integrate task queued")
            return f"csg_call table integrate {self.force_out} {self.pot_out}"

        @self.flow.executable_task
        async def csg_call_linearop(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd()}}):
            self.logger.info(f"[{time.time():.2f}] {self.sysname} csg_linearop task queued")
            return f"csg_call table linearop {self.pot_out} {self.pot_out} -1 0"

        @self.flow.executable_task
        async def analyze_potential_curve(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd()}}):
            self.logger.info(f"[{time.time():.2f}] {self.sysname} potcheck task queued")
            return f"python {self.potcheck} {os.path.join(self.get_candidate_cwd(), self.pot_out)}"

        @self.flow.executable_task
        async def copy_potential(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd()}}):
            self.logger.info(f"[{time.time():.2f}] {self.sysname} copy .pot task queued")
            return f"cp {os.path.join(self.get_candidate_cwd(), self.pot_out)} {os.path.join(self.get_candidate_cwd(), self.convert_xvg_input)}"

        @self.flow.executable_task
        async def pot_to_xvg(*args, task_description={"process_template": {"cwd" :self.get_candidate_cwd()}}):
            self.logger.info(f"[{time.time():.2f}] {self.sysname} .pot to .xvg task queued")
            return f'''csg_call --ia-type non-bonded --ia-name {self.hyperparameters['interaction_name']} 
                        --options {self.settings} 
                        convert_potential gromacs 
                        --clean {os.path.join(self.get_candidate_cwd(), self.convert_xvg_input)} 
                        {os.path.join(self.get_candidate_cwd(), self.convert_xvg_output)}'''

        @self.flow.block
        async def create_composite_workflow():
            json.dump(self.hyperparameters, open(os.path.join(self.get_candidate_cwd(), "hyperparameters.json"), "w"))

            try:
                ## Check if this interaction already has an acceptable potential found
                interaction_potential_exists = False
                async with parameter_states.DB_LOCK:
                    interaction_candidates_param = [hyperamaterSet for hyperamaterSet in parameter_states.DATABASE if hyperamaterSet['interaction_name'] == self.hyperparameters['interaction_name']]
                    if any(criterion in hyperamaterSet["result"] for hyperamaterSet in interaction_candidates_param for criterion in self.success_criteria):
                        interaction_potential_exists = True
                        
                if not interaction_potential_exists:
                    self.logger.info(f"[{time.time():.2f}] {self.sysname} workflow block started")

                    ## Workflow tasks
                    csg_fmatch_future = csg_fmatch()
                    csg_call_integrate_future = csg_call_integrate(csg_fmatch_future)
                    csg_call_linearop_future = csg_call_linearop(csg_call_integrate_future)
                    analyze_potential_curve_future = analyze_potential_curve(csg_call_linearop_future)            
                    copy_potential_future = copy_potential(analyze_potential_curve_future)
                    pot_to_xvg_future = pot_to_xvg(copy_potential_future)

                    await pot_to_xvg_future

                    self.logger.info(f"[{time.time():.2f}] {self.sysname} workflow block completed")

                    potential_curve_png = glob.glob(os.path.join(self.get_candidate_cwd(), "*.png"))
                    self.hyperparameters["result"] = os.path.splitext(os.path.basename(potential_curve_png[0]))[0] if len(potential_curve_png) > 0 else 'error'

                else:
                    self.logger.info(f"[{time.time():.2f}] {self.sysname} workflow block skipped due to an existing potential previously for {self.hyperparameters['interaction_name']}")
                    self.hyperparameters["result"] = "skipped"

            except DependencyFailureError as e:
                # If some candidates are expected to fail, radical.asyncflow.errors.DependencyFailureErrors must
                # be caught to prevent dragon.native.process_group.DragonUserCodeError appearing, which shuts down
                # the PACE2 job and prevents successful candidates from continuing their workflow.
                self.logger.info(f"[{time.time():.2f}] {self.sysname} workflow block failed due to a task dependency failure (i.e. csg_fmatch failed)")
                self.hyperparameters["result"] = 'failed'

            except Exception as e:
                self.logger.exception(f"[{time.time():.2f}] {self.sysname} workflow block failed: {repr(e)}")
                raise

            finally:
                return self.hyperparameters
        
        return await create_composite_workflow()
