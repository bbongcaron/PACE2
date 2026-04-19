from radical.asyncflow import WorkflowEngine
from radical.asyncflow import logging
import os, time

class BaseCandidatePACE2:
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

        self.candidate_specifications = candidate_specifications

        self.basename = self.candidate_specifications['basename']
        self.cid = str(cid)
        self.sysname = self.basename + "." + self.cid
        self.candidate_pool_dir = os.path.join(os.getcwd(), self.basename, self.sysname)

        self.session_dir_name = session_dir_name

    def set_workflow_engine(self, flow: WorkflowEngine):
        self.flow = flow
    
    def set_logger(self, logger: logging):
        self.logger = logger

    def get_task_cwd(self, taskname: str):
        return os.path.join(os.getcwd(), self.session_dir_name, self.sysname, taskname)

    async def run_composite_workflow(self):
        """
        Creates an radical.asyncflow Composite Workflow Block to be submitted to
        the asyncrhonous workflow manager.

        Returns:
            An awaited radical.asyncflow Composite Workflow Block.
        """
        @self.flow.block
        async def create_composite_workflow(self):
            self.logger.info(f"[{time.time():.2f}] placeholder workflow block started")
            await time.sleep(2)
            self.logger.info(f"[{time.time():.2f}] placeholder workflow block completed")

        return await self.create_composite_workflow()