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
        if not os.path.isdir(self.session_dir_name):
            os.mkdir(self.session_dir_name)

    def set_workflow_engine(self, flow: WorkflowEngine):
        self.flow = flow
    
    def set_logger(self, logger: logging):
        self.logger = logger

    def get_candidate_cwd(self, taskname=''):
        task_dir = os.path.join(os.getcwd(), self.session_dir_name, self.sysname, taskname) if taskname != '' else os.path.join(os.getcwd(), self.session_dir_name, self.sysname) 
        if not os.path.isdir(task_dir):
            os.mkdir(task_dir)
        return task_dir

    async def run_composite_workflow(self):
        """
        Creates an radical.asyncflow Composite Workflow Block to be submitted to
        the asyncrhonous workflow manager.

        Returns:
            An awaited radical.asyncflow Composite Workflow Block.
        """
    
        @self.flow.function_task
        async def task1(name: str):
            now = time.time()
            print(f"[{now:.2f}] {name} started")
            await asyncio.sleep(0.5)  # simulate work
            print(f"[{time.time():.2f}] {name} completed")
            return now

        @self.flow.block
        async def create_composite_workflow(self):
            self.logger.info(f"[{time.time():.2f}] placeholder workflow block started")
            await task1("task1")
            self.logger.info(f"[{time.time():.2f}] placeholder workflow block completed")

        return await self.create_composite_workflow()