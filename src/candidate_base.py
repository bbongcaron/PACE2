from Pipeline import Pipeline
from candidate_manager_asyncflow import CandidateManager

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
        self.cid = cid
        self.cycle_count = 0
        self.session_dir_name = session_dir_name

    def create_candidate_pipeline(self) -> Pipeline:
        pass

    async def run_workflow(self, flow: WorkflowEngine, logger: logging):
        """
        Creates an radical.asyncflow Composite Workflow Block to be submitted to
        the asyncrhonous workflow manager.

        Args:
            flow    : the asynchronous workflow manager
            logger  : the log message manager
        
        Returns:
            An awaited radical.asyncflow Composite Workflow Block (Pipeline).
        """
        @flow.block
        async def create_block(pipeline: Pipeline):
            logger.info(f"[{time.time():.2f}] {pipeline.name} pipeline started")

            # By definition, Task_i can execute only after all Tasks up to Task_(i-1)
            # have completed execution. Therefore, futures are not collected, and
            # Tasks are awaited at every iteration of the Task list.
            for i, task in enumerate(pipeline.tasks): 
                #
                # TO-DO: Obtain intermediate files from Task_(i-1) cwd
                #        and copy them to current Task_i cwd.
                #           
                #        A function that belongs to the Task class should be called
                #        here, and awaited, if function is asynchronous.
                #
                await task.run(flow)

            logger.info(f"[{time.time():.2f}] {pipeline.name} pipeline completed")

        return await create_block(self)