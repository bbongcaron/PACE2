import asyncio, time, os

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import LocalExecutionBackend
from radical.asyncflow import logging
from concurrent.futures import ProcessPoolExecutor
from rhapsody.backends import DragonExecutionBackendV3

class CandidateManager:
    """
        The AsyncFlow rendition of CandidateManger
    """

    def __init__(self, candidates: list, session_dir_name: str, num_workers=1):
        self.candidates = candidates
        self.num_workers = num_workers
        self.logger = logging.init_default_logger(log_level="DEBUG", output_file=os.path.join(session_dir_name, "logs.json"), structured_logging=True)

    async def _get_workflow_manager(self):
        """
        Configures and obtains the asynchronous workflow manager.

        Returns:
            WorkflowEngine: The asynchronous workflow manager.
        """
        import multiprocessing as mp

        # Set Dragon as multiprocessing backend
        mp.set_start_method("dragon")

        backend = await DragonExecutionBackendV3(
            num_workers=self.num_workers,
            disable_background_batching=False,
        )

        #backend = await LocalExecutionBackend(ProcessPoolExecutor())
        return await WorkflowEngine.create(backend=backend)

    async def execute_candidate_workflows(self):
        """
        Executes Pipeline coroutines concurrently.
        """
        flow = await self._get_workflow_manager()
        for candidate in self.candidates:
            candidate.set_workflow_engine(flow)
            candidate.set_logger(self.logger)

        try:
            await asyncio.gather(*[candidate.run_composite_workflow() for candidate in self.candidates])
        except Exception as e:
            self.logger.exception(e)
        finally:
            await flow.shutdown()

    def run(self):
        """
        Runs pipelines in workflow.
        """
        asyncio.run(self.execute_candidate_workflows())