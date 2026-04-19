import asyncio, time, os

from Pipeline import Pipeline

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import LocalExecutionBackend
from radical.asyncflow import logging
from concurrent.futures import ProcessPoolExecutor
from rhapsody.backends import DragonExecutionBackendV3

class CandidateManager:
    """
        The AsyncFlow rendition of CandidateManger
    """

    def __init__(self, pipelines: list[Pipeline], session_dir_name: str):
        self.pipelines = pipelines
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

        nodes = 1
        backend = await DragonExecutionBackendV3(
            num_workers=nodes * mp.cpu_count(),
            disable_background_batching=False,
        )

        #backend = await LocalExecutionBackend(ProcessPoolExecutor())
        return await WorkflowEngine.create(backend=backend)

    async def execute_pipelines(self):
        """
        Executes Pipeline coroutines concurrently.
        """
        flow = await self._get_workflow_manager()

        try:
            for pipeline in self.pipelines: 
                pipeline.flow = flow

            await asyncio.gather(*[pipeline.run(self.logger) for pipeline in self.pipelines])
        except Exception as e:
            self.logger.exception(e)
        finally:
            await flow.shutdown()

    def run(self):
        """
        Runs pipelines in workflow.
        """
        asyncio.run(self.execute_pipelines())