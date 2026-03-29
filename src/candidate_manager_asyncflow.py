import asyncio, time, os

from Pipeline import Pipeline

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import LocalExecutionBackend
from radical.asyncflow import logging
from concurrent.futures import ProcessPoolExecutor
from rhapsody.backends import DragonExecutionBackendV3

class CandidateManager:

    '''
        The AsyncFlow prototype rendition of CandidateManger

        Note that assembly of the asyncflow blocks/executable_tasks had to be in the same scope as the asyncflow backend/workflow engine

        So, there is some lower level logic present that we may want to create another layer of abstraction that deals with assembly
        of the workflows
        
    '''

    def __init__(self, pipelines: list[Pipeline], session_dir_name: str):
        self.pipelines = pipelines
        self.logger = logging.init_default_logger(log_level="DEBUG", output_file=os.path.join(session_dir_name, "logs.json"), structured_logging=True)
        
    async def get_workflow_manager(self):
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
        flow = await self.get_workflow_manager()

        try:
            await asyncio.gather(*[pipeline.run(flow, self.logger) for pipeline in self.pipelines])
        except Exception as e:
            self.logger.exception(e)
        finally:
            await flow.shutdown()

    def run(self):
        asyncio.run(self.execute_pipelines())