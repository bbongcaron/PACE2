import asyncio, time

from Task import Task
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

        1) can we name the task_execution fxn as seen in the logger?
        
    '''

    def __init__(self, pipelines: list[Pipeline]):
        self.pipelines = pipelines
        self.logger = logging.init_default_logger(log_level="DEBUG", output_file="logs.json", structured_logging=True)

    async def execute_pipelines(self):
        import multiprocessing as mp

        # Set Dragon as multiprocessing backend
        mp.set_start_method("dragon")
        # Create Dragon Batch backend (4 nodes with 128 workers each)
        nodes = 1
        backend = await DragonExecutionBackendV3(
            num_workers=nodes * mp.cpu_count(),
            disable_background_batching=False,
        )

        #backend = await LocalExecutionBackend(ProcessPoolExecutor())
        flow = await WorkflowEngine.create(backend=backend)

        # The asyncflow.block is the equivalent of the EntK pipeline
        @flow.block
        async def create_block(self, name, pipeline: Pipeline):
            
            # below is the actual pipeline workflow
            now = time.time()
            self.logger.info(f"[{now:.2f}] {name} started")

            futures = []
            for task in pipeline.tasks:
                task_backend_specific_kwargs = {
                    "process_template": {"cwd": task.cwd}
                }

                # A reusable higher-order function / lambda that will dynamically create asyncflow.executable_tasks for every task in the pipeline
                @flow.executable_task
                async def command_execution(*args, task_description=task_backend_specific_kwargs):            
                    return args[0]

                self.logger.info(f"{task.name} cwd should be {task.cwd}")
                for command in task.commands:
                    wf = command_execution(command, *futures)
                    futures.append(wf)

            await asyncio.gather(*futures)

            self.logger.info(f"[{time.time():.2f}] {name} completed")

        try:
            # try to gather the pipelines and run them in parallel (tasks within pipelines still run sequentially)
            await asyncio.gather(*[create_block(self, name=f"pipeline{i}", pipeline=pipeline) for i, pipeline in enumerate(self.pipelines)])
        except Exception as e:
            # log errors
            self.logger.exception(e)
        finally:
            # shutdown workflow engine
            await flow.shutdown()

    def run(self):
        asyncio.run(self.execute_pipelines())