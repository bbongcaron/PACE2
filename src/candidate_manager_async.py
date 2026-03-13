import asyncio, time

from Task import Task
from Pipeline import Pipeline

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import LocalExecutionBackend
from radical.asyncflow import logging
from concurrent.futures import ProcessPoolExecutor

class CandidateManagerAsync:

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
        backend = await LocalExecutionBackend(ProcessPoolExecutor())
        flow = await WorkflowEngine.create(backend=backend)

        # The asyncflow.block is the equivalent of the EntK pipeline
        @flow.block
        async def create_block(self, name, pipeline: Pipeline):
            
            # A reusable higher-order function / lambda that will dynamically create asyncflow.executable_tasks for every task in the pipeline
            @flow.executable_task
            async def task_execution(task: Task):
                return task.executable + " " + task.args

            # below is the actual pipeline workflow
            for task in pipeline.tasks:
                now = time.time()
                self.logger.info(f"[{now:.2f}] {task.name} started")
                wf = await task_execution(task)
                #time.sleep(5)
                self.logger.info(wf)
                self.logger.info(f"[{time.time():.2f}] {task.name} completed")

        try:
            # try to gather the pipelines and run them in parallel (tasks within pipelines still run sequentially)
            await asyncio.gather(*[create_block(self, name=f"pipeline1", pipeline=pipeline) for i, pipeline in enumerate(self.pipelines)])
        except Exception as e:
            # log errors
            self.logger.exception(e)
        finally:
            # shutdown workflow engine
            await flow.shutdown()

    def run(self):
        asyncio.run(self.execute_pipelines())