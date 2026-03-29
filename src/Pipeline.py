import asyncio, time
from Task import Task

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import logging

class Pipeline:
    """
    A Pipeline represents a collection of scientific processes (Tasks) that have a linear 
    temporal execution order. Each ```Task_i``` can execute only after all Tasks up to
    ```Task_(i-1)``` have completed execution.
    """

    def __init__(self, name=''):
        self.name = name
        self.tasks = []

    def add_task(self, task: Task):
        """
        Appends a Task to the Pipeline.

        Args:
            flow    : the Task to add to the Pipeline
        """
        self.tasks.append(task)
    
    async def run(self, flow: WorkflowEngine, logger: logging):
        """
        Creates an radical.asyncflow Composite Workflow Block to be submitted to
        the asyncrhonous workflow manager.

        Args:
            flow    : the asynchronous workflow manager
            logger  : the log message manager
        """
        @flow.block
        async def create_block(pipeline: Pipeline):
            logger.info(f"[{time.time():.2f}] {pipeline.name} pipeline started")

            for task in pipeline.tasks: await task.run(flow)

            logger.info(f"[{time.time():.2f}] {pipeline.name} pipeline completed")

        return await create_block(self)