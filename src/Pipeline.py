import asyncio, time
from Task import Task

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import logging

class Pipeline:
    """
    A pipeline represents a collection of objects that have a linear temporal
    execution order.  In this case, a pipeline consists of multiple 'Task'
    objects. Each ```Task_i``` can execute only after all stages up to
    ```Task_(i-1)``` have completed execution.
    """

    def __init__(self, name=''):
        self.name = name
        self.tasks = []

    def add_task(self, task: Task):
        self.tasks.append(task)
    
    async def run(self, flow: WorkflowEngine, logger: logging):
        @flow.block
        async def create_block(pipeline: Pipeline):
            logger.info(f"[{time.time():.2f}] {pipeline.name} pipeline started")

            for task in pipeline.tasks: await task.run(flow)

            logger.info(f"[{time.time():.2f}] {pipeline.name} pipeline completed")

        return await create_block(self)