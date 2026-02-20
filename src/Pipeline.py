import asyncio
from Task import Task

from radical.asyncflow import WorkflowEngine
from radical.asyncflow import ConcurrentExecutionBackend
from radical.asyncflow import logging
from concurrent.futures import ThreadPoolExecutor

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
'''
    @asyncflow.block
    async def pipeline_block(self):
        for i in range(len(self.tasks)):
            await self.tasks[i]
'''