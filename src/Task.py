import asyncio
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import ConcurrentExecutionBackend
from radical.asyncflow import logging
from concurrent.futures import ThreadPoolExecutor

class Task:
    """
    A Task is an abstraction of a computational unit. In this case, a Task
    consists of its executable along with its required software environment,
    files to be staged as input and output.
    """
    def __init__(self, name='', executable='', args=''):
        self.name = name
        self.executable = executable
        self.args = args
        pass

'''
    @flow.executable_task
    async def pre_exec_task(self):
        return self.pre_exec_executable + pre_exec_args
    
    @flow.executable_task
    async def exec_task(self, pre_exec_task_results):
        return self.exec + self.args

    async def taskflow(self):
        pre_exec_task_results = self.pre_exec_task()
        exec_result = self.exec_task(pre_exec_task_results)
        return await exec_result
'''