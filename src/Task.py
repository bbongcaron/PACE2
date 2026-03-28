import asyncio, os

class Task:
    """
    A Task is an abstraction of a computational unit. In this case, a Task
    consists of its executable along with its required software environment,
    files to be staged as input and output.
    
    """

    def __init__(self, name: str, commands: list[dict], candidateFiles: list[str], cwd: str, ranks: int, threads: int):
        self.name = name
        self.cwd = cwd
        self.commands = []
        self.candidateFiles = candidateFiles
        self.ranks = ranks
        self.threads = threads

        for commandDict in commands:
            executable = commandDict['executable']
            args = commandDict['args']
            self.commands.append(executable + ' ' + args)
        
        os.makedirs(cwd, exist_ok=True)

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