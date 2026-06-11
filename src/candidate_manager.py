import asyncio, time, os, parameter_states
import multiprocessing as mp
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import LocalExecutionBackend
from radical.asyncflow import logging
from concurrent.futures import ProcessPoolExecutor
from rhapsody.backends import DragonExecutionBackendV3

class CandidateManager:
    """
        The AsyncFlow rendition of CandidateManger
    """

    def __init__(self, candidates: list, session_dir_name: str, num_nodes=1):
        self.candidates = candidates
        self.num_nodes = num_nodes
        self.session_dir_name = session_dir_name
        self.logger = None
        self.batch_size = 50

    async def _get_workflow_manager(self):
        """
        Configures and obtains the asynchronous workflow manager.

        Returns:
            WorkflowEngine: The asynchronous workflow manager.
        """

        # Set Dragon as multiprocessing backend
        mp.set_start_method("dragon")

        backend = await DragonExecutionBackendV3()
        #backend = await LocalExecutionBackend(ProcessPoolExecutor())
        self.logger = logging.init_default_logger(log_level="INFO", output_file=os.path.join(self.session_dir_name, "logs.json"), structured_logging=True, show_details=True)

        self.logger.info(f"DragonExecutionBackendV3 created: {backend.batch.num_workers} workers")
        self.logger.info(f"{backend.batch.num_managers} managers")
        
        return await WorkflowEngine.create(backend=backend)

    async def execute_candidate_workflows(self):
        """
        Executes Pipeline coroutines concurrently.
        """
        flow = await self._get_workflow_manager()

        try:
            for candidate in self.candidates:
                candidate.set_workflow_engine(flow)
                candidate.set_logger(self.logger)
            
            for i in range(0, len(self.candidates), self.batch_size):
                results = await asyncio.gather(*[candidate.run_composite_workflow() for candidate in self.candidates[i:i + self.batch_size]])
                parameter_states.write_candidate_results(session_dir_name=self.session_dir_name, results_to_append=results)

        except Exception as e:
            self.logger.exception(e)
            print(type(e).__name__)
            print(type(e))

        finally:
            await flow.shutdown()

    def run(self):
        """
        Runs pipelines in workflow.
        """
        asyncio.run(self.execute_candidate_workflows())