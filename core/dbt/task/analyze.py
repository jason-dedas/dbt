import threading
from .runnable import GraphRunnableTask
from .base import BaseRunner

from .printer import (
    print_start_line,
    print_analysis_result_line,
    print_run_end_messages,
    get_counts,
)

from dbt import tracking
from dbt import utils
from dbt.contracts.results import NodeStatus, RunResult, RunStatus
from dbt.exceptions import (
    CompilationException,
    InternalException,
    RuntimeException,
)
from dbt.graph import ResourceTypeSelector, SelectionSpec, parse_difference
from dbt.logger import print_timestamped_line
from dbt.node_types import NodeType


def track_model_run(index, num_nodes, run_model_result):
    if tracking.active_user is None:
        raise InternalException('cannot track model run with no active user')
    invocation_id = tracking.active_user.invocation_id
    tracking.track_model_run({
        "invocation_id": invocation_id,
        "index": index,
        "total": num_nodes,
        "execution_time": run_model_result.execution_time,
        "run_status": str(run_model_result.status).upper(),
        "run_skipped": run_model_result.status == NodeStatus.Skipped,
        "run_error": run_model_result.status == NodeStatus.Error,
        "model_materialization": run_model_result.node.get_materialization(),
        "model_id": utils.get_hash(run_model_result.node),
        "hashed_contents": utils.get_hashed_contents(
            run_model_result.node
        ),
        "timing": [t.to_dict(omit_none=True) for t in run_model_result.timing],
    })

class AnalyzeRunner(BaseRunner):
    def get_node_representation(self):
        display_quote_policy = {
            'database': False, 'schema': False, 'identifier': False
        }
        relation = self.adapter.Relation.create_from(
            self.config, self.node, quote_policy=display_quote_policy
        )
        # exclude the database from output if it's the default
        if self.node.database == self.config.credentials.database:
            relation = relation.include(database=False)
        return str(relation)

    def describe_node(self):
        return "analysis {}".format(self.get_node_representation())

    def print_start_line(self):
        description = self.describe_node()
        print_start_line(description, self.node_index, self.num_nodes)

    def print_result_line(self, result):
        description = self.describe_node()
        print_analysis_result_line(result, description, self.node_index,
                                self.num_nodes)

    def before_execute(self):
        self.print_start_line()

    def after_execute(self, result):
        track_model_run(self.node_index, self.num_nodes, result)
        self.print_result_line(result)

    def execute(self, compiled_node, manifest):
        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=0,
            message=None,
            adapter_response={}
        )

    def compile(self, manifest):
        compiler = self.adapter.get_compiler()
        compiled_node = compiler.compile_node(self.node, manifest, {})

        if (compiled_node.resource_type == "analysis"):
            #with open(compiled_node.original_file_path.replace(".sql", ".csv"), "w") as f:
            output = self.adapter.execute(compiled_node.compiled_sql, fetch=True)[1]
            output.to_csv(compiled_node.original_file_path.replace(".sql", ".csv"))

        return compiled_node


class AnalyzeTask(GraphRunnableTask):
    def raise_on_first_error(self):
        return True

    def get_selection_spec(self) -> SelectionSpec:
        if self.args.selector_name:
            spec = self.config.get_selector(self.args.selector_name)
        else:
            spec = parse_difference(self.args.models, self.args.exclude)
        return spec

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=NodeType.executable(),
        )

    def get_runner_type(self):
        return AnalyzeRunner

    def task_end_messages(self, results):
        nodes = [r.node for r in results]
        stat_line = get_counts(nodes)

        execution_times = [t.execution_time for t in results]
        full_execution_time = round(sum(execution_times),2)

        print_timestamped_line("Finished compiling/analyzing {stat_line} in {execution_time}s."
            .format(stat_line=stat_line, execution_time = full_execution_time))
