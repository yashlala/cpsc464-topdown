import programs.workload.census_workloads as census_workloads

class WorkloadQueriesCreator():
    """
    """
    def __init__(self, schema, workload_names):

        self.schema = schema
        self.workload_query_names = census_workloads.getWorkload(workload_names)

        self.workload_queries_dict = self.schema.getQueries(self.workload_query_names)