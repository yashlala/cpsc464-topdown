from das_framework.driver import AbstractDASErrorMetrics

class ErrorMetricsStub(AbstractDASErrorMetrics):
    def __init__(self, **kwargs) -> None :
        super().__init__(**kwargs)

    def run(self, engine_tuple) -> None:
        """
        As a placeholder/stub, this does nothing.
        """
        return None
