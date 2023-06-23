from das_framework.driver import AbstractDASTakedown

class takedown(AbstractDASTakedown):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def removeWrittenData(self, reference):
        return True
