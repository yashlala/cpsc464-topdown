# -*- coding: utf-8 -*-

import logging
from configparser import ConfigParser

class Scaffold:
    """
    Just a singleton implementation with functions to match das-framework
    """

    class __Scaffold:
        def __init__(self, **kwargs) -> None:
            self.val = {}

    instance = None

    def __init__(self, **kwargs) -> None:
        if not Scaffold.instance:
            Scaffold.instance = Scaffold.__Scaffold(**kwargs)  # else:  #     print(self.instance.val)

    def __getattr__(self, name: str):
        return getattr(self.instance, name)

    # def __setattr__(self, key, value):
    #     setattr(self.instance)

    def experimentSetup(self, config: ConfigParser) -> None:
        logging.info("Setting up Scaffolding")
        pass

    def experimentTakedown(self, config: ConfigParser) -> None:
        logging.info("Taking down Scaffolding")
        pass
