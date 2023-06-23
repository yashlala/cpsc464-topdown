import pytest
import os
from configparser import ConfigParser

import programs.optimization.optimizer

@pytest.fixture
def config():
    conf_file = os.path.join(os.path.dirname(__file__), "test_config.ini")
    conf = ConfigParser()
    conf.read(conf_file)
    return conf

class AbstractOptimizerWithGurobiEnvironmentForTest(programs.optimization.optimizer.AbstractOptimizer):

    def run(self):
        pass

class OptimizerWithGurobiEnvironmentForTest(programs.optimization.optimizer.Optimizer):

    def run(self):
        pass

@pytest.mark.skip(reason="Skipping momentarily")
def test_getGurobiEnvironment(config, dd_das_stub):
    o =AbstractOptimizerWithGurobiEnvironmentForTest(config=config, stat_node=None, das=dd_das_stub)
    env = o.getGurobiEnvironment()
    #passes if no exception is thrown


# def test_newModel_standalone(config):
#     o = AbstractOptimizerWithGurobiEnvironmentForTest(config=config, stat_node=None)
#     env =  o.getGurobiEnvironment()
#     model = programs.optimization.optimizer.newModel(name="hello", env=env, config=config)
#     #passes if no exception is thrown

@pytest.mark.skip(reason="Skipping momentarily")
def test_newModel(config, dd_das_stub):
    o = AbstractOptimizerWithGurobiEnvironmentForTest(config=config, stat_node=None, das=dd_das_stub)
    env =  o.getGurobiEnvironment()
    o1 = OptimizerWithGurobiEnvironmentForTest(config=config, grb_env=env, stat_node=None, das=dd_das_stub)
    model = o1.newModel("hello")
    #passes if no exception is thrown
