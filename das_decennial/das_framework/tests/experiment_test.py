# experiment_test.py
#
# This program tests the ../experiment.py module with a contrived, simplified experiment
#
# Note that we don't need to change sys.path because there is a __init__.py in this directory
# and in the parent directory. For info, see:
# https://docs.pytest.org/en/latest/pythonpath.html


# import sys,os
from decimal import Decimal

# from driver import *
from das_framework.experiment import build_loops, increment_state, decode_loop, initial_state, substitute_config, run_experiment

import json
import driver
import pandas
import pytest

# from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from configparser import ConfigParser
import io

# For the test, we just read the config file from the string below.
# That way the test does not depend on the current directory, and there is no need for a config file.
# https://stackoverflow.com/questions/21766451/how-to-read-config-from-string-or-list

s_config = """
[DEFAULT]
root: .
name: test
loglevel: INFO

[experiment]
loop1: FOR demo.x = 1 to 10 step 1
loop2: FOR demo.y = 0.1 to 1.0 step .1
loop3: FOR demo.z = 0.1 to 10 mulstep 3.16
loop4: FOR demo.t IN 0.02,0.05,0.1
loop5: FOR demo.u STRIN a,b,c

[demo]
junk: 10

###
### what follows is largely from test.ini with the exception of the writer, which is a special writer that
### appends everything to a single file as a JSON object with the "x" and "y" values included.
###


[setup]
setup: demo_setup.setup

[reader]
reader: experiment_test.experiment_test_reader

[engine]
engine: demo_engine.engine

[writer]
writer: experiment_test.experiment_test_writer
output_fname: %(root)s/test_output.json

[validator]
validator: experiment_test.dummy_validator

[error_metrics]
error_metrics: experiment_test.dummy_error_metrics

[takedown]
takedown: demo_takedown.takedown
delete_output: False
"""

# Read the config file!
config = ConfigParser()
config.read_file( io.StringIO( s_config))

@pytest.mark.parametrize("s, answer", [
    ("FOR main.i=1 to 10 step 0.1" , ("main","i",Decimal('1'),Decimal('10'),Decimal('0.1'),"ADD")),
    ("FOR max.j=1 to 10"           , ("max", "j",Decimal('1'),Decimal('10'),Decimal('1'),"ADD")),
    ("FOR max.j=1 to 100 mulstep 3", ("max", "j",Decimal('1'), Decimal('100'), Decimal('3'), "MUL")),
    ("FOR max.j IN 0.1,0.2,0.3"    , ("max", "j",Decimal('0.1'),Decimal('0.3'),[Decimal('0.1'), Decimal('0.2'), Decimal('0.3')],"LIST")),
    ("FOR max.j STRIN A,b,C_ "     , ("max", "j",'A','b',['A','C_','b'],"STRLIST")),
    ("FOR max.j=1"                 , None),
])
def test_decode_loop(s, answer):
    assert decode_loop(s) == answer


def test_build_loop():
    ret = build_loops(config)
    assert ret == [('demo','x', Decimal('1.0'), Decimal('10.0'), Decimal('1.0'),"ADD"),
                   ('demo','y', Decimal('0.1'), Decimal('1.0'), Decimal('.1'),"ADD"),
                   ('demo','z', Decimal('0.1'), Decimal('10'), Decimal('3.16'), "MUL"),
                   ('demo', 't', Decimal('0.02'), Decimal('0.1'), [Decimal('0.02'), Decimal('0.05'), Decimal('0.1')], "LIST"),
                   ('demo', 'u', 'a', 'c', ['a', 'b', 'c'], "STRLIST")
                   ]

def test_initial_state():
    state = initial_state( build_loops( config ))
    assert state == (Decimal('1.0'), Decimal('0.1'), Decimal('0.1'), Decimal('0.02'), 'a')

def test_increment_state():
    loops = build_loops( config )
    state = initial_state( loops )
    state = increment_state(loops,state)
    assert state == (Decimal('2.0'), Decimal('0.1'), Decimal('0.1'), Decimal('0.02'), 'a')
    # loop 9 more times
    for i in range(9):
        state = increment_state(loops,state)
    assert state == (Decimal('1.0'), Decimal('0.2'), Decimal('0.1'), Decimal('0.02'), 'a')
    # Loop 89 more times
    for i in range(9,98):
        state = increment_state(loops,state)
    assert state == (Decimal('10.0'), Decimal('1.0'), Decimal('0.1'), Decimal('0.02'), 'a')
    # loop 400 more times
    for i in range(99,499):
        state = increment_state(loops, state)
    assert state == (Decimal('10.0'), Decimal('1.0'), Decimal('9.971220736'), Decimal('0.02'), 'a')
    # loop 1000 more times
    for i in range(499, 1499):
        state = increment_state(loops, state)
    assert state == (Decimal('10.0'), Decimal('1.0'), Decimal('9.971220736'), Decimal('0.1'), 'a')
    # loop 1000 more times
    for i in range(1499, 4499):
        state = increment_state(loops, state)
    assert state == (Decimal('10.0'), Decimal('1.0'), Decimal('9.971220736'), Decimal('0.1'), 'c')
    #Make sure it ends now
    assert increment_state(loops,state) == None


@pytest.mark.parametrize("s, answer",
                         [
                             ("loop3= FOR gurobi.topsum_reweighting STRIN True,False", ("False", "True")),
                             ("loop3= FOR gurobi.topsum_reweighting STRIN False,True", ("False", "True")),
                             ("loop3= FOR gurobi.topsum_reweighting STRIN b,a", ("a", "b")),
                             ("loop1= FOR a.x IN 1,2,3", (1, 2, 3)),

                         ])
def test_single_loop_increments(s, answer):
    loop = decode_loop(s)
    state = initial_state([loop])
    for i in range(len(answer)):
        assert state == (answer[i],)
        state = increment_state([loop], state)
    assert state is None


def test_substitute_config():
    loops = build_loops( config )
    state = initial_state( loops )
    newconfig = substitute_config( loops=loops, state=state, config=config)
    assert newconfig['demo']['junk'] == "10" # make sure that the old section is still there
    assert newconfig['demo']['x']    == "1"  # make sure we have a "1"
    assert newconfig['demo']['y']    == "0.1"  # make sure we have a "0.1"
    assert newconfig['demo']['z']    == "0.1"  # make sure we have a "0.1"
    assert newconfig['demo']['t']    == "0.02"  # make sure we have a "0.02"
    assert newconfig['demo']['u']    == "a"  # make sure we have a "a"

    # for giggles, lets try incrementing
    state = increment_state(loops, state)
    newconfig = substitute_config( loops=loops, state=state, config=config)
    assert newconfig['demo']['x']    == "2"  # make sure we have a "1"
    assert newconfig['demo']['y']    == "0.1"  # make sure we have a "0.1"
    assert newconfig['demo']['z']    == "0.1"  # make sure we have a "0.1"
    assert newconfig['demo']['t']    == "0.02"  # make sure we have a "0.02"
    assert newconfig['demo']['u']    == "a"  # make sure we have a "a"

# Run the full experiment
def test_run_experiment():
    loops = build_loops( config )
    state = initial_state( loops )
    # To test the callback, we collect all of the (x,y) pairs in the callback
    quintuplets = []
    def callback(config):
        quintuplets.append((config['demo']['x'],config['demo']['y'],config['demo']['z'],config['demo']['t'],config['demo']['u']))

    config[driver.EXPERIMENT][driver.EXPERIMENT_DIR] = '.' # use the current directory
    run_experiment(config=config, callback=callback)
    # I should have 4500 quintuplets
    assert len(quintuplets) == 4500
    # Check for some key values
    assert ('1', '0.1', '0.1', '0.02', 'a') in quintuplets
    assert ('5', '0.5', '3.1554496', '0.05', 'c') in quintuplets
    assert ('10', '1.0', '9.971220736', '0.1', 'b') in quintuplets


###
### this is for testing the experiment with an end-to-end test
###

class experiment_test_reader(driver.AbstractDASReader):
    def read(self):
        sales = {'ID': [1,2,3,4,5],
                 'Data': [10,20,30,40,50]}
        return pandas.DataFrame(sales)

class experiment_test_writer(driver.AbstractDASWriter):
    def write(self,df):
        # import copy
        fname = self.getconfig("output_fname")
        assert fname.endswith(".json")
        with open(fname,"a") as f:
            f.write( json.dumps( {"x":self.getfloat("x",section='demo'),
                                  "y":self.getfloat("y",section='demo'),
                                  'sum-ID':df['ID'].sum(),
                                  'sum-Data':df['Data'].sum()
                                  } ))
            f.write("\n")
        return fname            # where to find it


class dummy_validator(driver.AbstractDASValidator):
    def validate(self, original_data, written_data_reference, **kwargs):
        return True

class dummy_error_metrics(driver.AbstractDASErrorMetrics):
    def run(self, data):
        return None


if __name__=="__main__":
    test_run_experiment()
