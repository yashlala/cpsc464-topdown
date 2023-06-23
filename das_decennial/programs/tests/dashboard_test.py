import configparser

import programs.dashboard as dashboard
from programs.dashboard import *

def test_make_int():
    assert make_int("1") == 1
    assert make_int("1k") == 1024
    assert make_int("2m") == 2 * 1024 * 1024
    assert make_int("3g") == 3 * 1024 * 1024 * 1024


def test_Singleton():
    a = SQS_Client()
    b = SQS_Client()
    assert a is b

def test_send_config():
    os.environ['DAS_RUN_UUID'] = dashboard.fake_das_run_uuid()
    config = configparser.ConfigParser()
    config['a'] = {}
    config['a']['first'] = '1'
    config['b'] = {}
    config['b']['second'] = '2'
    dashboard.config_upload(config)
