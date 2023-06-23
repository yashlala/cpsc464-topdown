from programs.engine import primitives


def pytest_configure(config) -> None:
    primitives._called_from_test = True


#def pytest_addoption(parser):
#    parser.addoption("--prod", action="store", default="False")


#def pytest_generate_tests(metafunc):
#    # This is called for every test. Only get/set command line arguments
#    # if the argument is specified in the list of test "fixturenames".
#    option_value = metafunc.config.option.prod
#    if 'prod' in metafunc.fixturenames and option_value is not None:
#        metafunc.parametrize("prod", [option_value])
