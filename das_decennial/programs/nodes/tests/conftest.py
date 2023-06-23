from programs.engine import primitives

def pytest_configure(config):
    primitives._called_from_test = True
