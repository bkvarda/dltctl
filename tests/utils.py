import decorator
from click.testing import CliRunner

from databricks_cli.configure.provider import DatabricksConfig, DEFAULT_SECTION, \
    update_and_persist_config

TEST_PROFILE = 'test-profile'


def provide_conf(test):
    def wrapper(test, *args, **kwargs):
        config = DatabricksConfig.from_token('test-host', 'test-token')
        update_and_persist_config(DEFAULT_SECTION, config)
        config = DatabricksConfig.from_token('test-host-2', 'test-token-2')
        update_and_persist_config(TEST_PROFILE, config)
        return test(*args, **kwargs)
    return decorator.decorator(wrapper, test)