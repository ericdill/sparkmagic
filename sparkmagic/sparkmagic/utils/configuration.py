# Distributed under the terms of the Modified BSD License.
import copy
import sys
import base64
from hdijupyterutils.constants import EVENTS_HANDLER_CLASS_NAME, LOGGING_CONFIG_CLASS_NAME
from hdijupyterutils.utils import join_paths
from hdijupyterutils.configuration import override as _override
from hdijupyterutils.configuration import override_all as _override_all
from hdijupyterutils.configuration import with_override

from .constants import HOME_PATH, CONFIG_FILE, MAGICS_LOGGER_NAME, LIVY_KIND_PARAM, \
    LANG_SCALA, LANG_PYTHON, LANG_PYTHON3, LANG_R, \
    SESSION_KIND_SPARKR, SESSION_KIND_SPARK, SESSION_KIND_PYSPARK, SESSION_KIND_PYSPARK3, CONFIGURABLE_RETRY
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
import sparkmagic.utils.constants as constants


# This whole module should be rewritten with traitlets

# `d` here is a module-level global of configurations that takes runtime precedence over what the
# user has given with the config.json file which takes precedence over the hard-coded values
# in this module. Precedence: `d` > user configs > defaults in this file
d = {}

# The full path to the sparkmagic configuration file. It gets pulled from the environment
# or has some default values
path = join_paths(HOME_PATH, CONFIG_FILE)



def override(config, value):
    """Code in other modules can call in to the configuration via `override` to set a config value
    that takes highest precedence. Preference order:
    1. Values set via `override`
    2. Values set in the sparkmagic configuration file defined by `path`
    3. Values set in the default functions inside of this configuration.py module

    Parameters
    ----------
    config: str, configuration key
    value: object, value for `config` key
    """
    _override(d, path, config, value)


def override_all(obj):
    # This function does not appear to be used anywhere in this code base
    _override_all(d, obj)


# _with_override is a decorator that uses the name of the function that it is wrapping
# to pull out the default configuration parameter that is set in the sparkmagic config.json
# file. For example, see the function below called "session_configs"
_with_override = with_override(d, path)


# Helpers

# language_map = {LANG_SCALA: SESSION_KIND_SPARK,
#                 LANG_PYTHON: SESSION_KIND_PYSPARK,
#                 LANG_PYTHON3: SESSION_KIND_PYSPARK3,
#                 LANG_R: SESSION_KIND_SPARKR}
def get_livy_kind(language):
    # You could turn this whole function into just a dict lookup instead of if/elif/else
    # session_kind = language_map.get(language)
    # if session_kind is None:
    #     raise BadUserConfigurationException("Cannot get session kind for {}.".format(language))
    # return session_kind

    if language == LANG_SCALA:
        return SESSION_KIND_SPARK
    elif language == LANG_PYTHON:
        return SESSION_KIND_PYSPARK
    elif language == LANG_PYTHON3:
        return SESSION_KIND_PYSPARK3
    elif language == LANG_R:
        return SESSION_KIND_SPARKR
    else:
        raise BadUserConfigurationException("Cannot get session kind for {}.".format(language))

def get_auth_value(username, password):
    if username == '' and password == '':
        return constants.NO_AUTH

    return constants.AUTH_BASIC


# Configs


def get_session_properties(language):
    properties = copy.deepcopy(session_configs())
    properties[LIVY_KIND_PARAM] = get_livy_kind(language)
    return properties


# The _with_override decorator uses the name of the function, in this case "session_configs"
# as the key to pull out the configuration value set in the sparkmagic config.json file.
# Executing this function is going to read the config.json file, look for the "session_configs"
# key in that configuration file and return the value of that key. If the key, in this case
# "session_configs" is not specified in the sparkmagic configuration file then the return value
# as defined below will be used as the default.
@_with_override
def session_configs():
    return {}


@_with_override
def kernel_python_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998', u'auth': constants.NO_AUTH}


def base64_kernel_python_credentials():
    return _credentials_override(kernel_python_credentials)


# No one's gonna use pyspark and pyspark3 notebook on different endpoints. Reuse the old config.
@_with_override
def kernel_python3_credentials():
    return kernel_python_credentials()


def base64_kernel_python3_credentials():
    return base64_kernel_python_credentials()


@_with_override
def kernel_scala_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998', u'auth': constants.NO_AUTH}


def base64_kernel_scala_credentials():
    return _credentials_override(kernel_scala_credentials)

@_with_override
def kernel_r_credentials():
    return {u'username': u'', u'base64_password': u'', u'url': u'http://localhost:8998', u'auth': constants.NO_AUTH}


def base64_kernel_r_credentials():
    return _credentials_override(kernel_r_credentials)


@_with_override
def logging_config():
    return {
        u"version": 1,
        u"formatters": {
            u"magicsFormatter": {
                u"format": u"%(asctime)s\t%(levelname)s\t%(message)s",
                u"datefmt": u""
            }
        },
        u"handlers": {
            u"magicsHandler": {
                u"class": LOGGING_CONFIG_CLASS_NAME,
                u"formatter": u"magicsFormatter",
                u"home_path": HOME_PATH
            }
        },
        u"loggers": {
            MAGICS_LOGGER_NAME: {
                u"handlers": [u"magicsHandler"],
                u"level": u"DEBUG",
                u"propagate": 0
            }
        }
    }


@_with_override
def events_handler_class():
    return EVENTS_HANDLER_CLASS_NAME


@_with_override
def wait_for_idle_timeout_seconds():
    return 15


@_with_override
def livy_session_startup_timeout_seconds():
    return 60


@_with_override
def fatal_error_suggestion():
    return u"""The code failed because of a fatal error:
\t{}.

Some things to try:
a) Make sure Spark has enough available resources for Jupyter to create a Spark context.
b) Contact your Jupyter administrator to make sure the Spark magics library is configured correctly.
c) Restart the kernel."""


@_with_override
def resource_limit_mitigation_suggestion():
    return ""


@_with_override
def ignore_ssl_errors():
    return False


@_with_override
def coerce_dataframe():
    return True


@_with_override
def use_auto_viz():
    return True


@_with_override
def default_maxrows():
    return 2500


@_with_override
def default_samplemethod():
    return "take"


@_with_override
def default_samplefraction():
    return 0.1


@_with_override
def pyspark_dataframe_encoding():
    return u'utf-8'


@_with_override
def heartbeat_refresh_seconds():
    return 30


@_with_override
def heartbeat_retry_seconds():
    return 10


@_with_override
def livy_server_heartbeat_timeout_seconds():
    return 0


@_with_override
def server_extension_default_kernel_name():
    return "pysparkkernel"


@_with_override
def custom_headers():
    return {}


@_with_override
def retry_policy():
    return CONFIGURABLE_RETRY


@_with_override
def retry_seconds_to_sleep_list():
    return [0.2, 0.5, 1, 3, 5]


@_with_override
def configurable_retry_policy_max_retries():
    # Sum of default values is ~10 seconds.
    # Plus 15 seconds more wanted, that's 3 more 5 second retries.
    return 8


def _credentials_override(f):
    """Provides special handling for credentials. It still calls _override().
    If 'base64_password' in config is set, it will base64 decode it and returned in return value's 'password' field.
    If 'base64_password' is not set, it will fallback to 'password' in config.
    """
    credentials = f()
    base64_decoded_credentials = {k: credentials.get(k) for k in ('username', 'password', 'url', 'auth')}
    base64_password = credentials.get('base64_password')
    if base64_password is not None:
        try:
            base64_decoded_credentials['password'] = base64.b64decode(base64_password).decode()
        except Exception:
            exception_type, exception, traceback = sys.exc_info()
            msg = "base64_password for %s contains invalid base64 string: %s %s" % (f.__name__, exception_type, exception)
            raise BadUserConfigurationException(msg)
    if base64_decoded_credentials['auth'] is None:
        base64_decoded_credentials['auth'] = get_auth_value(base64_decoded_credentials['username'], base64_decoded_credentials['password'])
    return base64_decoded_credentials
