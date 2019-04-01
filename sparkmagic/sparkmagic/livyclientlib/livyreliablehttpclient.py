# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.


from .linearretrypolicy import LinearRetryPolicy
from .configurableretrypolicy import ConfigurableRetryPolicy
from .reliablehttpclient import ReliableHttpClient
from sparkmagic.utils.constants import LINEAR_RETRY, CONFIGURABLE_RETRY
import sparkmagic.utils.configuration as conf
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException


class LivyReliableHttpClient(object):
    """A Livy-specific Http client which wraps the normal ReliableHttpClient. Propagates
    HttpClientExceptions up.

    See the [Livy REST API](https://livy.incubator.apache.org/docs/latest/rest-api.html)
    docs for full info

    Endpoints that aren't implemented:
    * GET /sessions/{sessionId}/state
        Returns the state of session
    * POST /sessions/{sessionId}/statements/{statementId}/cancel
        Cancel the specified statement in this session.
    * POST /sessions/{sessionId}/completion
        Runs a statement in a session.
    * All of the /batches endpoints
    """
    def __init__(self, http_client, endpoint):
        self.endpoint = endpoint
        self._http_client = http_client

    @staticmethod
    def from_endpoint(endpoint):
        headers = {"Content-Type": "application/json" }
        headers.update(conf.custom_headers())
        retry_policy = LivyReliableHttpClient._get_retry_policy()
        return LivyReliableHttpClient(ReliableHttpClient(endpoint, headers, retry_policy), endpoint)

    def post_statement(self, session_id, data):
        # Runs a statement in a session.
        return self._http_client.post(self._statements_url(session_id), [201], data).json()

    def get_statement(self, session_id, statement_id):
        # Returns a specified statement in a session.
        return self._http_client.get(self._statement_url(session_id, statement_id), [200]).json()

    def get_sessions(self):
        # Returns all the active interactive sessions.
        return self._http_client.get("/sessions", [200]).json()

    def post_session(self, properties):
        # Creates a new interactive Scala, Python, or R shell in the cluster.
        # Valid properties are listed in the Livy REST API docs
        return self._http_client.post("/sessions", [201], properties).json()

    def get_session(self, session_id):
        # Returns the session information specified by `session_id`
        return self._http_client.get(self._session_url(session_id), [200]).json()

    def delete_session(self, session_id):
        # Kills the Session.
        self._http_client.delete(self._session_url(session_id), [200, 404])

    def get_all_session_logs(self, session_id):
        # Endpoint sort of implemended. /session/{session_id}/log endpoint takes
        # the following parameters:
        # `from`: The line offset
        # `size`: the total number of lines to return
        return self._http_client.get(self._session_url(session_id) + "/log?from=0", [200]).json()

    def get_headers(self):
        return self._http_client.get_headers()

    @staticmethod
    def _session_url(session_id):
        return "/sessions/{}".format(session_id)

    @staticmethod
    def _statements_url(session_id):
        # Returns all the statements in a session.
        return "/sessions/{}/statements".format(session_id)

    @staticmethod
    def _statement_url(session_id, statement_id):
        return "/sessions/{}/statements/{}".format(session_id, statement_id)

    @staticmethod
    def _get_retry_policy():
        policy = conf.retry_policy()
        # Consider moving the remaining code in this function into the retry_policy
        # in the conf module.

        if policy == LINEAR_RETRY:
            return LinearRetryPolicy(seconds_to_sleep=5, max_retries=5)
        elif policy == CONFIGURABLE_RETRY:
            return ConfigurableRetryPolicy(retry_seconds_to_sleep_list=conf.retry_seconds_to_sleep_list(), max_retries=conf.configurable_retry_policy_max_retries())
        else:
            raise BadUserConfigurationException(u"Retry policy '{}' not supported".format(policy))
