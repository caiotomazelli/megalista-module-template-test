from enum import Enum
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from uploaders.appsflyer.appsflyer_s2s_uploader_async import AppsFlyerS2SUploaderDoFn

# TODO(caiotomazelli): Investigate Enum union.
# Reference: https://gist.github.com/plammens/ab1a2f236b5c6d748f193eb12eefa6dd
class DestinationType(Enum):
  APPSFLYER_S2S_EVENTS = range(1)

    def __eq__(self, other):
        if other is None:
            return False
        return self.name == other.name

# TODO(caiotomazelli): Discuss with team if we can decouple this class
class MegalistaStep(beam.PTransform):
    def __init__(self, oauth_credentials, dataflow_options=None):
        self._oauth_credentials = oauth_credentials
        self._dataflow_options = dataflow_options

    def expand(self, executions):
        pass

class AppsFlyerEventsStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data - AppsFlyer S2S events' >>
            BatchesFromExecutions(DestinationType.APPSFLYER_S2S_EVENTS, 1000, transactional=True)
            | 'Upload - AppsFlyer S2S events' >>
            beam.ParDo(AppsFlyerS2SUploaderDoFn(self._dataflow_options.appsflyer_dev_key))
            | 'Persist results - AppsFlyer S2S events' >> beam.ParDo(TransactionalEventsResultsWriter(self._dataflow_options.bq_ops_dataset))
        )
