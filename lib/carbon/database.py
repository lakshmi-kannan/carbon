#TODO(chrismd): unify this with the webapp's storage plugin system after the 1.1 merge

import errno
import os
from os.path import join, exists, dirname
from carbon.util import PluginRegistrar


# Ugly global state
whisper_available = False


class TimeSeriesDatabase(object):
  "Abstract base class for Carbon database backends"
  __metaclass__ = PluginRegistrar
  plugins = {}

  #def read(self, metric, start_time, end_time):
  #  after 1.1 merge

  def write(self, metric, datapoints):
    "Persist datapoints in the database for metric"
    raise NotImplemented()

  def exists(self, metric):
    "Return True if the given metric path exists, False otherwise."
    raise NotImplemented()

  def create(self, metric, **options):
    "Create an entry in the database for metric using options"
    raise NotImplemented()

  def get_metadata(self, metric, key):
    "Lookup metric metadata"
    raise NotImplemented()

  def set_metadata(self, metric, key, value):
    "Modify metric metadata"
    raise NotImplemented()


# "native" plugins below
try:
  import whisper
  whisper_available = True
except ImportError:
  pass
else:
  class WhisperDatabase(TimeSeriesDatabase):
    plugin_name = 'whisper'

    def __init__(self, settings):
      self.settings = settings
      self.data_dir = settings['LOCAL_DATA_DIR']
      self.sparse_create = settings['whisper'].get('SPARSE_CREATE', False)
      whisper.AUTOFLUSH = settings['whisper'].get('AUTOFLUSH', False)
      whisper.LOCK = settings['whisper'].get('LOCK_WRITES', False)

    def _get_filesystem_path(self, metric):
      return join(self.data_dir, *metric.split('.')) + '.wsp'

    def write(self, metric, datapoints):
      path = self._get_filesystem_path(metric)
      whisper.update_many(path, datapoints)

    def exists(self, metric):
      return exists(self._get_filesystem_path(metric))

    def create(self, metric, **options):
      path = self._get_filesystem_path(metric)
      directory = dirname(path)
      try:
        os.makedirs(directory, 0755)
      except OSError, e:
        if e.errno != errno.EEXIST:
          raise

      # convert argument naming convention
      options['archiveList'] = options.pop('retentions')
      options['xFilesFactor'] = options.pop('xfilesfactor')
      options['aggregationMethod'] = options.pop('aggregation-method')
      options['sparse'] = self.sparse_create

      whisper.create(path, **options)
      os.chmod(path, 0755)

    def get_metadata(self, metric, key):
      if key != 'aggregationMethod':
        raise ValueError("Whisper only supports the 'aggregationMethod' metadata key,"
                         " invalid key: " + key)
      path = self._get_filesystem_path(metric)
      return whisper.info(path)['aggregationMethod']

    def set_metadata(self, metric, key, value):
      if key != 'aggregationMethod':
        raise ValueError("Whisper only supports the 'aggregationMethod' metadata key,"
                         " invalid key: " + key)
      path = self._get_filesystem_path(metric)
      return whisper.setAggregationMethod(path, value)


try:
  import ceres
except ImportError:
  pass
else:
  class CeresDatabase(TimeSeriesDatabase):
    plugin_name = 'ceres'

    def __init__(self, settings):
      self.settings = settings
      self.data_dir = settings['LOCAL_DATA_DIR']
      self.tree = ceres.CeresTree(self.data_dir)
      ceres_settings = settings['ceres']
      behavior = ceres_settings.get('DEFAULT_SLICE_CACHING_BEHAVIOR')
      if behavior:
        ceres.setDefaultSliceCachingBehavior(behavior)
      if 'MAX_SLICE_GAP' in ceres_settings:
        ceres.MAX_SLICE_GAP = int(ceres_settings['MAX_SLICE_GAP'])

    def write(self, metric, datapoints):
      self.tree.store(metric, datapoints)

    def exists(self, metric):
      return self.tree.hasNode(metric)

    def create(self, metric, **options):
      # convert argument naming convention
      options['retentions'] = options.pop('retentions')
      options['timeStep'] = options['retentions'][0][0]
      options['xFilesFactor'] = options.pop('xfilesfactor')
      options['aggregationMethod'] = options.pop('aggregation-method')
      self.tree.createNode(metric, **options)

    def get_metadata(self, metric, key):
      return self.tree.getNode(metric).readMetadata()[key]

    def set_metadata(self, metric, key, value):
      node = self.tree.getNode(metric)
      metadata = node.readMetadata()
      metadata[key] = value
      node.writeMetadata(metadata)

try:
  from carbon.blueflood import Blueflood
except ImportError, e:
  pass
else:
  class BluefloodDatabase(TimeSeriesDatabase):
    plugin_name = 'blueflood'

    def __init__(self, settings):
      self.settings = settings
      bf_settings = settings['blueflood']

      self.bf_tenant = bf_settings.get('BLUEFLOOD_TENANT_ID')
      self.bf_password = bf_settings.get('BLUEFLOOD_API_KEY')

      bf_host = bf_settings.get('BLUEFLOOD_HOST')
      bf_in_port = bf_settings.get('BLUEFLOOD_INGESTION_PORT')
      bf_out_port = bf_settings.get('BLUEFLOOD_QUERY_PORT')

      self.bf_client = Blueflood(self.bf_tenant, self.bf_password, bf_host=bf_host,
                                 ingestion_port=bf_in_port, query_port=bf_out_port)

      # FIX: BF currently doesn't support discovery. So let's use whisper for now.
      # We create whisper files during metric "creation". But we don't really write
      # metrics into those files.
      if whisper:
        self._whisper = WhisperDatabase(settings)

    def write(self, metric, datapoints):
      self.bf_client.write(metric, datapoints)

    def exists(self, metric):
      self.bf_client.exists(metric)

    def create(self, metric, **options):
      self.bf_client.create(metric, **options)
      self._whisper.create(metric, **options)

    def get_metadata(self, metric, key):
      self.bf_client.get_metadata(metric, key)

    def set_metadata(self, metric, key, value):
      self.bf_client.set_metadata(metric, key, value)
