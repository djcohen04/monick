import traceback

class EnvClient(object):
    def __init__(self, filename='.env'):
        ''' Envirnoment/Secret Variable Loader
        '''
        self._filename = filename
        self._load()

    def _load(self):
        ''' Load Env Variables into Object Instance Attributes
        '''
        with open(self._filename, 'r') as envfile:
            for line in envfile:
                try:
                    key, value = line.strip().split('=')
                    setattr(self, key, value)
                except:
                    print('WARN: Error Parsing .env File Row, Skipping...')
