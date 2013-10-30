from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log
from requests import get
import logging

class FqlForeignDataWrapper(ForeignDataWrapper):

  def __init__(self, options, columns):
    super(FqlForeignDataWrapper, self).__init__(options, columns)

    self.validate(options, columns)

    self.columns = columns
    self.key     = options['key']
    self.table   = options['table_name']

  def validate(self, options, columns):
    if 'key' not in options:
      log(message = 'No api key given', level = logging.ERROR)

    if 'table_name' not in options:
      log(message = 'No corresponding FQL table', level = logging.ERROR)

  def handle_error(self, response):
    error = response['error']['message']
    log(message = error, level = logging.ERROR)

  def get_query_string(self, columns, quals):
    cols = ','.join(columns)

    query = 'select %s from %s' % (cols, self.table)

    if 'uid' in columns and 'uid' not in [q.field_name for q in quals]:
        query = query + ' where uid=me()'

    return query

  def execute(self, quals, columns):

    params   = { 'q': self.get_query_string(columns,quals), 'access_token': self.key }
    request  = get('https://graph.facebook.com/fql', params=params)

    response = request.json()

    if 'error' in response:
      self.handle_error(response)
    else:
      for entry in response['data']:
        yield entry
