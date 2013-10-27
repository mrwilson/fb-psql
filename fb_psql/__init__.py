from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log
from requests import get
import logging

class FqlForeignDataWrapper(ForeignDataWrapper):

  def __init__(self, options, columns):
    super(FqlForeignDataWrapper, self).__init__(options, columns)
    self.validate(options, columns)
    self.columns = columns
    self.key = options["key"]

  def validate(self, options, columns):
    if "key" not in options:
      log(message = "No api key given", level = logging.ERROR)

  def execute(self, quals, columns):
    params = { 'q': 'select uid2 from friend where uid1=me()', 'access_token': self.key }

    r = get('https://graph.facebook.com/fql', params=params)

    response = r.json()

    for entry in response["data"]:
      yield entry
