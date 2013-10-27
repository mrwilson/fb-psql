from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log

import logging

class FqlForeignDataWrapper(ForeignDataWrapper):

  def __init__(self, options, columns):
    super(FqlForeignDataWrapper, self).__init__(options, columns)
    self.validate(options, columns)
    self.columns = columns

  def validate(self, options, columns):
    if "key" not in options:
      log(message = "No api key given", level = logging.ERROR)

  def execute(self, quals, columns):
    line = {}
    for column_name in self.columns:
      line[column_name] = "example";
    yield line
