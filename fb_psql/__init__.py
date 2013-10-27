from multicorn import ForeignDataWrapper

class FqlForeignDataWrapper(ForeignDataWrapper):

  def __init__(self, options, columns):
    super(FqlForeignDataWrapper, self).__init__(options, columns)
    self.columns = columns

  def execute(self, quals, columns):
    line = {}
    for column_name in self.columns:
      line[column_name] = "example";
    yield line
