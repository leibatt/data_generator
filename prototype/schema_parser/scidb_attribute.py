class ScidbAttribute(object):
  '''
  Class used to store SciDB attribute information
  '''
  def __init__(self, name, dtype):
    self.name = name;
    self.dtype = dtype;

  def __repr__(self):
    s = "ScidbAttribute(%s,%s)" % (self.name,self.dtype)
    return s
