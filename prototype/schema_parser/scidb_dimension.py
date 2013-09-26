class ScidbDimension(object):
  '''
  Class used to store SciDB dimension information
  '''
  def __init__(self, name, dtype,range_start,range_end,chunk_width,chunk_overlap):
    self.name = name
    self.dtype = dtype
    self.range_start = -1
    self.range_end = -1
    self.chunk_width = -1
    self.chunk_overlap = 0

    if range_start is not None:
      self.range_start = int(range_start)
    if range_end is not None:
      self.range_end = int(range_end)
    if chunk_width is not None:
      self.chunk_width = int(chunk_width)
    if chunk_overlap is not None:
      self.chunk_overlap = int(chunk_overlap)

  def __repr__(self):
    s = "ScidbDimension(%s,%s,%d,%d,%d,%d)" % \
        (self.name,self.dtype,self.range_start,self.range_end,self.chunk_width,self.chunk_overlap)
    return s
