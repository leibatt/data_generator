class Partition(object):
  def __init__(self,Schema,range_starts,range_widths):
    self.Schema = Schema
    self.range_starts = []
    self.range_widths = []
    self.nodecount = 0

    if range_starts is not None:
      self.range_starts = range_starts
      self.nodecount = len(range_starts)
    if range_widths is not None:
      self.range_widths = range_widths

  def get_chunks(self,node_id):
    '''
    Returns the list of chunk id's associated with the given node
    using this partitioning scheme
    '''
    return []

  def compute_overlap(self,other,node_id):
    '''
    Returns the list of chunk id's that are shared between this partitioning scheme and another
    partitioning scheme for the given node
    Arguments:
      other -- an instance of the Partition class to compare with
      node_id -- the node to compare for overlap between the partitioning schemes
    '''
    return []
