from partition import Partition

class RangePartition(Partition):
  def __init__(self,Schema,range_starts,range_widths):
    for i in range(len(range_starts)):
      for j in range(len(range_starts[i])):
        range_starts[i][j] = int(range_starts[i][j])
    for i in range(len(range_widths)):
      for j in range(len(range_widths[i])):
        range_widths[i][j] = int(range_widths[i][j])

    super(RangePartition,self).__init__(Schema,range_starts,range_widths)

  def get_chunks(self,node_id):
    '''
    Returns the list of chunk id's associated with the given node
    using this partitioning scheme
    '''
    node_chunks = []
    if self.nodecount <= node_id: # this layout usees fewer nodes
      return node_chunks
    range_start = self.range_starts[node_id]
    range_width = self.range_widths[node_id]
    range_end = [0] * len(range_start)
    for i in range(len(range_start)):
      range_end[i] = range_start[i] + range_width[i] - 1
    totalchunks = self.Schema.compute_totalchunks()
    #chunksize = self.Schema.compute_chunksize
    numdims = len(self.Schema.dimensions)
    for i in range(totalchunks):
      coord = self.Schema.compute_dimid(i,0)
      #print "chunk:",i,",coord:",coord,",range start:",range_start,",range end:",range_end
      add = True
      for dim in range(numdims):
        if (coord[dim] < range_start[dim]) or (coord[dim] > range_end[dim]):
          add = False
          break
      if add:
        #print "adding chunk",i
        node_chunks.append(i)
    return node_chunks

  def compute_overlap(self,other,node_id):
    '''
    Returns the list of chunk id's that are shared between this partitioning scheme and another
    partitioning scheme for the given node
    Arguments:
      other -- an instance of the Partition class to compare with
      node_id -- the node to compare for overlap between the partitioning schemes
    '''
    overlap = []
    non_overlap1 = [] # only need to keep track of this one
    non_overlap2 = [] # just tracking this to be thorough
    node1_chunks = self.get_chunks(node_id)
    node2_chunks = other.get_chunks(node_id)

    len1 = len(node1_chunks)
    len2 = len(node2_chunks)
    i1 = 0
    i2 = 0
    while (i1 < len1) and (i2 < len2):
      val1 = node1_chunks[i1]
      val2 = node2_chunks[i2]
      if val1 == val2:
        overlap.append(val1)
        i1 += 1
        i2 += 1
      elif val1 < val2:
        i1 += 1
        non_overlap1.append(val1)
      else:
        i2 += 1
        non_overlap2.append(val2)
    while i1 < len1:
      val1 = node1_chunks[i1]
      non_overlap1.append(val1)
      i1 += 1
    while i2 < len2:
      val2 = node2_chunks[i2]
      non_overlap2.append(val2)
      i2 += 1
    return (overlap,non_overlap1,non_overlap2)

  @staticmethod
  def parse(partition_string):
    #split on |
    node_strings = partition_string.split('|')
    numnodes = len(node_strings)
    range_starts = []
    range_widths = []
    #for each item (one for each node):
    for node_string in node_strings:
      # split on :, first is node name, second is starts and ranges
      nar = node_string.split(':')
      node_id = int(nar[0])
      range_string = nar[1]
      # split second on ',' and count the items
      range_items = range_string.split(',')
      # if the number of items is not even, this is wrong
      lri = len(range_items)
      if (lri % 2) != 0:
        raise Exception("malformed ranges for node "+nar[0]+": "+range_string)
      # store the first half as range_starts
      half = lri/2 # this should just work bc it's even
      range_starts.append(range_items[:half])
      # store the second half as range_widths
      range_widths.append(range_items[half:])
    return (range_starts,range_widths)
