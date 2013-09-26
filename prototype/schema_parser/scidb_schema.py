from scidb_attribute import ScidbAttribute
from scidb_dimension import ScidbDimension
import math

class ScidbSchema:
  '''
  Class used to store a parsed SciDB schema.
  '''
  def __init__(self, name, attributes, dimensions):
    '''
    Arguments:
      name       -- name of the array
      attributes -- list of ScidbAttribute objects
      dimensions -- list of ScidbDimension objects
    '''
    self.name = ""
    self.attributes = []
    self.dimensions = []

    if name is not None:
      self.name = name
    if (attributes is not None) and (len(attributes) > 0):
      self.attributes = attributes
    if (dimensions is not None) and (len(dimensions) > 0):
      self.dimensions = dimensions

  def compute_dimid(self,chunk_id,cell_id):
    '''
    Given a chunk id and local cell id for the chunk,
    this method uses the schema definition to generate proper dimension id's for the
    cell.

    Arguments:
      chunk_id -- a single integer representing the location of the chunk.
      cell_id -- a single integer representing the location of the cell within the given chunk
    
    Returns:
      An integer array of length equal to the number of dimensions in the schema
    Note: I assume the first dimension is most significant, and last dimension least significant
    '''
    numdims = len(self.dimensions)
    base_index = [0] * numdims
    offset_index = [0] * numdims
    chunksize = self.compute_chunksize()
    totalchunks = self.compute_totalchunks()
    range_starts = []
    base_widths = []
    widths = []
    for dim in self.dimensions:
      chunks = math.ceil((1.0*dim.range_end - dim.range_start + 1) / dim.chunk_width)
      base_widths.append(chunks) # chunks along this dimension
      widths.append(dim.chunk_width)
      range_starts.append(dim.range_start)
    if numdims > 1:
      for i,base_width in enumerate(base_widths[:numdims-1]):
        totalchunks /= base_width
        while chunk_id >= totalchunks:
          chunk_id -= totalchunks
          base_index[i] += 1
        base_index[i] *= widths[i] # want the logical index, not chunk index

      for i,width in enumerate(widths[:numdims-1]):
        chunksize /= width
        while cell_id >= chunksize:
          cell_id -= chunksize
          offset_index[i] += 1
        offset_index[i] += base_index[i] + range_starts[i]# compute final index for this dim
    #final index for last dim
    base_index[numdims-1] = chunk_id * widths[numdims-1]
    offset_index[numdims-1] = cell_id + base_index[numdims-1] + range_starts[numdims - 1]
    return offset_index

  def compute_chunksize(self):
    '''
    Multiplies all chunk widths together to get the logical size of a single chunk in the schema
    '''
    chunksize = 1
    for dim in self.dimensions:
      chunksize *= dim.chunk_width
    return chunksize


  def compute_totalchunks(self):
    '''
    Computes the total number of chunks created in the schema by computing number of chunks along
    each dimension and multiplying them together.
    '''
    totalchunks = 1
    for dim in self.dimensions: # assume these are integers
      cw = dim.chunk_width
      rs = dim.range_start
      re = dim.range_end
      dimrange = re - rs + 1
      numchunks = math.ceil(dimrange / cw)
      if numchunks > 0:
        totalchunks *= numchunks
    return int(totalchunks)


  @staticmethod
  def parse_schema(schema_def):
    '''
    Method to parse a SciDB schema definition. Dimensions are assumed to be the default SciDB
    datatype of int64.
    Arguments:
      schema_def -- string representing a SciDB schema definiton
    Returns:
      tuple containing the name, list of attributes, and list of dimensions represented in schema_def

    Example:
      sdef = "array<attribute1:double>[dimension1=1:10,10,0,dimension2=1:10,10,0]"
      (name,attributes,dimensions) = Schema.parse(sdef)
    '''
    a1 = schema_def.index('<')
    a2 = schema_def.index('>')
    d1 = schema_def.index('[')
    d2 = schema_def.index(']')

    name = schema_def[:a1]

    attr_string = schema_def[a1+1:a2] # want to exclude '<'
    dim_string = schema_def[d1+1:d2] # want to exclude '['

    attributes = ScidbSchema.parse_attributes(attr_string)
    dimensions = ScidbSchema.parse_dimensions(dim_string)

    return (name,attributes,dimensions)

  @staticmethod
  def parse_attributes(attr_string):
    '''
    Turns a SciDB description of attributes into a list of ScidbAttribute objects
    Arguments:
      attr_string -- string representing attributes from a SciDB schema definition
                     *without* '<' and '>'
    Returns:
      A list of ScidbAttribute objects
    '''
    attribute_list = []

    #split on ','
    attrs = attr_string.split(',')
    #split each token on ':' for (name,dtype)
    for attr in attrs:
      pair = attr.split(':')
      if len(pair) == 2: # there should only be 2 items here
        name = pair[0]
        dtype = pair[1]
        attribute_list.append(ScidbAttribute(name,dtype))
      else:
        raise Exception("malformed attribute:\""+attr+"\"")
    return attribute_list   

  @staticmethod
  def parse_dimensions(dim_string):
    '''
    Turns a SciDB description of dimensions into a list of ScidbDimension objects
    Arguments:
      dim_string -- string representing dimensions from a SciDB schema definition
                     *without* '[' and ']'
    Returns:
      A list of ScidbDimension objects
    '''

    dimension_list = []
    #split on ','
    dims = dim_string.split(',')
    if (len(dims) % 3) != 0:
      raise Exception("Malformed dimension description. \
        Incorrect number of parameters per dimension: \""+dim_string+"\"")
    temp = None
    #traverse every 3 tokens (name+range,chunk width, chunk overlap)
    for i,dim in enumerate(dims):
      i = i % 3
      if i == 0: # name and dimension range
        temp = ScidbDimension(None,None,None,None,None,None)
        temp.dtype = "int64"
        #name+range
        nar = dim.split('=')
        if len(nar) == 2: # name and range only
          name = nar[0]
          if len(name) != 0:
            temp.name = name
          else: # name is empty
            raise Exception("Malformed dimension description. No dimension name:\""+dim+"\"")
          dim_range = nar[1].split(':')
          if (len(dim_range) == 2) and (len(dim_range[0]) > 0) and (len(dim_range[1]) > 0): 
            # range start and range end only
            temp.range_start = int(dim_range[0])
            temp.range_end = int(dim_range[1])
          else: # split for dimension range didn't work
            raise Exception("Malformed dimension description. Incorrect range:\""+dim+"\"")
        else: # split on '=' for name/range didn't work
          raise Exception("Malformed dimension description. \
            Typo with name/dimension range:\""+dim+"\"")
      elif i == 1: # chunk width
        if len(dim) > 0:
          temp.chunk_width = int(dim)
        else:
          raise Exception("Malformed dimension description. No chunk width.")
      else: # chunk overlap
        if len(dim) > 0:
          temp.chunk_overlap = int(dim)
          dimension_list.append(temp)
        else:
          raise Exception("Malformed dimension description. No chunk overlap.")       
    return dimension_list
