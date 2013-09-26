class Chunk(object):
  '''
  Class used to store and manage generated chunk data/metadata
  '''
  def __init__(self,Schema,chunk_id,cellcount,coordinates,attributes):
    self.schema = Schema
    self.chunk_id = chunk_id
    self.coordinates = coordinates
    self.attributes = attributes
    self.cellcount = cellcount
