import os

offset = 0

def file_setup(Schema,aorb='A',path_prefix=''):
  dim_handles = []
  attr_handles = []
  chunkmap_handle = None
  path = os.path.join(path_prefix,Schema.name+aorb)
  try: # make the directory for the data
    os.makedirs(path)
  except OSError:
    if not os.path.isdir(path):
      raise
  for i,dim in enumerate(Schema.dimensions):
    f = os.path.join(path,'dim'+str(i)+'.chunk')
    dim_handles.append(open(f,'w'))
  for i,attr in enumerate(Schema.attributes):
    f = os.path.join(path,'attr'+str(i)+'.chunk')
    attr_handles.append(open(f,'w'))
  f = os.path.join(path,'chunkmap')
  chunkmap_handle = open(f,'w')
  return (dim_handles,attr_handles,chunkmap_handle)

def close_handle(handle):
  '''
  Closes a single file handle
  '''
  handle.close()

def close_handles(handles):
  '''
  Closes a list of file handles
  '''
  for i in range(len(handles)):
    handles[i].close()

def write_dimvals(chunk,dim_handles,chunkmap_handle):
  global offset
  chunk_id = chunk.chunk_id
  coords = chunk.coordinates
  #coords contains 1 list for each nonempty cell of length # dims
  for coord in coords:
    for i,handle in enumerate(dim_handles):
      handle.write(str(coord[i]))
      handle.write('\n')
  chunkmap_handle.write(','.join([str(chunk_id),str(offset),str(chunk.cellcount)]))
  chunkmap_handle.write('\n')
  # do not close handles!!!
  offset += chunk.cellcount

def reset_offset():
  global offset
  offset = 0

def write_attrvals(chunk,attr_handles):
  chunk_id = chunk.chunk_id
  attrvals = chunk.attributes
  # attrvals contains 1 list for each attribute of length cellcount
  for i,handle in enumerate(attr_handles):
    for attr in attrvals[i]:
      handle.write(str(attr))
      handle.write('\n')
  # do not write to chunkmap!
  # do not close handles!

def write_chunk(chunk,dim_handles,attr_handles,chunkmap_handle):
  print "writing chunk"
  write_dimvals(chunk,dim_handles,chunkmap_handle)
  write_attrvals(chunk,attr_handles)
