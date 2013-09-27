import os
import numpy as np

offset = 0
offsetB = 0

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
  numdims = len(Schema.dimensions)
  for i,dim in enumerate(Schema.dimensions):
    f = os.path.join(path,str(i)+'.chunk')
    dim_handles.append(open(f,'w'))
  for i,attr in enumerate(Schema.attributes):
    f = os.path.join(path,str(i+numdims)+'.chunk')
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

def write_dimvals(chunk,dim_handles,chunkmap_handle,aorb='A'):
  global offset
  global offsetB
  chunk_id = chunk.chunk_id
  coords = chunk.coordinates
  #coords contains 1 list for each nonempty cell of length # dims
  for coord in coords:
    for i,handle in enumerate(dim_handles):
      handle.write(str(coord[i]))
      handle.write('\n')
  o = offset
  if aorb == 'B':
    o = offsetB
  chunkmap_handle.write(','.join([str(chunk_id),str(o),str(chunk.cellcount)]))
  chunkmap_handle.write('\n')
  # do not close handles!!!
  if aorb == 'A':
    offset += chunk.cellcount
  else:
    offsetB += chunk.cellcount

def reset_offset(aorb='A'):
  global offset
  if aorb == 'B':
    offsetB = 0
  else:
    offset = 0

def write_dimvals_binary(chunk,dim_handles,chunkmap_handle):
  global offset
  chunk_id = chunk.chunk_id
  coords = np.array(chunk.coordinates)
  coords = coords.T # take the transpose
  #write to dimension files
  for i,handle in enumerate(dim_handles): #each row of coords is all values for dim i
    handle.write(coords[i].tostring())
  # do not close handles!!!

def write_attrvals_binary(chunk,attr_handles):
  chunk_id = chunk.chunk_id
  attrvals = np.array(chunk.attributes)
  # attrvals contains 1 list for each attribute of length cellcount
  for i,handle in enumerate(attr_handles):
    handle.write(attrvals[i].tostring())
  # do not close handles!

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

def write_chunk(chunk,dim_handles,attr_handles,chunkmap_handle,aorb='A'):
  #print "writing chunk"
  write_dimvals(chunk,dim_handles,chunkmap_handle,aorb=aorb)
  write_attrvals(chunk,attr_handles)

def write_chunk_binary(chunk,dim_handles,attr_handles,chunkmap_handle):
  #print "writing chunk"
  write_dimvals_binary(chunk,dim_handles,chunkmap_handle)
  write_attrvals_binary(chunk,attr_handles)
  
