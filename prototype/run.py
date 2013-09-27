import schema_parser as parser
from zipf import zipf_variable
import math
import random
import io # located in io.py
from chunk import Chunk
from partition import Partition #do I need this?
from range_partition import RangePartition
import numpy as np

schema_defs = [
"array1<attr1:double,attr2:int64>[dim1=1:200000,1000,0,dim2=1:200000,1000,0]"
#,"array2<attr1:double,attr2:int64>[dim1=1:10,5,0,dim2=0:9,5,0]"
#,"array4<attr1:double,attr2:int64>[dim1=0:9,5,0,dim2=0:9,5,0]"
#,"array3<attr1:double,attr2:int64>[dim1=1:100,10,0,dim2=0:99,20,0]"
]

partition_defs = {
# quarters
'quarters':"0:1,1,100000,100000\
|2:1,100001,100000,100000\
|1:100001,1,100000,100000\
|3:100001,100001,100000,100000"
# rows
,'rows':"0:1,1,50000,200000\
|1:50001,1,50000,200000\
|2:100001,1,50000,200000\
|3:150001,1,50000,200000"
}

#partition_defs = [
##modis quarters
#"0:0,-1800000,-900000,20161,1800000,900000\
#|2:0,-1800000,0,20161,1800000,900001\
#|1:0,0,-900000,20161,1800001,900000\
#|3:0,0,0,20161,1800001,900001"
##modis rows
#,"0:0,-1800000,-900000,20161,3600001,450000\
#|1:0,-1800000,-450000,20161,3600001,450000\
#|2:0,-1800000,0,20161,3600001,450000\
#|3:0,-1800000,450000,20161,3600001,450001"
#]

#print "name:",name
#print "attributes:",attributes
#print "dimensions:",dimensions

DEFAULT_ALPHA = .74
DEFAULT_CHUNKSIZE = 100
DEFAULT_CUTOFF = .95

DEFAULT_ZIPF = zipf_variable(DEFAULT_ALPHA,DEFAULT_CHUNKSIZE)

def get_cellcounts(totalchunks,chunksize,distribution=DEFAULT_ZIPF):
  '''
  Create an array that generates the cell count for the given number of chunks using the given
  distribution

  Arguments:
    totalchunks  -- the number of chunks to generate cell counts for
    distribution -- a function representing a random variable with the desired distribution for the
                    cell counts (i.e. zipf, gaussian, uniform, etc.). Will be zipf by default
  '''
  cellcounts = []
  hotchunks = []
  for i in range(totalchunks):
    val = distribution()
    cellcounts.append(val)
    hotchunks.append(val > (DEFAULT_CUTOFF*chunksize))
  return (cellcounts,hotchunks)

def get_hotcold_cellcounts(totalchunks,chunksize,distribution=DEFAULT_ZIPF):
  hotchunks = 0
  coldchunks = 0
  cutoff = DEFAULT_CUTOFF * chunksize
  for i in range(totalchunks):
    val = distribution()
    if (i % 1000) == 0:
      print "completed",i,"calculations"
      print "val:",val
    if val > cutoff:
      hotchunks += 1
    else:
      coldchunks += 1
  return (hotchunks,coldchunks)

def build_chunk(Schema,chunk_id,cellcount,chunksize):
  chosen = []
  coords = []   # coordinates of nonempty cells
  attrvals = [] # attribute values of nonempty cells
  chunk = Chunk(Schema,chunk_id,cellcount,[],[]) # empty chunk for now

  if chunksize == 1:
    return [0]
  candidates = range(chunksize)
  if cellcount == chunksize:
    return candidates
  #for i in range(cellcount): # shuffle the first k items
  #  x = random.randint(i,chunksize-1) #should be possible to stay in place
  #  temp = candidates[x]
  #  candidates[x] = candidates[i]
  #  candidates[i] = temp
  #chosen = candidates[:cellcount]
  chosen = choose_randk(candidates,cellcount)
  #generate dimension coordinates
  for i in chosen:
    coords.append(Schema.compute_dimid(chunk_id,i))
  chunk.coordinates = coords
  #generate attribute values
  attrvals = generate_attributes(Schema.attributes,cellcount)
  chunk.attributes = attrvals
  print "chosen:",chosen
  #print "dimensions for cell",chosen[0],"of chunk",chunk_id,":",Schema.compute_dimid(chunk_id,chosen[0])
  print "coords for chunk",chunk_id,":",coords
  print "attributes for chunk",chunk_id,":",attrvals
  return chunk

def choose_randk(candidates,k):
  '''
  returns a list of k randomly selected items chosen from the given list
  '''
  l = len(candidates) - 1
  for i in range(k): # shuffle the first k items
    x = random.randint(i,l) #should be possible to stay in place
    temp = candidates[x]
    candidates[x] = candidates[i]
    candidates[i] = temp
  return candidates[:k]

def build_chunks(Schema,cellcounts,chunksize):
  for i,cellcount in enumerate(cellcounts):
    build_chunk(Schema,i,cellcount,chunksize)


def generate_attributes(attributes,cellcount,shift=3):
  result = []
  window = math.pow(10,3)
  numattrs = len(attributes)

  for attr in attributes:
    #array of length cellcount for each attribute
    x = []
    if attr.dtype in ['float','double']:
      x = [random.random() * window for j in range(cellcount)]
    else: # assume int or uint type
      x = [random.randint(1,window) for j in range(cellcount)]
    result.append(x)
  return result


# create schema and partition objects
(name,attributes,dimensions) = parser.ScidbSchema.parse_schema(schema_defs[0])
Schema = parser.ScidbSchema(name,attributes,dimensions)
(range_starts,range_widths) = RangePartition.parse(partition_defs['quarters'])
range1 = RangePartition(Schema,range_starts,range_widths)
(range_starts,range_widths) = RangePartition.parse(partition_defs['rows'])
range2 = RangePartition(Schema,range_starts,range_widths)

numnodes = max(range1.nodecount,range2.nodecount)
overlaps = []
non_overlaps1 = []
non_overlaps2 = []

'''
#compute overlap for all nodes
for i in range(numnodes):
  #print "range1 chunks on node "+str(i)+":",range1.get_chunks(i)
  #print "range2 chunks on node "+str(i)+":",range2.get_chunks(i)
  (overlap,non_overlap1,non_overlap2) = range1.compute_overlap(range2,i)
  overlaps.append(overlap)
  if len(non_overlap1) > 0:
    non_overlaps1.append(non_overlap1)
  if len(non_overlap2) > 0:
    non_overlaps2.append(non_overlap2)
  #print "overlap for node",i,"on range1 and range2:",overlap
  #print "non-overlap for node",i,"on range1:",non_overlap1
  #print "non-overlap for node"+str(i)+" on range2:",non_overlap2

non_overlaps = None
if len(non_overlaps1) < len(non_overlaps2):
  non_overlaps = non_overlaps2
else:
  non_overlaps = non_overlaps1

total_regions = len(overlaps) + len(non_overlaps)

print total_regions,"total regions of overlap and non-overlap"
'''

#generate the cellcounts
io.reset_offset()
# prepare file handles
(dim_handles,attr_handles,chunkmap_handle) = io.file_setup(Schema,path_prefix='data')
totalchunks = Schema.compute_totalchunks()
print "total chunks in",Schema.name,":",totalchunks
chunksize = Schema.compute_chunksize()
print "chunk size of",Schema.name,":",chunksize
#get zipf distribution ready
z = zipf_variable(DEFAULT_ALPHA,100)
#generate cellcounts for each chunk
(numhot,numcold) = get_hotcold_cellcounts(totalchunks,100,distribution=z)
#numhot_pernode = 1.0 * numhot / total_regions
#print "total hot chunks: ",numhot
#print "average hot chunks per node:",numhot_pernode

#generate and write chunks to disk
#for i in range(min(3,totalchunks)):
#  chunk = build_chunk(Schema,i,cellcounts[i],chunksize)
#  io.write_chunk(chunk,dim_handles,attr_handles,chunkmap_handle)

io.close_handles(dim_handles)
io.close_handles(attr_handles)
io.close_handle(chunkmap_handle)


'''
for schema_def in schema_defs: # for each listed schema
  #reset offset for chunk writing
  io.reset_offset()
  #build schema
  (name,attributes,dimensions) = parser.ScidbSchema.parse_schema(schema_def)
  Schema = parser.ScidbSchema(name,attributes,dimensions)
  # prepare file handles
  (dim_handles,attr_handles,chunkmap_handle) = io.file_setup(Schema,path_prefix='data')
  totalchunks = Schema.compute_totalchunks()
  print "total chunks in",Schema.name,":",totalchunks
  chunksize = Schema.compute_chunksize()
  print "chunk size of",Schema.name,":",chunksize
  #get zipf distribution ready
  z = zipf_variable(DEFAULT_ALPHA,chunksize)
  #generate cellcounts for each chunk
  cellcounts = get_cellcounts(totalchunks=totalchunks,chunksize=chunksize,distribution=z)
  print "cellcounts for",Schema.name,":",cellcounts
  #generate and write chunks to disk
  for i in range(min(3,totalchunks)):
    chunk = build_chunk(Schema,i,cellcounts[i],chunksize)
    io.write_chunk(chunk,dim_handles,attr_handles,chunkmap_handle)

  io.close_handles(dim_handles)
  io.close_handles(attr_handles)
  io.close_handle(chunkmap_handle)
 ''' 
