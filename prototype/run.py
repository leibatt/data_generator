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
,"array5<attr1:int64,attr2:int64>[dim1=1:100,25,0,dim2=1:100,25,0]"
,"array6<attr1:double,attr2:int64>[dim1=1:1000,50,0,dim2=1:1000,50,0]"
#,"array2<attr1:double,attr2:int64>[dim1=1:10,5,0,dim2=0:9,5,0]"
#,"array4<attr1:double,attr2:int64>[dim1=0:9,5,0,dim2=0:9,5,0]"
,"array3<attr1:int64>[dim1=1:100,10,0,dim2=1:100,10,0]"
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
# quarters
,'quarters2':"0:1,1,50,50\
|2:1,51,50,50\
|1:51,1,50,50\
|3:51,51,50,50"
# rows
,'rows2':"0:1,1,25,100\
|1:26,1,25,100\
|2:51,1,25,100\
|3:76,1,25,100"
,'quarters3':"0:1,1,500,500\
|2:1,501,500,500\
|1:501,1,500,500\
|3:501,501,500,500"
# rows
,'rows3':"0:1,1,250,1000\
|1:251,1,250,1000\
|2:501,1,250,1000\
|3:751,1,250,1000"
# quarters
,'quarters4':"0:1,1,50,50\
|2:1,51,50,50\
|1:51,1,50,50\
|3:51,51,50,50"
# rows
,'rows4':"0:1,1,30,100\
|1:31,1,20,100\
|2:51,1,30,100\
|3:81,1,20,100"

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

DEFAULT_ALPHA = 1.
DEFAULT_CHUNKSIZE = 100
DEFAULT_CUTOFF = .8

min_chunksize = .05
max_chunksize = .5
z_range = 10

join_alignment = .5

DEFAULT_ZIPF = zipf_variable(DEFAULT_ALPHA,DEFAULT_CHUNKSIZE)

##############################
'''
myschema = schema_defs[0]
myrange1 = 'quarters'
myrange2 = 'rows'
'''

'''
myschema = schema_defs[1]
myrange1 = 'quarters2'
myrange2 = 'rows2'
'''

'''
myschema = schema_defs[2]
myrange1 = 'quarters3'
myrange2 = 'rows3'
'''

myschema = schema_defs[3]
myrange1 = 'quarters4'
myrange2 = 'rows4'

##############################

def get_hotcold_cellcounts(totalchunks,n,distribution=DEFAULT_ZIPF):
  hotchunks = []
  coldchunks = []
  cutoff = DEFAULT_CUTOFF * n
  for i in range(totalchunks):
    val = distribution()
    if val >= cutoff:
      hotchunks.append(val)
    else:
      coldchunks.append(val)
  return (hotchunks,coldchunks)

def build_ABchunks(Schema,chunk_id,countA,countB,chunksize,overlap=1.):
  maxcount = max(countA,countB)
  mincount = min(countA,countB)
  # build the maxchunk from scratch
  maxchunk = build_chunk(Schema,chunk_id,maxcount,chunksize)
  if maxcount == mincount: # they are the same, so stop here
    return (maxchunk,maxchunk)
  attrlen = len(Schema.attributes)
  minchunk = Chunk(Schema,chunk_id,mincount,[],[[]]*attrlen) # empty chunk for now
  minitems = choose_randk(range(maxcount),mincount) # copy random subset of the larger chunk

  # copy the selected cells from maxchunk to minchunk
  for i in minitems:
    minchunk.coordinates.append(maxchunk.coordinates[i])
    for j in range(attrlen):
      minchunk.attributes[j].append(maxchunk.attributes[j][i])
  if countA > countB:
    chunkA = maxchunk
    chunkB = minchunk
  else:
    chunkA = minchunk
    chunkB = maxchunk
  return (chunkA,chunkB)
  

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
  chosen = choose_randk(candidates,cellcount)
  #generate dimension coordinates
  for i in chosen:
    coords.append(Schema.compute_dimid(chunk_id,i))
  chunk.coordinates = coords
  #generate attribute values
  attrvals = generate_attributes(Schema.attributes,cellcount)
  chunk.attributes = attrvals
  #print "chosen:",chosen
  #print "dimensions for cell",chosen[0],"of chunk",chunk_id,":",Schema.compute_dimid(chunk_id,chosen[0])
  #print "coords for chunk",chunk_id,":",coords
  #print "attributes for chunk",chunk_id,":",attrvals
  return chunk

def shuffle_firstk(candidates,k):
  '''
  shuffles the first k items, and returns the resulting list. this assumes k < len(candidates)
  '''
  l = len(candidates) - 1
  for i in range(k): # shuffle the first k items
    x = random.randint(i,l) #should be possible to stay in place
    temp = candidates[x]
    candidates[x] = candidates[i]
    candidates[i] = temp
  return candidates #return everything

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

def map_chunksize(z,minc,maxc,chunksize):
  global z_range
  maxc = 1. * maxc * chunksize
  minc = 1. * minc * chunksize
  return math.ceil(1. * z / z_range * (maxc - minc) + minc)

# create schema and partition objects
(name,attributes,dimensions) = parser.ScidbSchema.parse_schema(myschema)
Schema = parser.ScidbSchema(name,attributes,dimensions)
(range_starts,range_widths) = RangePartition.parse(partition_defs[myrange1])
range1 = RangePartition(Schema,range_starts,range_widths)
(range_starts,range_widths) = RangePartition.parse(partition_defs[myrange2])
range2 = RangePartition(Schema,range_starts,range_widths)

numnodes = max(range1.nodecount,range2.nodecount)
overlaps = []
non_overlaps1 = []
non_overlaps2 = []

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

#generate the cellcounts
io.reset_offset()
io.reset_offset(aorb='B')
# prepare file handles
(dim_handlesA,attr_handlesA,chunkmap_handleA) = io.file_setup(Schema,path_prefix='data')
(dim_handlesB,attr_handlesB,chunkmap_handleB) = io.file_setup(Schema,aorb='B',path_prefix='data')
totalchunks = Schema.compute_totalchunks()
print "total chunks in",Schema.name,":",totalchunks
chunksize = Schema.compute_chunksize()
print "chunk size of",Schema.name,":",chunksize
#get zipf distribution ready
z = zipf_variable(DEFAULT_ALPHA,z_range)
#generate cellcounts for each chunk
(hotchunks,coldchunks) = get_hotcold_cellcounts(totalchunks,z_range,distribution=z)
numhot = len(hotchunks)
numhot_perregion = numhot / total_regions
hotcounts = [numhot_perregion] * total_regions

#fraction of hot chunks *not* aligned in B
numnotaligned = int(1. * numhot * join_alignment)
# divided evenly across all regions
numnotaligned_perregion = numnotaligned / total_regions
nonaligncounts = [numnotaligned_perregion] * total_regions

remainder = numhot - total_regions * numhot_perregion
remainder_nonaligned = numnotaligned - numnotaligned_perregion * total_regions
for i in range(total_regions):
  if remainder > 0:
    hotcounts[i] +=1
    remainder -= 1
  if remainder_nonaligned > 0:
    nonaligncounts[i] +=1
    remainder_nonaligned -= 1

print "total hot chunks: ",numhot
print "hot chunks per region:",hotcounts
print "total chunks *not* aligned in B:",numnotaligned
print "non-aligned chunks per region:",nonaligncounts

# put all these regions together, doesn't matter because they don't overlap
regions = overlaps + non_overlaps

hot_indexA = 0
cold_indexA = 0
hot_indexB = 0
cold_indexB = 0
cellcountsA = [0] * totalchunks # cell counts for all chunks
cellcountsB = [0] * totalchunks # cell counts for all chunks

#Build array A
#arrange cellcounts per region:
for i,region in enumerate(regions):
  #this is a list
  #pick k chunks from this region to be hot
  h = hotcounts[i]
  x = nonaligncounts[i]
  # can actually shuffle first k+x elements here, where x is # hotchunks in array B that should
  # *not* align with hot chunks in array A for this region
  k = shuffle_firstk(region,h+x) #list of chunk indexes with first k shuffled
  regions[i] = k # save this for later for use with array B
  #print "k:",k[:100]

  #first k are made hot in A
  for j in range(h):
    cellcountsA[k[j]] = hotchunks[hot_indexA] #the chunk at index k[j] is now hot
    hot_indexA += 1

  #the rest are cold in A
  for j in range(h,len(k)):
    cellcountsA[k[j]] = coldchunks[cold_indexA] #the chunk at index k[j] is now cold
    cold_indexA += 1

  for j in (range(h-x)+range(h,h+x)):
    cellcountsB[k[j]] = hotchunks[hot_indexB]
    hot_indexB += 1

  for j in (range(h-x,h)+range(h+x,len(k))):
    cellcountsB[k[j]] = coldchunks[cold_indexB]
    cold_indexB += 1


if (hot_indexA != len(hotchunks)) or (cold_indexA != len(coldchunks)):
  print "hot_indexA:",hot_indexA,",hotchunk count:",len(hotchunks)
  print "cold_indexA:",cold_indexA,",coldchunk count:",len(coldchunks)
  raise Exception("bad math here for array A")

if (hot_indexB != len(hotchunks)) or (cold_indexB != len(coldchunks)):
  print "hot_index:",hot_indexB,",hotchunk count:",len(hotchunks)
  print "cold_index:",cold_indexB,",coldchunk count:",len(coldchunks)
  raise Exception("bad math here for array B")


#print "raw cellcounts for array A:",cellcountsA,",mean:",np.mean(cellcountsA)

for i in range(1,z_range+1):
  print str(i)+":",cellcountsA.count(i)

for i in range(totalchunks):
  cellcountsA[i] = map_chunksize(cellcountsA[i],min_chunksize,max_chunksize,chunksize)
  cellcountsB[i] = map_chunksize(cellcountsB[i],min_chunksize,max_chunksize,chunksize)

print "new cellcounts for array A:",cellcountsA,",mean:",np.mean(cellcountsA)
print "regions:",regions
print "totalchunks:",totalchunks
#print "mean cellcount:",np.mean(cellcountsA)

#generate and write chunks to disk
for i in range(totalchunks):
  (chunkA,chunkB) = build_ABchunks(Schema,i,int(cellcountsA[i]),int(cellcountsB[i]),chunksize)
  #chunk = build_chunk(Schema,i,int(cellcountsA[i]),chunksize)
  #io.write_chunk_binary(chunkA,dim_handlesA,attr_handlesA,chunkmap_handleA)
  io.write_chunk(chunkA,dim_handlesA,attr_handlesA,chunkmap_handleA)
  #io.write_chunk_binary(chunkB,dim_handlesB,attr_handlesB,chunkmap_handleB)
  io.write_chunk(chunkB,dim_handlesB,attr_handlesB,chunkmap_handleB,aorb='B')

io.close_handles(dim_handlesA)
io.close_handles(attr_handlesA)
io.close_handle(chunkmap_handleA)
io.close_handles(dim_handlesB)
io.close_handles(attr_handlesB)
io.close_handle(chunkmap_handleB)

