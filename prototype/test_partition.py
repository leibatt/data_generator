import schema_parser as parser
from partition import Partition #do I need this?
from range_partition import RangePartition
import chunk_util as cutil

schema_defs = [
"array1<attr1:double,attr2:int64>[dim1=1:100,25,0,dim2=1:100,25,0]"
#,"array2<attr1:double,attr2:int64>[dim1=1:10,5,0,dim2=0:9,5,0]"
#,"array4<attr1:double,attr2:int64>[dim1=0:9,5,0,dim2=0:9,5,0]"
#,"array3<attr1:double,attr2:int64>[dim1=1:100,10,0,dim2=0:99,20,0]"
]

partition_defs = {
# quarters
'quarters':"0:1,1,50,50\
|2:1,51,50,50\
|1:51,1,50,50\
|3:51,51,50,50"
# rows
,'rows':"0:1,1,25,100\
|1:26,1,25,100\
|2:51,1,25,100\
|3:76,1,25,100"
}

(name,attributes,dimensions) = parser.ScidbSchema.parse_schema(schema_defs[0])
Schema = parser.ScidbSchema(name,attributes,dimensions)
(range_starts,range_widths) = RangePartition.parse(partition_defs['quarters'])
print "range starts for quarters:",range_starts
print "range widths for quarters:",range_widths
range1 = RangePartition(Schema,range_starts,range_widths)
(range_starts,range_widths) = RangePartition.parse(partition_defs['rows'])
print "range starts for rows:",range_starts
print "range widths for rows:",range_widths
range2 = RangePartition(Schema,range_starts,range_widths)

numnodes = max(range1.nodecount,range2.nodecount)
overlaps = []
non_overlaps1 = []
non_overlaps2 = []

#compute overlap for node 0
for i in range(numnodes):
  #print "range1 chunks on node "+str(i)+":",range1.get_chunks(i)
  #print "range2 chunks on node "+str(i)+":",range2.get_chunks(i)
  (overlap,non_overlap1,non_overlap2) = range1.compute_overlap(range2,i)
  overlaps.append(overlap)
  if len(non_overlap1) > 0:
    non_overlaps1.append(non_overlap1)
  if len(non_overlap2) > 0:
    non_overlaps2.append(non_overlap2)
  print "overlap for node",i,"on range1 and range2:",overlap
  print "non-overlap for node",i,"on range1:",non_overlap1
  print "non-overlap for node"+str(i)+" on range2:",non_overlap2

non_overlaps = None
if len(non_overlaps1) < len(non_overlaps2):
  non_overlaps = non_overlaps2
else:
  non_overlaps = non_overlaps1

total_regions = len(overlaps) + len(non_overlaps)

print total_regions,"total regions of overlap and non-overlap"


