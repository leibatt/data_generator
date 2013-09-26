from scidb_schema import ScidbSchema
from scidb_attribute import ScidbAttribute
from scidb_dimension import ScidbDimension

print "testing SciDB schema parse functon"
schema_def = "array<attr1:double,attr2:int64>[dim1=1:10,10,0,dim2=0:9,10,0]"
(name,attributes,dimensions) = ScidbSchema.parse_schema(schema_def)

print "name:",name
print "attributes:",attributes
print "dimensions:",dimensions
