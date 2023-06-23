from programs.schema.schemas.schemamaker import SchemaMaker
from programs.schema.schema import buildTestRowdicts
from das_constants import CC

schema = SchemaMaker.fromName(CC.SCHEMA_HOUSEHOLD2010)
hht_rows = buildTestRowdicts(schema, dimlist=(0,4,5))