-- VIEW: MAXIMO.DMMAPPINGATTRIBUTEVIEW
CREATE OR REPLACE VIEW "MAXIMO"."DMMAPPINGATTRIBUTEVIEW" ("OBJECTNAME", "ATTRIBUTENAME", "PERSISTENT", "RESTRICTED") AS SELECT matt.objectname, matt.attributename, matt.persistent, matt.restricted from maxattribute matt where 1=1;
