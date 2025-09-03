-- VIEW: MAXIMO.DMMAPPINGATTRIBUTEVIEW
CREATE OR REPLACE VIEW "MAXIMO"."DMMAPPINGATTRIBUTEVIEW" AS
select matt.objectname, matt.attributename, matt.persistent, matt.restricted from maxattribute matt where 1=1
