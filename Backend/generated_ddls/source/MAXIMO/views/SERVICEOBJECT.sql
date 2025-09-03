-- VIEW: MAXIMO.SERVICEOBJECT
CREATE OR REPLACE VIEW "MAXIMO"."SERVICEOBJECT" AS
select servicename as serviceobject, description as servicedesc, cast(null as varchar(100)) as description, maxserviceid as maxserviceid, 0 as maxobjectid from maxservice union all select distinct maxservice.servicename || '.' || maxobject.objectname as serviceobject,  cast(null as varchar(100)) as servicedesc, maxobject.description as description, 0 as maxserviceid, maxobject.maxobjectid as maxobjectid from maxservice,maxobject where maxobject.servicename=maxservice.servicename
