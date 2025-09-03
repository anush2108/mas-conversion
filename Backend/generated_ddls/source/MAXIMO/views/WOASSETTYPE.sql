-- VIEW: MAXIMO.WOASSETTYPE
CREATE OR REPLACE VIEW "MAXIMO"."WOASSETTYPE" AS
select workorder.workorderid as workorderid , workorder.status as status ,workorder.wonum as wonum, workorder.orgid as orgid, workorder.siteid as siteid, workorder.origrecordid as origrecordid, workorder.origrecordclass as origrecordclass, workorder.statusdate as statusdate,
workorder.assetnum as assetnum,workorder.pmnum as pmnum, workorder.jpnum as jpnum, workorder.location as location, workorder.failurecode as failurecode,
workorder.worktype as worktype ,workorder.actlabhrs as actlabhrs , workorder.rowstamp as rowstamp ,asset.assettype as assettype,
asset.rowstamp as rowstamp1 from asset RIGHT OUTER JOIN workorder on workorder.assetnum =asset.assetnum and workorder.siteid=asset.siteid and workorder.orgid=asset.orgid
