-- VIEW: MAXIMO.INVUSELINEREVIEW
CREATE OR REPLACE VIEW "MAXIMO"."INVUSELINEREVIEW" AS
select matrectransid, fromsiteid, invuselineid, invpicklistid, enterby, transdate, issuetype, quantity, frombin, tobin from matrectrans where fromsiteid is not null and invuseid is not null and invuselineid is not null and invpicklistid is not null 
				union all 
				select matusetransid, siteid, invuselineid, invpicklistid, enterby, transdate, issuetype, qtyrequested, binnum, null as tobin
				from matusetrans 
				where siteid is not null and invuseid is not null and invuselineid is not null and invpicklistid is not null
