-- VIEW: MAXIMO.TLOAMDPAPARENT
CREATE OR REPLACE VIEW "MAXIMO"."TLOAMDPAPARENT" AS
select deployedasset.nodeid, dpacomputer.tloamparentid from deployedasset left outer join dpacomputer on deployedasset.nodeid = dpacomputer.nodeid
