-- VIEW: MAXIMO.TLOAMDPAPARENT
CREATE OR REPLACE VIEW "MAXIMO"."TLOAMDPAPARENT" ("NODEID", "TLOAMPARENTID") AS SELECT deployedasset.nodeid, dpacomputer.tloamparentid from deployedasset left outer join dpacomputer on deployedasset.nodeid = dpacomputer.nodeid;
