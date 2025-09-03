-- VIEW: MAXIMO.TLOAMPROMOTE
CREATE OR REPLACE VIEW "MAXIMO"."TLOAMPROMOTE" AS
select deployedasset.nodeid, deployedasset.assetclass, deployedasset.nodename, deployedasset.domainname, deployedasset.guid, deployedasset.tloamhash,deployedasset.description, deployedasset.siteid as dpldsiteid, deployedasset.serialnumber, deployedasset.tloamispromoted,dpacomputer.tloamparentid from deployedasset left join dpacomputer on deployedasset.nodeid = dpacomputer.nodeid
