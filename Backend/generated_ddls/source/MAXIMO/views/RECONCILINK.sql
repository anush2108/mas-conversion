-- VIEW: MAXIMO.RECONCILINK
CREATE OR REPLACE VIEW "MAXIMO"."RECONCILINK" AS
select reconlink.nodeid, reconlink.assetclass, reconlink.assetid, reconlink.assetnum, reconlink.compset, reconlink.guid, reconlink.linkdate, reconlink.linkrulename, reconlink.reconlinkid, reconlink.recontype, reconlink.rowstamp, reconlink.siteid from reconlink where recontype in (select value from synonymdomain where domainid ='RECONTYPE' and maxvalue in ('CI')) and compset in (select value from synonymdomain where domainid ='RECONTYPE' and maxvalue in ('ACTUAL CI'))
