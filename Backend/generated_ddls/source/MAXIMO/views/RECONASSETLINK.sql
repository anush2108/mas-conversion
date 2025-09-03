-- VIEW: MAXIMO.RECONASSETLINK
CREATE OR REPLACE VIEW "MAXIMO"."RECONASSETLINK" AS
select reconlink.assetclass, reconlink.assetid, reconlink.assetnum, reconlink.compset, reconlink.guid, reconlink.linkdate, reconlink.linkrulename, reconlink.nodeid, reconlink.reconlinkid, reconlink.recontype, reconlink.rowstamp, reconlink.siteid from reconlink where recontype in (select value from synonymdomain where domainid ='RECONTYPE' and maxvalue in ('ASSET')) and compset in (select value from synonymdomain where domainid ='RECONTYPE' and maxvalue in ('DEPLOYED ASSET'))
