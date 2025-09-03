-- VIEW: MAXIMO.ESCCOMMLOG
CREATE OR REPLACE VIEW "MAXIMO"."ESCCOMMLOG" AS
select commlog.bcc, commlog.cc, commlog.commlogid, commlog.commloguid, commlog.createby, commlog.createdate, commlog.inbound, commlog.issendfail, commlog.keepfailed, commlog.message, commlog.orgobject, commlog.ownerid, commlog.ownertable, commlog.replyto, commlog.rowstamp, commlog.sendfrom, commlog.sendto, commlog.subject, commlog.uniqueid from commlog where orgobject = 'ESCALATION'
