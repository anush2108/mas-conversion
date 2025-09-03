-- VIEW: MAXIMO.ITEMSETFILTER
CREATE OR REPLACE VIEW "MAXIMO"."ITEMSETFILTER" AS
select distinct sets.setid, maxuser.loginid as userid, groupuser.userid as maxuser, applicationauth.app  from sets, organization, groupuser, applicationauth, maxuser  where sets.setid = organization.itemsetid
and settype in (select value from synonymdomain where domainid='SETTYPE' and maxvalue = 'ITEM')
and maxuser.userid = groupuser.userid
and organization.orgid in (select orgid from orgfilter where userid = maxuser.loginid and app = applicationauth.app)
