-- VIEW: MAXIMO.COMPANYSETFILTER
CREATE OR REPLACE VIEW "MAXIMO"."COMPANYSETFILTER" AS
select distinct sets.setid, maxuser.loginid as userid, groupuser.userid as maxuser, applicationauth.app  from sets, organization, groupuser, applicationauth, maxuser  where sets.setid = organization.companysetid
and settype in (select value from synonymdomain where domainid='SETTYPE' and maxvalue = 'COMPANY')
and maxuser.userid = groupuser.userid
and organization.orgid in (select orgid from orgfilter where userid = maxuser.loginid and app = applicationauth.app)
