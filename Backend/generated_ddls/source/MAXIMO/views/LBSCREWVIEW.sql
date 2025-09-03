-- VIEW: MAXIMO.LBSCREWVIEW
CREATE OR REPLACE VIEW "MAXIMO"."LBSCREWVIEW" AS
select  lbs.refobject,lbs.key1,lbs.key2,lbs.longitude,lbs.latitude,lbs.altitude,lbs.locationaccuracy,lbs.altitudeaccuracy,lbs.heading,lbs.speed,lbs.lastupdate,cr.amcrewid,cr.amcrew,cr.orgid,cr.shiftnum from  lbslocation lbs left join amcrew cr on  lbs.key1 = cr.orgid and lbs.key2 = cr.amcrew   where lbs.refobject = 'AMCREW' and cr.status in (select value from synonymdomain where domainid = 'CREWSTATUS' and maxvalue ='ACTIVE')
