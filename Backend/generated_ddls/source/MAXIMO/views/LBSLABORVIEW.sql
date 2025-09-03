-- VIEW: MAXIMO.LBSLABORVIEW
CREATE OR REPLACE VIEW "MAXIMO"."LBSLABORVIEW" AS
select  lbs.refobject,lbs.key1,lbs.key2,lbs.longitude,lbs.latitude,lbs.altitude,lbs.locationaccuracy,lbs.altitudeaccuracy,lbs.heading,lbs.speed,lbs.lastupdate,lb.laborid,lb.laborcode,lb.orgid,am.amcrew,am.effectivedate,am.enddate,p.shiftnum from  lbslocation lbs left join labor lb on  lbs.key1 = lb.orgid and lbs.key2 =lb.laborcode left join amcrewlabor am on lb.laborcode = am.laborcode and lb.orgid = am.orgid  left join personcal p on lb.laborcode = p.personid and lb.orgid = p.orgid 
 where lbs.refobject = 'LABOR' and lb.status in (select value from synonymdomain where domainid = 'LABORSTATUS' and maxvalue ='ACTIVE')
