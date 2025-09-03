-- VIEW: MAXIMO.ASSET_USERCUST_SITE_MAIN
CREATE OR REPLACE VIEW "MAXIMO"."ASSET_USERCUST_SITE_MAIN" AS
SELECT
    addperson, assetlocusercustid, assetnum, iscustodian, isprimary, isuser, location,
    modifyperson, multiid, orgid, personid, removeperson, rowstamp, siteid,
    willbecustodian, willbeprimary, willbeuser
FROM assetlocusercust
WHERE assetnum IS NOT NULL
  AND addperson = 0
  AND siteid = 'MAIN'
