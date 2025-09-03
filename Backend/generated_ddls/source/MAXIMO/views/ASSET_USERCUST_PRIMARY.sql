-- VIEW: MAXIMO.ASSET_USERCUST_PRIMARY
CREATE OR REPLACE VIEW "MAXIMO"."ASSET_USERCUST_PRIMARY" AS
SELECT
    addperson, assetlocusercustid, assetnum, iscustodian, isprimary, isuser, location,
    modifyperson, multiid, orgid, personid, removeperson, rowstamp, siteid,
    willbecustodian, willbeprimary, willbeuser
FROM assetlocusercust
WHERE assetnum IS NOT NULL
  AND addperson = 0
  AND isprimary = 1
