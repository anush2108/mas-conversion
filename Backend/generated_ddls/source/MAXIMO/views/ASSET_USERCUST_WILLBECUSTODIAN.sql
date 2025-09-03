-- VIEW: MAXIMO.ASSET_USERCUST_WILLBECUSTODIAN
CREATE OR REPLACE VIEW "MAXIMO"."ASSET_USERCUST_WILLBECUSTODIAN" AS
SELECT
    addperson, assetlocusercustid, assetnum, iscustodian, isprimary, isuser, location,
    modifyperson, multiid, orgid, personid, removeperson, rowstamp, siteid,
    willbecustodian, willbeprimary, willbeuser
FROM assetlocusercust
WHERE assetnum IS NOT NULL
  AND addperson = 0
  AND willbecustodian = 1
