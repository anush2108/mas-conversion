-- VIEW: MAXIMO.EXTNODESINASTLOCREL
CREATE OR REPLACE VIEW "MAXIMO"."EXTNODESINASTLOCREL" ("ASSETNUM") AS SELECT distinct sourceassetnum as assetnum from assetlocrelation union select distinct targetassetnum as assetnum from assetlocrelation;
