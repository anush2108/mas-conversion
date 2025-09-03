-- VIEW: MAXIMO.DCLNODESINBV
CREATE OR REPLACE VIEW "MAXIMO"."DCLNODESINBV" ("ASSETNUM") AS SELECT distinct asset.assetnum as assetnum from asset left join classstructure on asset.classstructureid = classstructure.classstructureid where asset.classstructureid is null or classstructure.showinassettopo=0;
