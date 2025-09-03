-- VIEW: MAXIMO.TLOAMASSETCONTRACT
CREATE OR REPLACE VIEW "MAXIMO"."TLOAMASSETCONTRACT" AS
select distinct asset.assetid,asset.assetnum, contract.contractnum,contract.contracttype, asset.siteid, contract.revisionnum, contract.historyflag,asset.rowstamp from contract,contractasset,warrantyasset,asset where asset.assetid=contractasset.assetid and contractasset.contractnum=contract.contractnum and contractasset.revisionnum=contract.revisionnum or asset.assetid=warrantyasset.assetid and warrantyasset.contractnum=contract.contractnum and warrantyasset.revisionnum=contract.revisionnum
