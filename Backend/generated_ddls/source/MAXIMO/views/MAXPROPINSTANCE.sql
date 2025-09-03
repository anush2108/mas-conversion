-- VIEW: MAXIMO.MAXPROPINSTANCE
CREATE OR REPLACE VIEW "MAXIMO"."MAXPROPINSTANCE" AS
select maxpropvalue.accesstype, maxpropvalue.changeby, maxpropvalue.changedate, maxpropvalue.encryptedvalue, maxpropvalue.maxpropvalueid, maxpropvalue.propname, maxpropvalue.propvalue, maxpropvalue.rowstamp, maxpropvalue.serverhost, maxpropvalue.servername from maxpropvalue where servername  not in ('COMMON')
