-- VIEW: MAXIMO.RECONCOMPRULE
CREATE OR REPLACE VIEW "MAXIMO"."RECONCOMPRULE" AS
select reconrule.compset, reconrule.description, reconrule.fullcicompare, reconrule.hasld, reconrule.langcode, reconrule.reconruleid, reconrule.recontype, reconrule.rowstamp, reconrule.rulename, reconrule.ruletype from reconrule where ruletype='COMPARISON'
