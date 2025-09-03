-- VIEW: MAXIMO.RECONLINKRULE
CREATE OR REPLACE VIEW "MAXIMO"."RECONLINKRULE" AS
select reconrule.compset, reconrule.description, reconrule.fullcicompare, reconrule.hasld, reconrule.langcode, reconrule.reconruleid, reconrule.recontype, reconrule.rowstamp, reconrule.rulename, reconrule.ruletype from reconrule where ruletype='LINK'
