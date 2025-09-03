-- VIEW: MAXIMO.VIEWSECURITYDET
CREATE OR REPLACE VIEW "MAXIMO"."VIEWSECURITYDET" ("GROUPNAME", "APPNAME", "DESCRIPTION") AS SELECT groupname, appname, (select description from maxapps where app=appname) as description from reportappauth where 1=1 group by groupname,appname;
