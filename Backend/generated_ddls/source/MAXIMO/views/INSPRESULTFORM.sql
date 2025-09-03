-- VIEW: MAXIMO.INSPRESULTFORM
CREATE OR REPLACE VIEW "MAXIMO"."INSPRESULTFORM" AS
select inspectionresult.resultnum, inspectionresult.createdate, inspectionresult.createdby, inspectionresult.asset, inspectionresult.location, inspectionresult.status, 
                    inspectionform.name, inspectionform.revision, inspquestion.groupseq, inspquestion.description as description_question, inspfield.description as description_field, inspfield.inspformnum, 
                    inspfield.inspquestionnum, inspfield.inspfieldnum, inspectionresult.orgid, inspectionresult.siteid from inspectionresult, inspectionform, inspquestion, inspfield where inspectionresult.inspformnum = inspectionform.inspformnum and inspectionresult.revision = inspectionform.revision and inspectionresult.orgid = inspectionform.orgid
                    and inspectionform.inspformnum = inspquestion.inspformnum and inspectionform.orgid = inspquestion.orgid and inspectionform.revision=inspquestion.revision
                    and inspquestion.inspquestionnum = inspfield.inspquestionnum and inspectionform.inspformnum = inspfield.inspformnum and inspectionform.orgid = inspfield.orgid 
                    and inspectionform.revision=inspfield.revision
