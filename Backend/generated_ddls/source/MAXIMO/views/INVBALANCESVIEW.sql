-- VIEW: MAXIMO.INVBALANCESVIEW
CREATE OR REPLACE VIEW "MAXIMO"."INVBALANCESVIEW" AS
select invbalances.itemnum, invbalances.location, invbalances.binnum, invbalances.lotnum, invbalances.curbal, invbalances.physcnt, invbalances.physcntdate, invbalances.reconciled, invbalances.orgid, invbalances.siteid, invbalances.itemsetid, invbalances.conditioncode, invbalances.stagingbin, invbalances.stagedcurbal, invbalances.invbalancesid, invbalances.rowstamp as rowstamp,inventory.rowstamp as rowstamp1,item.rowstamp as rowstamp2, inventory.abctype, inventory.issueunit, inventory.ccf, item.rotating, item.description, item.itemid,
invbalances.nextphycntdate from invbalances, inventory, item 
 where inventory.itemnum=item.itemnum and inventory.itemsetid=item.itemsetid and inventory.itemnum=invbalances.itemnum and inventory.itemsetid=invbalances.itemsetid and inventory.location=invbalances.location and inventory.siteid=invbalances.siteid
