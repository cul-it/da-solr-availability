package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ChangeDetector {

  public static Map<String,Set<Change>> detectChangedInstances(
      Connection inventory, OkapiClient okapi, Timestamp since ) throws SQLException, IOException {

    Map<String,Set<Change>> changes = new HashMap<>();

    int limit = 500;
    Timestamp modDateCursor = since;
    List<Map<String, Object>> changedInstances;

    do {
      changedInstances = okapi.queryAsList("/instance-storage/instances",
          "metadata.updatedDate>"+modDateCursor.toInstant().toString()+
          " sortBy metadata.updatedDate",limit);
      INSTANCE: for (Map<String,Object> instance : changedInstances) {

        String hrid = (String)instance.get("hrid");
        String instanceJson = mapper.writeValueAsString(instance);
        Map<String,String> metadata = (Map<String,String>) instance.get("metadata");
        Timestamp modDate = Timestamp.from(Instant.parse(
            metadata.get("updatedDate").replace("+00:00","Z")));
        modDateCursor = modDate;

        if ( getPreviousInstance == null )
          getPreviousInstance = inventory.prepareStatement(
              "SELECT content FROM instanceFolio WHERE hrid = ?");
        getPreviousInstance.setString(1, hrid);
        try ( ResultSet rs = getPreviousInstance.executeQuery() ) {
          while (rs.next()) if (rs.getString("content").equals(instanceJson)) continue INSTANCE;
        }

        String id = (String)instance.get("id");
        Object o1 = instance.get("discoverySuppress");
        boolean active1 =
            (o1 == null)?false:String.class.isInstance(o1)?Boolean.valueOf((String)o1):(boolean)o1;
        Object o2 = instance.get("staffSuppress");
        boolean active2 =
            (o2 == null)?false:String.class.isInstance(o2)?Boolean.valueOf((String)o2):(boolean)o2;
        boolean active = active1 && active2;
        String source = (String)instance.get("source");
        if (replaceInstance == null)
          replaceInstance = inventory.prepareStatement(
              "REPLACE INTO instanceFolio (id, hrid, active, source, moddate, content) "+
              " VALUES (?,?,?,?,?,?)");
        replaceInstance.setString(1, id);
        replaceInstance.setString(2, hrid);
        replaceInstance.setBoolean(3, active);
        replaceInstance.setString(4, source);
        replaceInstance.setTimestamp(5, modDate);
        replaceInstance.setString(6, instanceJson);
        try {
          replaceInstance.executeUpdate();
        } catch ( SQLException e ) {
          e.printStackTrace();
          System.out.println(id);
          System.out.println(hrid);
          System.out.println(instanceJson);
          throw new SQLException( e );
        }

        Change c = new Change(Change.Type.INSTANCE,id,"Instance modified",
            modDate,null,trackUserChange( inventory, instanceJson ));
        if ( ! changes.containsKey(hrid)) {
          Set<Change> t = new HashSet<>();
          t.add(c);
          changes.put(hrid,t);
        }
        changes.get(hrid).add(c);

        if ( ! source.equals("MARC") ) continue INSTANCE;

        String marc = null;
        String srsQuery = "/source-storage/records/"+id+"/formatted?idType=INSTANCE";
        try {
          marc = okapi.query(srsQuery).replaceAll("\\s*\\n\\s*", " ");
        } catch (IOException e) {
          if ( e.getMessage().equals("Not Found") ) {
            // If MARC not found, wait 3 seconds and try one more time.
            System.out.printf("MARC Record Not Found in SRS: %s %s\n",hrid,id);
            try {
              marc = okapi.query("/source-storage/records/"+id+"/formatted?idType=INSTANCE")
                  .replaceAll("\\s*\\n\\s*", " ");
            } catch (IOException e2) {
              if ( e2.getMessage().equals("Not Found") ) {
                System.out.printf("MARC Record Not Found in SRS: %s %s\n",hrid,id);
                continue INSTANCE;
              }
            }
          }
        }
        if ( getPreviousBib == null )
          getPreviousBib = inventory.prepareStatement(
              "SELECT content FROM bibFolio WHERE instanceHrid = ?");
        getPreviousBib.setString(1, hrid);
        try ( ResultSet rs = getPreviousBib.executeQuery() ) {
          while (rs.next()) if (rs.getString("content").equals(marc)) continue INSTANCE;
        }

        Matcher m = modDateP.matcher(marc);
        Timestamp marcTimestamp = (m.matches())
            ? Timestamp.from(Instant.parse(m.group(1).replace("+0000","Z"))): null;
        if ( replaceBib == null )
          replaceBib = inventory.prepareStatement(
              "REPLACE INTO bibFolio (instanceHrid,moddate,content) VALUES (?,?,?)");
        replaceBib.setString(1, hrid);
        replaceBib.setTimestamp(2, marcTimestamp);
        replaceBib.setString(3, marc);
        replaceBib.executeUpdate();
      }
    } while (changedInstances.size() == limit);

    return changes;
  }

  public static Map<String,Set<Change>> detectChangedHoldings(
      Connection inventory, OkapiClient okapi, Timestamp since ) throws SQLException, IOException {

    Map<String,Set<Change>> changes = new HashMap<>();

    int limit = 2000;
    Timestamp modDateCursor = since;
    List<Map<String, Object>> changedHoldings;

    do {
      changedHoldings = okapi.queryAsList("/holdings-storage/holdings",
          "metadata.updatedDate>"+modDateCursor.toInstant().toString()+
          " sortBy metadata.updatedDate",limit);
      HOLDING: for (Map<String,Object> holding : changedHoldings) {
        
        String hrid = (String)holding.get("hrid");
        String holdingJson = mapper.writeValueAsString(holding);
        Map<String,String> metadata = (Map<String,String>) holding.get("metadata");
        Timestamp modDate = Timestamp.from(Instant.parse(
            metadata.get("updatedDate").replace("+00:00","Z")));
        modDateCursor = modDate;

        if ( getPreviousHolding == null )
          getPreviousHolding = inventory.prepareStatement(
              "SELECT content FROM holdingFolio WHERE hrid = ?");
        getPreviousHolding.setString(1, hrid);
        try ( ResultSet rs = getPreviousHolding.executeQuery() ) {
          while (rs.next()) if (rs.getString("content").equals(holdingJson)) continue HOLDING;
        }

        String instanceId = (String)holding.get("instanceId");
        if (getInstanceHridByInstanceId == null)
          getInstanceHridByInstanceId = inventory.prepareStatement(
              "SELECT hrid FROM instanceFolio WHERE id = ?");
        getInstanceHridByInstanceId.setString(1,instanceId);
        String instanceHrid = null;
        try (ResultSet rs = getInstanceHridByInstanceId.executeQuery() ) {
          while (rs.next()) {
            instanceHrid = rs.getString(1);
          }
        }

        if ( instanceHrid == null ) {
          System.out.println("Holding "+hrid+" can't be tracked to instance, not queueing for index.");
          continue;
        }

        String id = (String)holding.get("id");
        Object o = holding.get("discoverySuppress");
        boolean active =
            (o == null)?false:String.class.isInstance(o)?Boolean.valueOf((String)o):(boolean)o;
        if (replaceHolding == null)
          replaceHolding = inventory.prepareStatement(
              "REPLACE INTO holdingFolio (id,hrid,instanceId,instanceHrid,active,moddate,content) "+
              " VALUES (?,?,?,?,?,?,?)");
        replaceHolding.setString(1, id);
        replaceHolding.setString(2, hrid);
        replaceHolding.setString(3, instanceId);
        replaceHolding.setString(4, instanceHrid);
        replaceHolding.setBoolean(5, active);
        replaceHolding.setTimestamp(6, modDate);
        replaceHolding.setString(7, holdingJson);
        replaceHolding.executeUpdate();

        Change c = new Change(Change.Type.HOLDING,id,"Holding modified",
            modDate,null,trackUserChange( inventory, holdingJson ));
        if ( ! changes.containsKey(instanceHrid)) {
          Set<Change> t = new HashSet<>();
          t.add(c);
          changes.put(instanceHrid,t);
        }
        changes.get(instanceHrid).add(c);

      }
    } while (changedHoldings.size() == limit);

    return changes;
  }

  public static Map<String,Set<Change>> detectChangedItems(
      Connection inventory, OkapiClient okapi, Timestamp since ) throws SQLException, IOException {

    Map<String,Set<Change>> changes = new HashMap<>();

    int limit = 5000;
    Timestamp modDateCursor = since;
    List<Map<String, Object>> changedItems;

    do {
      changedItems = okapi.queryAsList("/item-storage/items",
          "metadata.updatedDate>"+modDateCursor.toInstant().toString()+
          " sortBy metadata.updatedDate",limit);
      for (Map<String,Object> item : changedItems) {
        
        String hrid = (String)item.get("hrid");
        String itemJson = mapper.writeValueAsString(item);
        Map<String,String> metadata = (Map<String,String>) item.get("metadata");
        Timestamp modDate = Timestamp.from(Instant.parse(
            metadata.get("updatedDate").replace("+00:00","Z")));
        modDateCursor = modDate;

        if ( getPreviousItem == null )
          getPreviousItem = inventory.prepareStatement("SELECT content FROM itemFolio WHERE hrid = ?");
        getPreviousItem.setString(1, hrid);
        boolean changed = true;
        try ( ResultSet rs = getPreviousItem.executeQuery() ) {
          while (rs.next()) if (rs.getString("content").equals(itemJson)) changed = false;
        }
        if ( ! changed ) continue;

        String holdingId = (String)item.get("holdingsRecordId");
        if (getItemParentage == null)
          getItemParentage = inventory.prepareStatement(
              "SELECT instanceHrid, hrid FROM holdingFolio WHERE id = ?");
        getItemParentage.setString(1,holdingId);
        String instanceHrid = null;
        String holdingHrid = null;
        try (ResultSet rs = getItemParentage.executeQuery() ) {
          while (rs.next()) {
            instanceHrid = rs.getString(1);
            holdingHrid = rs.getString(2);
          }
        }
        if ( instanceHrid == null ) {
          System.out.println("Item "+hrid+" can't be tracked to instance, not queueing for index.");
          continue;
        }

        String id = (String)item.get("id");
        String barcode = (item.containsKey("barcode"))?((String)item.get("barcode")).trim():null;
        if (barcode != null && barcode.length()>14) {
          System.out.println("Barcode too long. Omitting ["+hrid+"/"+barcode+"]");
          barcode = null;
        }
        if (replaceItem == null)
          replaceItem = inventory.prepareStatement(
              "REPLACE INTO itemFolio (id, hrid, holdingId, holdingHrid, moddate, barcode, content) "+
              " VALUES (?,?,?,?,?,?,?)");
        replaceItem.setString(1, id);
        replaceItem.setString(2, hrid);
        replaceItem.setString(3, holdingId);
        replaceItem.setString(4, holdingHrid);
        replaceItem.setTimestamp(5, modDate);
        replaceItem.setString(6, barcode);
        replaceItem.setString(7, itemJson);
        replaceItem.executeUpdate();

        Change c = new Change(Change.Type.ITEM,id,"Item modified",
            modDate,null,trackUserChange( inventory, itemJson ));
        if ( ! changes.containsKey(instanceHrid)) {
          Set<Change> t = new HashSet<>();
          t.add(c);
          changes.put(instanceHrid,t);
        }
        changes.get(instanceHrid).add(c);
      }

    } while (changedItems.size() == limit);

    return changes;
  }

  public static Map<String,Set<Change>> detectChangedLoans(
      Connection inventory, OkapiClient okapi, Timestamp since ) throws SQLException, IOException {

    Map<String,Set<Change>> changes = new HashMap<>();

    int limit = 5000;
    Timestamp modDateCursor = since;
    List<Map<String, Object>> changedLoans;

    do {
      changedLoans = okapi.queryAsList("/loan-storage/loans",
          "metadata.updatedDate>"+modDateCursor.toInstant().toString()+
          " sortBy metadata.updatedDate",limit);
      LOAN: for (Map<String,Object> loan : changedLoans) {

        String id = (String)loan.get("id");
        String itemId = (String)loan.get("itemId");
        String loanJson = mapper.writeValueAsString(loan);
        Map<String,String> metadata = (Map<String,String>) loan.get("metadata");
        Timestamp modDate = Timestamp.from(Instant.parse(
            metadata.get("updatedDate").replace("+00:00","Z")));
        modDateCursor = modDate;

        if ( getPreviousLoan == null )
          getPreviousLoan = inventory.prepareStatement("SELECT content FROM loanFolio WHERE id = ?");
        getPreviousLoan.setString(1, id);
        try ( ResultSet rs = getPreviousLoan.executeQuery() ) {
          while (rs.next()) if (rs.getString("content").equals(loanJson)) continue LOAN;
        }

        if (getLoanParentage == null)
          getLoanParentage = inventory.prepareStatement(
              "SELECT instanceHrid, itemFolio.hrid FROM holdingFolio, itemFolio"+
              " WHERE itemFolio.id = ? AND itemFolio.holdingHrid = holdingFolio.hrid");
        getLoanParentage.setString(1, itemId);
        String instanceHrid = null;
        String itemHrid = null;
        try (ResultSet rs = getLoanParentage.executeQuery() ) {
          while (rs.next()) {
            instanceHrid = rs.getString(1);
            itemHrid = rs.getString(2);
          }
        }
        if ( instanceHrid == null ) {
          System.out.printf("Loan %s (item %s) can't be tracked to instance,"
              + " not queueing for index.\n", id, itemHrid);
          continue;
        }

        if (replaceLoan == null)
          replaceLoan = inventory.prepareStatement(
              "REPLACE INTO loanFolio (id, holdingId, itemHrid, moddate, content) VALUES (?,?,?,?,?)");
        replaceLoan.setString(1, id);
        replaceLoan.setString(2, itemId);
        replaceLoan.setString(3, itemHrid);
        replaceLoan.setTimestamp(4, modDate);
        replaceLoan.setString(5, loanJson);
        replaceLoan.executeUpdate();

        Change c = new Change(Change.Type.LOAN,id,"Item modified",
            modDate,null,trackUserChange( inventory, loanJson ));
        if ( ! changes.containsKey(instanceHrid)) {
          Set<Change> t = new HashSet<>();
          t.add(c);
          changes.put(instanceHrid,t);
        }
        changes.get(instanceHrid).add(c);
      }
    } while (changedLoans.size() == limit);

    return changes;
  }

  public static Map<String,Set<Change>> detectChangedOrderLines(
      Connection inventory, OkapiClient okapi, Timestamp since ) throws SQLException, IOException {

    Map<String,Set<Change>> changes = new HashMap<>();

    int limit = 5000;
    Timestamp modDateCursor = since;
    List<Map<String, Object>> changedPols;

    do {
      changedPols = okapi.queryAsList("/orders-storage/po-lines",
          "metadata.updatedDate>"+modDateCursor.toInstant().toString()+
          " sortBy metadata.updatedDate",limit);
      POL: for (Map<String,Object> pol : changedPols) {

        String id = (String)pol.get("id");
        Map<String,String> metadata = (Map<String,String>) pol.get("metadata");
        Timestamp modDate = Timestamp.from(Instant.parse(
            metadata.get("updatedDate").replace("+00:00","Z")));
        modDateCursor = modDate;
        String polJson = mapper.writeValueAsString(pol);

        if ( getPreviousOrderLine == null )
          getPreviousOrderLine = inventory.prepareStatement(
              "SELECT content FROM orderLineFolio WHERE id = ?");
        getPreviousOrderLine.setString(1, id);
        try ( ResultSet rs = getPreviousOrderLine.executeQuery() ) {
          while (rs.next()) if (rs.getString("content").equals(polJson)) continue POL;
        }

        String instanceId = (String)pol.get("instanceId");
        if (getInstanceHridByInstanceId == null)
          getInstanceHridByInstanceId = inventory.prepareStatement(
              "SELECT hrid FROM instanceFolio WHERE id = ?");
        getInstanceHridByInstanceId.setString(1,instanceId);
        String instanceHrid = null;
        try (ResultSet rs = getInstanceHridByInstanceId.executeQuery() ) {
          while (rs.next()) instanceHrid = rs.getString(1);
        }

        if (replaceOrderLine == null)
          replaceOrderLine = inventory.prepareStatement(
              "REPLACE INTO orderLineFolio (id, instanceId, instanceHrid, orderId, moddate, content)"+
              " VALUES (?,?,?,?,?,?)");
        replaceOrderLine.setString(1, id);
        replaceOrderLine.setString(2, (instanceId==null)?"":instanceId);
        replaceOrderLine.setString(3, instanceHrid);
        replaceOrderLine.setString(4, (String)pol.get("purchaseOrderId"));
        replaceOrderLine.setTimestamp(5, modDate);
        replaceOrderLine.setString(6, polJson);
        replaceOrderLine.executeUpdate();

        if ( instanceHrid == null ) {
          System.out.println(
              "Purchase order line "+id+" can't be tracked to instance, not queueing for index.");
          continue;
        }
        Change c = new Change(Change.Type.ORDER,id,"Order Line modified",
            modDate,null,trackUserChange( inventory, polJson ));
        if ( ! changes.containsKey(instanceHrid)) {
          Set<Change> t = new HashSet<>();
          t.add(c);
          changes.put(instanceHrid,t);
        }
        changes.get(instanceHrid).add(c);
      }

    } while (changedPols.size() == limit);

    return changes;

  }

  public static Map<String,Set<Change>> detectChangedOrders(
      Connection inventory, OkapiClient okapi, Timestamp since ) throws SQLException, IOException {

    Map<String,Set<Change>> changes = new HashMap<>();

    int limit = 5000;
    Timestamp modDateCursor = since;
    List<Map<String, Object>> changedOrders;

    do {
      changedOrders = okapi.queryAsList("/orders-storage/purchase-orders",
          "metadata.updatedDate>"+modDateCursor.toInstant().toString()+
          " sortBy metadata.updatedDate",limit);
      ORDER: for (Map<String,Object> order : changedOrders) {

        String id = (String)order.get("id");
        Map<String,String> metadata = (Map<String,String>) order.get("metadata");
        Timestamp modDate = Timestamp.from(Instant.parse(
            metadata.get("updatedDate").replace("+00:00","Z")));
        modDateCursor = modDate;
        String orderJson = mapper.writeValueAsString(order);

        if ( getPreviousOrder == null )
          getPreviousOrder = inventory.prepareStatement("SELECT content FROM orderFolio WHERE id = ?");
        getPreviousOrder.setString(1, id);
        try ( ResultSet rs = getPreviousOrder.executeQuery() ) {
          while (rs.next()) if (rs.getString("content").equals(orderJson)) continue ORDER;
        }

        if (replaceOrder == null)
          replaceOrder = inventory.prepareStatement(
              "REPLACE INTO orderFolio (id, moddate, content) VALUES (?,?,?)");
        replaceOrder.setString(1, id);
        replaceOrder.setTimestamp(2, modDate);
        replaceOrder.setString(3, orderJson);
        replaceOrder.executeUpdate();

        if (getOrderParentage == null)
          getOrderParentage = inventory.prepareStatement(
              "SELECT instanceHrid FROM orderLineFolio WHERE orderId = ?");
        getOrderParentage.setString(1, id);

        try (ResultSet rs = getOrderParentage.executeQuery() ) {
          while (rs.next()) {
            String instanceHrid = rs.getString(1);
            if ( instanceHrid == null || instanceHrid.isEmpty() ) continue;
            Change c = new Change(Change.Type.ORDER,id,"Order modified",
                modDate,null,trackUserChange( inventory, orderJson ));
            if ( ! changes.containsKey(instanceHrid)) {
              Set<Change> t = new HashSet<>();
              t.add(c);
              changes.put(instanceHrid,t);
            }
            changes.get(instanceHrid).add(c);
          }
        }
      }

    } while (changedOrders.size() == limit);

    return changes;
  }

  public static String trackUserChange( Connection inventory, String json ) throws SQLException {
    Matcher userM = modUserP.matcher(json);
    if ( userM.matches() ) {
      String userId = userM.group(1);
      if ( trackUpdatesByUser == null )
        trackUpdatesByUser = inventory.prepareStatement(
            "INSERT INTO userChanges (id) VALUES (?)");
      trackUpdatesByUser.setString(1, userId);
      trackUpdatesByUser.executeUpdate();
      return userId;
    }
    return null;
  }

  static Pattern modDateP = Pattern.compile("^.*\"updatedDate\" *: *\"([^\"]+)\".*$");
  static Pattern modUserP = Pattern.compile("^.*\"updatedByUserId\" *: *\"([^\"]+)\".*$");

  static PreparedStatement getPreviousInstance = null;
  static PreparedStatement getPreviousBib = null;
  static PreparedStatement getPreviousHolding = null;
  static PreparedStatement getPreviousItem = null;
  static PreparedStatement getPreviousLoan = null;
  static PreparedStatement getPreviousOrder = null;
  static PreparedStatement getPreviousOrderLine = null;
  static PreparedStatement replaceInstance = null;
  static PreparedStatement replaceBib = null;
  static PreparedStatement replaceHolding = null;
  static PreparedStatement replaceItem = null;
  static PreparedStatement replaceLoan = null;
  static PreparedStatement replaceOrder = null;
  static PreparedStatement replaceOrderLine = null;
  static PreparedStatement getInstanceHridByInstanceId = null;
  static PreparedStatement getItemParentage = null;
  static PreparedStatement getLoanParentage = null;
  static PreparedStatement getOrderParentage = null;
  static PreparedStatement trackUpdatesByUser = null;

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

}
