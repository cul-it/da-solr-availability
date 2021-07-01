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
        Object o = instance.get("discoverySuppress");
        boolean active =
            (o == null)?false:String.class.isInstance(o)?Boolean.valueOf((String)o):(boolean)o;
        if (replaceInstance == null)
          replaceInstance = inventory.prepareStatement(
              "REPLACE INTO instanceFolio (id, hrid, active, moddate, content) "+
              " VALUES (?,?,?,?,?)");
        replaceInstance.setString(1, id);
        replaceInstance.setString(2, hrid);
        replaceInstance.setBoolean(3, active);
        replaceInstance.setTimestamp(4, modDate);
        replaceInstance.setString(5, instanceJson);
        replaceInstance.executeUpdate();

        Change c = new Change(Change.Type.INSTANCE,id,"Instance modified",modDate,null);
        if ( ! changes.containsKey(hrid)) {
          Set<Change> t = new HashSet<>();
          t.add(c);
          changes.put(hrid,t);
        }
        changes.get(hrid).add(c);
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

        if (getHoldingParentage == null)
          getHoldingParentage = inventory.prepareStatement(
              "SELECT hrid FROM instanceFolio WHERE id = ?");
        getHoldingParentage.setString(1, (String)holding.get("instanceId"));
        String instanceHrid = null;
        try (ResultSet rs = getHoldingParentage.executeQuery() ) {
          while (rs.next()) instanceHrid = rs.getString(1);
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
              "REPLACE INTO holdingFolio (id, hrid, instanceHrid, active, moddate, content) "+
              " VALUES (?,?,?,?,?,?)");
        replaceHolding.setString(1, id);
        replaceHolding.setString(2, hrid);
        replaceHolding.setString(3, instanceHrid);
        replaceHolding.setBoolean(4, active);
        replaceHolding.setTimestamp(5, modDate);
        replaceHolding.setString(6, holdingJson);
        replaceHolding.executeUpdate();

        Change c = new Change(Change.Type.HOLDING,id,"Holding modified",modDate,null);
        if ( ! changes.containsKey(hrid)) {
          Set<Change> t = new HashSet<>();
          t.add(c);
          changes.put(hrid,t);
        }
        changes.get(hrid).add(c);
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
        try ( ResultSet rs = getPreviousInstance.executeQuery() ) {
          while (rs.next()) if (rs.getString("content").equals(itemJson)) changed = false;
        }
        if ( ! changed ) continue;

        if (getItemParentage == null)
          getItemParentage = inventory.prepareStatement(
              "SELECT instanceHrid, hrid FROM holdingFolio WHERE id = ?");
        getItemParentage.setString(1, (String)item.get("holdingsRecordId"));
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
        String barcode = (item.containsKey("barcode"))?(String)item.get("barcode"):null;
        if (replaceItem == null)
          replaceItem = inventory.prepareStatement(
              "REPLACE INTO itemFolio (id, hrid, holdingHrid, moddate, barcode, content) "+
              " VALUES (?,?,?,?,?,?)");
        replaceItem.setString(1, id);
        replaceItem.setString(2, hrid);
        replaceItem.setString(3, holdingHrid);
        replaceItem.setTimestamp(4, modDate);
        replaceItem.setString(5, barcode);
        replaceItem.setString(6, itemJson);
        replaceItem.executeUpdate();

        Change c = new Change(Change.Type.ITEM,id,"Item modified",modDate,null);
        if ( ! changes.containsKey(instanceHrid)) {
          Set<Change> t = new HashSet<>();
          t.add(c);
          changes.put(instanceHrid,t);
        }
        changes.get(instanceHrid).add(c);
      }
    } while (changedItems.size() == limit);
/*TODO detect batch updates?    if ( changes.size() > 4 )
      for ( Set<Change> bibChanges : changes.values() ) for ( Change c : bibChanges )
        c.type = Change.Type.ITEM_BATCH;*/
    return changes;
  }
  static PreparedStatement getPreviousInstance = null;
  static PreparedStatement getPreviousHolding = null;
  static PreparedStatement getPreviousItem = null;
  static PreparedStatement replaceInstance = null;
  static PreparedStatement replaceHolding = null;
  static PreparedStatement replaceItem = null;
  static PreparedStatement getItemParentage = null;
  static PreparedStatement getHoldingParentage = null;

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

}
