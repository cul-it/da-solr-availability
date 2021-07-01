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
        if (replaceItem == null)
          replaceItem = inventory.prepareStatement(
              "REPLACE INTO instanceFolio (id, hrid, active, moddate, content) "+
              " VALUES (?,?,?,?,?)");
        replaceItem.setString(1, id);
        replaceItem.setString(2, hrid);
        replaceItem.setBoolean(3, active);
        replaceItem.setTimestamp(4, modDate);
        replaceItem.setString(5, instanceJson);
        replaceItem.executeUpdate();

        Change c = new Change(Change.Type.ITEM,id,"Item modified",modDate,null);
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

        if ( getPreviousInstance == null )
          getPreviousInstance = inventory.prepareStatement("SELECT content FROM itemFolio WHERE hrid = ?");
        getPreviousInstance.setString(1, hrid);
        boolean changed = true;
        try ( ResultSet rs = getPreviousInstance.executeQuery() ) {
          while (rs.next()) if (rs.getString("content").equals(itemJson)) changed = false;
        }
        if ( ! changed ) continue;

        if (getParentage == null)
          getParentage = inventory.prepareStatement(
              "SELECT instanceHrid, hrid FROM holdingFolio WHERE id = ?");
        getParentage.setString(1, (String)item.get("holdingsRecordId"));
        String instanceHrid = null;
        String holdingHrid = null;
        try (ResultSet rs = getParentage.executeQuery() ) {
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
  static PreparedStatement replaceItem = null;
  static PreparedStatement getParentage = null;

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

}
