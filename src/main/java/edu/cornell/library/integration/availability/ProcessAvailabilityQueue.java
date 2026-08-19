package edu.cornell.library.integration.availability;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.naming.AuthenticationException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.availability.MultivolumeAnalysis.MultiVolFlag;
import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.folio.BoundWith;
import edu.cornell.library.integration.folio.FolioClient;
import edu.cornell.library.integration.folio.Holding;
import edu.cornell.library.integration.folio.Holdings;
import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.ItemReference;
import edu.cornell.library.integration.folio.Items;
import edu.cornell.library.integration.folio.Items.Item;
import edu.cornell.library.integration.folio.Items.ItemList;
import edu.cornell.library.integration.folio.LoanTypes;
import edu.cornell.library.integration.folio.Locations;
import edu.cornell.library.integration.folio.OpenOrder;
import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.folio.ServicePoints;
import edu.cornell.library.integration.solr.SolrQueries;
import edu.cornell.library.integration.solr.SolrUtils;


public class ProcessAvailabilityQueue {

  public static void main(String[] args)
      throws IOException, SQLException, InterruptedException, SolrServerException, AuthenticationException {

    Map<String, String> env = System.getenv();
    String configFile = env.get("configFile");
    if (configFile == null)
      throw new IllegalArgumentException("configFile must be set in environment to valid file path.");
    Properties prop = new Properties();
    File f = new File(configFile);
    if (f.exists()) {
      try ( InputStream is = new FileInputStream(f) ) { prop.load( is ); }
    } else System.out.println("File does not exist: "+configFile);

    try (Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("databaseURLCurrent"),prop.getProperty("databaseUserCurrent"),
            prop.getProperty("databasePassCurrent"));
        Connection classificationDB = DriverManager.getConnection(
            prop.getProperty("databaseURLCallNos"),prop.getProperty("databaseUserCallNos"),
            prop.getProperty("databasePassCallNos"));
        PreparedStatement readQStmt = inventoryDB.prepareStatement
            ("SELECT availQueue.hrid, priority"+
             "  FROM availQueue"+
             "  LEFT JOIN bibLock ON availQueue.hrid = bibLock.hrid"+
             " WHERE bibLock.date IS NULL"+
             " ORDER BY priority, record_date LIMIT 1");
        PreparedStatement deqStmt = inventoryDB.prepareStatement
            ("DELETE FROM availQueue WHERE hrid = ?");
        PreparedStatement allForBib = inventoryDB.prepareStatement
            ("SELECT id, cause, record_date FROM availQueue WHERE hrid = ?");
        PreparedStatement createLockStmt = inventoryDB.prepareStatement
            ("INSERT INTO bibLock (hrid) values (?)",Statement.RETURN_GENERATED_KEYS);
        PreparedStatement unlockStmt = inventoryDB.prepareStatement
            ("DELETE FROM bibLock WHERE id = ?");
        PreparedStatement oldLocksCleanupStmt = inventoryDB.prepareStatement
            ("DELETE FROM bibLock WHERE date < DATE_SUB( NOW(), INTERVAL 15 MINUTE)");
        PreparedStatement clearFromQueueStmt = inventoryDB.prepareStatement
            ("DELETE FROM availQueue WHERE id = ?");
        PreparedStatement queueGen = inventoryDB.prepareStatement
            ("INSERT INTO generationQueue ( hrid, priority, cause, record_date )"
                + " VALUES (?,?,?,NOW())");
        Http2SolrClient solr = SolrUtils.getSolrClient(prop, "blacklightSolrCore");
        Http2SolrClient callNumberSolr = SolrUtils.getSolrClient(prop, "callnumSolrCore");
        ) {

      FolioClient folio = new FolioClient(prop,"Folio");
      Locations locations = new Locations(folio);
      ReferenceData holdingsNoteTypes = new ReferenceData(folio, "/holdings-note-types","name");
      ReferenceData callNumberTypes = new ReferenceData(folio, "/call-number-types","name");
      ReferenceData statCodes = new ReferenceData(folio,"/statistical-codes","code");
      LoanTypes.initialize(folio);
      ServicePoints.initialize(folio);
      Items.initialize(folio, locations);

      String solrDocumentCacheDirectory = prop.getProperty("solrDocumentCacheDirectory");

      for (int i = 0; i < 500_000; i++){
        BibToUpdate bib = null;
        Set<Integer> ids = new HashSet<>();
        Integer priority = null;
        List<Integer> lockIds = new ArrayList<>();
        try (  ResultSet rs = readQStmt.executeQuery() ) {

          BIB: while ( rs.next() ) {

            // batch only within a single priority level
            if (priority == null)
              priority = rs.getInt("priority");
            else if ( priority < rs.getInt("priority"))
              break;
            String bibId = rs.getString("hrid");

            // Attempt to lock bib, and skip if fails
            createLockStmt.setString(1,bibId);
            try {
              createLockStmt.executeUpdate();
            } catch (SQLException e) {
              System.out.println("Tried to lock instance "+bibId);
              continue BIB;
            }

            // Get all queue items for selected bib
            allForBib.setString(1, bibId);
            try ( ResultSet rs2 = allForBib.executeQuery() ) {
              Set<Change> changes = new HashSet<>();
              while (rs2.next()) {
                changes.add(new Change(Change.Type.RECORD,null,rs2.getString("cause"),
                                       rs2.getTimestamp("record_date"),null));
                int id = rs2.getInt("id");
                ids.add(id);
              }
              bib = new BibToUpdate(bibId,changes);
            }
            try ( ResultSet generatedKeys = createLockStmt.getGeneratedKeys() ) {
              if (generatedKeys.next()) lockIds.add( generatedKeys.getInt(1) );
            }
          }
        }

        if ( bib == null ) {
          oldLocksCleanupStmt.executeUpdate();
          queueRecordsNotRecentlyUpdated(inventoryDB,solr);
        } else {
          UpdateResults updateSuccess = updateBibInSolr(
              folio,inventoryDB,classificationDB,solr,callNumberSolr,solrDocumentCacheDirectory,locations,
              holdingsNoteTypes, callNumberTypes, statCodes, bib, priority);
          if ( ! updateSuccess.equals(UpdateResults.FAILURE) ) {
            for (int id : ids) {
              clearFromQueueStmt.setInt(1, id);
              clearFromQueueStmt.addBatch();
            }
            clearFromQueueStmt.executeBatch();
            for (int lockId : lockIds) {
              unlockStmt.setInt(1, lockId);
              unlockStmt.addBatch();
            }
            unlockStmt.executeBatch();
            if ( updateSuccess.equals(UpdateResults.NOBIBDATA) ) {
              System.out.println(bib.bibId +" lacks processed bib data. Redirecting to gen queue.");
              queueGen.setString(1, bib.bibId);
              queueGen.setInt(2,priority);
              Set<String> changes = new HashSet<>();
              for (Change c : bib.changes)  changes.add(c.toString());
              queueGen.setString(3, "Redirected from availability <"+String.join("; ",changes)+">");
              queueGen.executeUpdate();
            }
          }
        }
      }
    }
  }


  private static void queueRecordsNotRecentlyUpdated(Connection inventory, SolrClient solr)
      throws SQLException, SolrServerException, IOException {

    // Confirm that queue is actually empty
    try ( Statement stmt = inventory.createStatement();
        ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM availQueue")) {
      while (rs.next()) if ( rs.getInt(1) > 10 ) return;
    }

    // Get least recently visited records
    try (Statement stmt = inventory.createStatement();
        ResultSet rs = stmt.executeQuery(
            "SELECT hrid,visit_date FROM availQueueDates ORDER BY visit_date ASC limit 1000");
        PreparedStatement insert = inventory.prepareStatement(
            "INSERT INTO availQueue(hrid, priority, cause, record_date) VALUES (?,9,'Age of Record',?)");
        ) {
      while (rs.next()) {
        insert.setString(1, rs.getString("hrid"));
        insert.setTimestamp(2, rs.getTimestamp("visit_date"));
        insert.addBatch();
      }
      insert.executeBatch();
    }
    return;
  }

  final static String solrFieldsDataQuery = "SELECT * FROM processedMarcData WHERE hrid = ?";
  static UpdateResults updateBibInSolr(
      FolioClient folio, Connection inventory, Connection classificationDB,
      SolrClient solr, SolrClient callNumberSolr, String solrDocumentCacheDirectory, Locations locations,
      ReferenceData holdingsNoteTypes, ReferenceData callNumberTypes, ReferenceData statCodes,
      BibToUpdate changedBib, Integer priority)
      throws SQLException, IOException, InterruptedException, AuthenticationException {

    Set<SolrInputDocument> callnumSolrDocs = new HashSet<>();
    String bibId = changedBib.bibId;

    // Retrieve Bibliographic Solr fields and build Solr document to add availability info into
    SolrInputDocument doc = null;
    try (PreparedStatement pstmt = inventory.prepareStatement(solrFieldsDataQuery)){
      pstmt.setString(1, bibId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if ( ! rs.next() ) {
          System.out.println( "Bibliographic Solr fields for record "+bibId+" not found.");
          return UpdateResults.NOBIBDATA;
        }
        doc = constructSolrInputDocument( rs, bibId );
      }
    }

    // Retrieve instance id for bib
    String instanceId = null;
    try (PreparedStatement instanceByHrid = inventory.prepareStatement(
        "SELECT * FROM instanceFolio WHERE hrid = ?");) {
        instanceByHrid.setString(1, bibId);
        try ( ResultSet rs = instanceByHrid.executeQuery() ) {
          if ( ! rs.next() ) {
            System.out.printf("Instance not found for hrid %s\n",bibId);
            return UpdateResults.FAILURE;
          }
          doc.addField("instance_id", rs.getString("id"));
        }
    }

    // Retrieve all the holdings and items for the bib
    HoldingSet holdings = Holdings.retrieveHoldingsByInstanceHrid(
        inventory,locations,holdingsNoteTypes,callNumberTypes, String.valueOf(bibId));
    ItemList items = Items.retrieveItemsForHoldings(folio, inventory, bibId, holdings);

    // Modify Solr document with holding, item, and availability data
    doc.addField("statcode_facet", holdings.getStatCodes(statCodes));
    doc.addField("statcode_facet", items.getStatCodes(statCodes));
    doc.addField("item_count_i", items.itemCount());
    if ( Items.applyDummyRMCItems(holdings,items) )
      doc.addField("availability_facet","RMC Dummy Item");
    boolean active = doc.getFieldValue("type").equals("Catalog");
    if (active && holdings.fullyRemoteCampus()) {
      active = false;
      doc.removeField("type");
      doc.addField("type", "Hidden not suppressed");
    }
    doc.addField("notes_t", holdings.getNotes());

    boolean masterBoundWith =
        BoundWith.storeRecordLinksInInventory(inventory,String.valueOf(bibId),holdings);
    if (masterBoundWith) {
      doc.addField("bound_with_master_b", true);
      Set<String> changedItems = extractChangedItemIds( changedBib.changes );
      BoundWith.identifyAndQueueOtherBibsInMasterVolume(
          inventory, String.valueOf(bibId), changedItems );
    }
    EnumSet<BoundWith.Flag> f = BoundWith.dedupeBoundWithReferences(holdings,items);
    for ( BoundWith.Flag flag : f )
      doc.addField("availability_facet",flag.getAvailabilityFlag());
    if ( ! f.isEmpty() )
      doc.addField("bound_with_b", true);

    doc.removeField("barcode_t");
    doc.addField("barcode_t", items.getBarcodes());
    doc.removeField("barcode_addl_t");
    doc.addField("barcode_addl_t", holdings.getBoundWithBarcodes());


    Instant returnedUntil = holdings.summarizeItemAvailability(items);
    if ( returnedUntil != null ) {
      doc.addField("availability_facet", "Returned");
      doc.addField("returned_until_dt",
          ZonedDateTime.ofInstant(returnedUntil, ZoneId.of("UTC"))
          .format(DateTimeFormatter.ISO_INSTANT));
    }
    if ( OpenOrder.applyOpenOrders(inventory,holdings,bibId) )
      doc.addField("availability_facet", "On Order");
    if ( holdings.noItemsAvailability() )
      doc.addField("availability_facet", "No Items Print");
    if ( holdings.hasRecent() )
      doc.addField("availability_facet", "Recent Issues");
    if ( doc.containsKey("url_access_json") )
      Holdings.mergeAccessLinksIntoHoldings( holdings,doc.getFieldValues("url_access_json"));
    if ( holdings.size() > 0 )
      doc.addField("holdings_json", holdings.toJson());
    else doc.addField("availability_facet", "No Unsuppressed Holdings");

    for ( TreeSet<Item> itemsForHolding : items.getItems().values() )
      for ( Item i : itemsForHolding )
        if (i.status != null && i.loanType.shortLoan == true)
          doc.addField("availability_facet", "Short Loan");
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(holdings);
    doc.addField("availability_json", b.toJson());
    if ( b.availAt != null && b.unavailAt != null )
      doc.addField("availability_facet", "Avail and Unavail");
    Set<String> locationFacet = holdings.getLocationFacetValues();
    if (doc.containsKey("location"))
      doc.removeField("location");
    if (locationFacet == null)
      System.out.println("b"+bibId+" location facets are null.");
    else if (! locationFacet.isEmpty())
      doc.addField("location", locationFacet);

    for ( Holding h : holdings.values()) {
      if (h.donors != null)
        for (String donor : h.donors) {
          doc.addField("donor_display", donor);
          doc.addField("donor_t", donor);
        }
      if (h.call != null && h.call.matches(".*In Process.*"))
        doc.addField("availability_facet","In Process");
      if (h.itemSummary != null && h.itemSummary.unavail != null)
        for (ItemReference ir : h.itemSummary.unavail)
          if (ir.status != null && ir.status.status != null)
            doc.addField("availability_facet",ir.status.status);
      if (h.callNumberSuffix != null)
        doc.addField("lc_callnum_suffix", h.callNumberSuffix);

      if (h.itemSummary != null &&
          h.itemSummary.tempLocs != null &&
          ! h.itemSummary.tempLocs.isEmpty())
        doc.addField("availability_facet","Partial Temp Locs");
      if (h.copy != null)
        doc.addField("availability_facet", "Copies");
      if (! h.active )
        doc.addField("availability_facet", "Suppressed Holdings");
    }
    Set<String> changes = new HashSet<>();
    for (Change c : changedBib.changes)  changes.add(c.toString());
    EnumSet<MultiVolFlag> multiVolFlags = MultivolumeAnalysis.analyze(
        (String)doc.getFieldValue("format_main_facet"),
        (doc.containsKey("description_display")
            ?join(doc.getFieldValues("description_display")):""),
        (doc.containsKey("f300e_b")), holdings, items);
    if ( items.itemCount() > 0 )
      doc.addField("items_json", items.toJson());
    boolean oldMultiVolFlag = Boolean.valueOf((String)doc.getFieldValue("multivol_b"));
    if ( oldMultiVolFlag != multiVolFlags.contains(MultiVolFlag.MULTIVOL)) {
      System.out.println("Multivol logic conclusion mismatch b"+bibId);
    }
    doc.removeField("multivol_b");
    for (MultiVolFlag flag : multiVolFlags) {
      String solrField = flag.getSolrField();
      if ( doc.containsKey(solrField) ) doc.remove(solrField);
      doc.addField(solrField, true);
    }

    WorksAndInventory.updateInventory( inventory, doc );

    List<SolrInputDocument> thisDocsCallNumberDocs =
        CallNumberBrowse.generateBrowseDocuments(classificationDB,doc, holdings);

    callnumSolrDocs.addAll( thisDocsCallNumberDocs );
    Set<String> allCallNumbers =
        CallNumberBrowse.collateCallNumberList(thisDocsCallNumberDocs);
    doc.addField("callnumber_display", allCallNumbers);
    
    for (String collection : CallNumberTools.getCollectionFlags(
        CallNumberBrowse.allCallNumbers(thisDocsCallNumberDocs)) )
      doc.addField("collection",collection);

    if (doc.containsKey("notes")) {
        doc.addField("notes_display", doc.getFieldValues("notes"));
        doc.removeField("notes");
    }
    if (doc.containsKey("lc_callnum_facet")) {
      List<String> modified = new ArrayList<>();
      for (Object value : doc.getFieldValues("lc_callnum_facet")) {
        modified.add(  ((String)value).replaceAll(":", " > "));
      }
      doc.removeField("lc_callnum_facet");
      doc.addField("lc_callnum_facet", modified);
    }


    System.out.println(bibId+" ("+doc.getFieldValue("title_display")+"): "+String.join("; ",
        changes)+" priority:"+priority);

    try {
      List<Boolean> updated = SolrQueries.updateInSolrBlacklightCallnum(
          solr, callNumberSolr, solrDocumentCacheDirectory, doc, callnumSolrDocs);
      updateAvailQueueDates(inventory, bibId, updated);
    } catch (SolrServerException | RemoteSolrException e) {
      System.out.printf("Error communicating with Solr server after processing.");
      e.printStackTrace();
      Thread.sleep(5000);
      return UpdateResults.FAILURE;
    }
    return UpdateResults.SUCCESS;
  }


  private static SolrInputDocument constructSolrInputDocument(ResultSet rs, String hrid)
      throws SQLException {

    List<String> fields = Arrays.asList(
    // field list maintained here, and in SQL query in updateBibsInSolr()
    "authortitle_solr_fields", "title130_solr_fields",    "subject_solr_fields",
    "pubinfo_solr_fields",     "format_solr_fields",      "factfiction_solr_fields",
    "language_solr_fields",    "isbn_solr_fields",        "series_solr_fields",
    "titlechange_solr_fields", "toc_solr_fields",         "instruments_solr_fields",
    "marc_solr_fields",        "simpleproc_solr_fields",  "findingaids_solr_fields",
    "citationref_solr_fields", "url_solr_fields",         "hathilinks_solr_fields",
    "newbooks_solr_fields",    "recordtype_solr_fields",  "recordboost_solr_fields",
    "callnumber_solr_fields",  "otherids_solr_fields" );
    SolrInputDocument doc = new SolrInputDocument();
    String dates = rs.getString("record_dates");
    if (dates != null)
      doc.addField("record_dates_display", dates);
    for (String field : fields) {
      String data = rs.getString(field);
      if ( data == null ) continue;
      String[] values = data.split("\n");
      for (String value : values) {
        if (value.isEmpty()) continue;
        if (value.startsWith("^")) {
          // solr no longer supports document level boosting. We could convert this
          // to a boost on the main title field, but there's doesn't seem to be demand
//          doc.setDocumentBoost(Float.valueOf(value.substring(1)));
          continue;
        }
        String[] parts = value.split(": ", 2);
        doc.addField(parts[0], parts[1]);
      }
    }
    return doc;
  }

  private static String join(Collection<Object> objects) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Object o : objects)
      if ( first ) { first = false; sb.append((String)o); }
      else sb.append(" ").append((String)o);
    return sb.toString();
  }

  private static Set<String> extractChangedItemIds(Set<Change> changes) {
    Set<String> items = new HashSet<>();
    for (Change c : changes)
      if (c.detail.contains("ITEM") || c.detail.contains("CIRC") || c.detail.contains("LOAN"))
        for (String part : c.detail.split("[\\s\"]+"))
          if (number.matcher(part).matches()) items.add(part);
    return items;
  }
  private static Pattern number = Pattern.compile("[0-9]+");


  private static void updateAvailQueueDates(Connection inventory, String bibId, List<Boolean> updated)
      throws SQLException {
    String query ;
    Boolean bib = updated.get(0);
    Boolean call = updated.get(1);
    if (bib) {
      if (call) query ="UPDATE availQueueDates SET visit_date = NOW(), bib_update_date = NOW(), call_update_date = NOW()"
                      +" WHERE hrid = ?";
      else      query ="UPDATE availQueueDates SET visit_date = NOW(), bib_update_date = NOW() WHERE hrid = ?";
    } else {
      if (call) query ="UPDATE availQueueDates SET visit_date = NOW(), call_update_date = NOW() WHERE hrid = ?";
      else      query ="UPDATE availQueueDates SET visit_date = NOW() WHERE hrid = ?";
    }
    try (PreparedStatement stmt = inventory.prepareStatement(query)) {
      stmt.setString(1, bibId);
      int upd = stmt.executeUpdate();
      if (upd == 0) 
        try (PreparedStatement insert = inventory.prepareStatement(
            "INSERT INTO availQueueDates (hrid, visit_date, bib_update_date, call_update_date) "+
            "VALUES (?, NOW(), NOW(), NOW())")) {
          insert.setString(1, bibId);
          insert.executeUpdate();
        }
    }
  }

  public static class BibToUpdate implements Comparable<BibToUpdate>{
    final String bibId;
    final Set<Change> changes;
    public BibToUpdate(String bibId, Set<Change> changes) {
      this.bibId = bibId;
      this.changes = changes;
    }
    @Override public int compareTo(BibToUpdate o) {
      if ( o == null ) return -1;
      return this.bibId.compareTo( o.bibId );
    }
    @Override public boolean equals(Object o) {
      if ( o == null || ! o.getClass().equals(this.getClass() )) return false;
      return Objects.equals(this.bibId,((BibToUpdate)o).bibId);
    }
    @Override public int hashCode() { return this.bibId.hashCode(); }
  }
  private static ObjectMapper mapper = new ObjectMapper();
  private static enum UpdateResults { SUCCESS, FAILURE, NOBIBDATA; }
}
