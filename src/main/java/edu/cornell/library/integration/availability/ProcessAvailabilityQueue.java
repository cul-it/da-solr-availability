package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.availability.MultivolumeAnalysis.MultiVolFlag;
import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.voyager.BoundWith;
import edu.cornell.library.integration.voyager.Holding;
import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.ItemReference;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.Items.Item;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class ProcessAvailabilityQueue {

  public static void main(String[] args) throws IOException, SQLException, XMLStreamException, InterruptedException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }

    try (Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
        Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        Statement stmt = inventoryDB.createStatement();
        PreparedStatement pstmt = inventoryDB.prepareStatement
            ("SELECT availabilityQueue.bib_id, priority"+
             "  FROM availabilityQueue, solrFieldsData"+
             " WHERE availabilityQueue.bib_id = solrFieldsData.bib_id"+
             " ORDER BY priority LIMIT 4");
        PreparedStatement deqStmt = inventoryDB.prepareStatement
            ("DELETE FROM availabilityQueue WHERE bib_id = ?");
        PreparedStatement bibActiveStmt = inventoryDB.prepareStatement
            ("SELECT active FROM bibRecsVoyager WHERE bib_id = ?");
        PreparedStatement allForBib = inventoryDB.prepareStatement
            ("SELECT id, cause, record_date FROM availabilityQueue WHERE bib_id = ?");
        PreparedStatement deprioritizeStmt = inventoryDB.prepareStatement
            ("UPDATE availabilityQueue SET priority = 9 WHERE id = ?");
        PreparedStatement clearFromQueueStmt = inventoryDB.prepareStatement
            ("DELETE FROM availabilityQueue WHERE id = ?");
        ConcurrentUpdateSolrClient solr = new ConcurrentUpdateSolrClient( System.getenv("SOLR_URL"),50,1);
        SolrClient callNumberSolr = new HttpSolrClient( System.getenv("CALLNUMBER_SOLR_URL") )
        ) {

      for (int i = 0; i < 40_000; i++){
        Set<BibToUpdate> bibs = new HashSet<>();
        Set<Integer> ids = new HashSet<>();
        stmt.execute("LOCK TABLES solrFieldsData READ, availabilityQueue WRITE, bibRecsVoyager READ");
        Integer priority = null;
        try (  ResultSet rs = pstmt.executeQuery() ) {
          while ( rs.next() ) {

            // batch only within a single priority level
            if (priority == null)
              priority = rs.getInt("priority");
            else if ( priority < rs.getInt("priority"))
              break;
            int bibId = rs.getInt("bib_id");

            // Confirm bib is active
            Boolean active = null;
            bibActiveStmt.setInt(1, bibId);
            try (ResultSet rs2 = bibActiveStmt.executeQuery())
            { while (rs2.next()) active = rs2.getBoolean(1); }
            if ( active == null ) {
              System.out.println("Bib in availability queue, not bibRecsVoyager: "+bibId);
              continue;
            }

            // Get all queue items for selected bib
            allForBib.setInt(1, bibId);
            try ( ResultSet rs2 = allForBib.executeQuery() ) {
              Set<Change> changes = new HashSet<>();
              while (rs2.next()) {
                changes.add(new Change(Change.Type.RECORD,null,rs2.getString("cause"),
                                       rs2.getTimestamp("record_date"),null));
                int id = rs2.getInt("id");
                deprioritizeStmt.setInt(1,id);
                deprioritizeStmt.addBatch();
                ids.add(id);
              }
              bibs.add(new BibToUpdate(bibId,changes,active ));
              deprioritizeStmt.executeBatch();
            }
          }
        }
        stmt.execute("UNLOCK TABLES");

        if ( bibs.isEmpty() )
          Thread.sleep(3000);
        else {
          updateBibsInSolr(voyager,inventoryDB,solr,callNumberSolr,bibs, priority);
          if (priority != null && priority <= 5)
            solr.blockUntilFinished();
        }
        for (int id : ids) {
          clearFromQueueStmt.setInt(1, id);
          clearFromQueueStmt.addBatch();
        }
        clearFromQueueStmt.executeBatch();
      }
      solr.blockUntilFinished();
    }
  }

  final static String solrFieldsDataQuery =
      "SELECT record_dates," + // field list maintained here, and in constructSolrInputDocument() below
      "       authortitle_solr_fields, title130_solr_fields,    subject_solr_fields,     pubinfo_solr_fields," + 
      "       format_solr_fields,      factfiction_solr_fields, language_solr_fields,    isbn_solr_fields," + 
      "       series_solr_fields,      titlechange_solr_fields, toc_solr_fields,         instruments_solr_fields," + 
      "       marc_solr_fields,        simpleproc_solr_fields,  findingaids_solr_fields, citationref_solr_fields," + 
      "       url_solr_fields,         hathilinks_solr_fields,  newbooks_solr_fields,    recordtype_solr_fields," + 
      "       recordboost_solr_fields, holdings_solr_fields,    otherids_solr_fields" + 
      "  FROM solrFieldsData"+
      " WHERE bib_id = ?";
  static void updateBibsInSolr(
      Connection voyager, Connection inventory,
      SolrClient solr, SolrClient callNumberSolr,
      Set<BibToUpdate> changedBibs, Integer priority)
      throws SQLException, IOException, XMLStreamException, InterruptedException {

    while (! changedBibs.isEmpty()) {
      List<BibToUpdate> completedBibUpdates = new ArrayList<>();

      try (PreparedStatement pstmt = inventory.prepareStatement(solrFieldsDataQuery)){

        Set<SolrInputDocument> solrDocs = new HashSet<>();
        Set<SolrInputDocument> callnumSolrDocs = new HashSet<>();

        for (BibToUpdate updateDetails : changedBibs) {
          int bibId = updateDetails.bibId;
          pstmt.setInt(1, bibId);
          try (ResultSet rs = pstmt.executeQuery()) {
            while ( rs.next() ) {

              SolrInputDocument doc = constructSolrInputDocument( rs );
              HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyager,bibId);
              holdings.getRecentIssues(voyager, inventory, bibId);
              ItemList items = Items.retrieveItemsForHoldings(voyager, inventory, bibId, holdings);
              if ( ! updateDetails.active ) {
                doc.removeField("type");
                doc.addField("type", "Suppressed Bib");
              }

              boolean masterBoundWith = BoundWith.storeRecordLinksInInventory(inventory,bibId,holdings);
              if (masterBoundWith) {
                doc.addField("bound_with_master_b", true);
                Set<Integer> changedItems = extractChangedItemIds( updateDetails.changes );
                BoundWith.identifyAndQueueOtherBibsInMasterVolume( inventory, bibId, changedItems );
              }
              EnumSet<BoundWith.Flag> f = BoundWith.dedupeBoundWithReferences(holdings,items);
              for (BoundWith.Flag flag : f)
                doc.addField("availability_facet",flag.getAvailabilityFlag());
              doc.removeField("barcode_t");
              doc.addField("barcode_t", items.getBarcodes());
              doc.removeField("barcode_addl_t");
              doc.addField("barcode_addl_t", holdings.getBoundWithBarcodes());

              if ( ! f.isEmpty() )
                doc.addField("bound_with_b", true);

              if ( holdings.summarizeItemAvailability(items) ) 
                doc.addField("availability_facet", "Returned");
              if ( holdings.applyOpenOrderInformation(voyager,bibId) )
                doc.addField("availability_facet", "On Order");
              if ( holdings.noItemsAvailability() )
                doc.addField("availability_facet", "No Items Print");
              if ( holdings.hasRecent() )
                doc.addField("availability_facet", "Recent Issues");
              if ( doc.containsKey("url_access_json") )
                Holdings.mergeAccessLinksIntoHoldings( holdings,doc.getFieldValues("url_access_json"));
              if ( holdings.size() > 0 )
                doc.addField("holdings_json", holdings.toJson());
              for ( TreeSet<Item> itemsForHolding : items.getItems().values() )
                for ( Item i : itemsForHolding )
                  if (i.status != null && i.status.shortLoan != null)
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

              for ( Integer mfhdId : holdings.getMfhdIds()) {
                Holding h = holdings.get(mfhdId);
                if (h.donors != null)
                  for (String donor : h.donors)
                    doc.addField("donor_display", donor);
                if (h.call != null && h.call.matches(".*In Process.*"))
                  doc.addField("availability_facet","In Process");
                if (h.itemSummary != null && h.itemSummary.unavail != null)
                  for (ItemReference ir : h.itemSummary.unavail)
                    if (ir.status != null && ir.status.code != null) {
                      String status = ir.status.code.values().iterator().next();
                      doc.addField("availability_facet",status);
                    }
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
              for (Change c : updateDetails.changes)  changes.add(c.toString());
              EnumSet<MultiVolFlag> multiVolFlags = MultivolumeAnalysis.analyze(
                  (String)doc.getFieldValue("format_main_facet"),
                  (doc.containsKey("description_display")?join(doc.getFieldValues("description_display")):""),
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

              // Temporary workaround can be removed once all Solr fields in db have SimpleProc v1.2
              if ( doc.containsKey("f300e_b")) {
                doc.removeField("f300e_b");
                doc.addField("f300e_b", true);
              }

              WorksAndInventory.updateInventory( inventory, doc );

              List<SolrInputDocument> thisDocsCallNumberDocs = CallNumberBrowse.generateBrowseDocuments(doc,holdings);
              callnumSolrDocs.addAll( thisDocsCallNumberDocs );
              doc.addField("callnumber_display", CallNumberBrowse.collateCallNumberList(thisDocsCallNumberDocs));

              solrDocs.add(doc);
              callNumberSolr.deleteByQuery("bibid:"+bibId);

              System.out.println(bibId+" ("+doc.getFieldValue("title_display")+"): "+String.join("; ",
                  changes)+" priority:"+priority);
//              System.out.println(ClientUtils.toXML(doc).replaceAll("(<field)", "\n$1"));
            }
            solr.add(solrDocs);
            if ( ! callnumSolrDocs.isEmpty() && updateDetails.active )
              callNumberSolr.add(callnumSolrDocs);
          }
          completedBibUpdates.add(updateDetails);
        }
      } catch (SolrServerException | RemoteSolrException e) {
        System.out.printf("Error communicating with Solr server after processing %d of %d bib update batch.",
            completedBibUpdates.size(),changedBibs.size());
        e.printStackTrace();
        Thread.sleep(5000);
      } finally {
        for (BibToUpdate updateDetails : completedBibUpdates)
          changedBibs.remove(updateDetails);
      }
    }
  }

  private static SolrInputDocument constructSolrInputDocument(ResultSet rs) throws SQLException {

    List<String> fields = Arrays.asList( // field list maintained here, and in SQL query in updateBibsInSolr()
    "authortitle_solr_fields", "title130_solr_fields",    "subject_solr_fields",     "pubinfo_solr_fields",
    "format_solr_fields",      "factfiction_solr_fields", "language_solr_fields",    "isbn_solr_fields",
    "series_solr_fields",      "titlechange_solr_fields", "toc_solr_fields",         "instruments_solr_fields",
    "marc_solr_fields",        "simpleproc_solr_fields",  "findingaids_solr_fields", "citationref_solr_fields",
    "url_solr_fields",         "hathilinks_solr_fields",  "newbooks_solr_fields",    "recordtype_solr_fields",
    "recordboost_solr_fields", "holdings_solr_fields",    "otherids_solr_fields" );
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
          doc.setDocumentBoost(Float.valueOf(value.substring(1)));
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

  private static Set<Integer> extractChangedItemIds(Set<Change> changes) {
    Set<Integer> items = new HashSet<>();
    for (Change c : changes)
      if (c.detail.contains("ITEM") || c.detail.contains("CIRC"))
        for (String part : c.detail.split("[\\s\"]+"))
          if (number.matcher(part).matches())
            items.add(Integer.valueOf(part));
    return items;
  }
  private static Pattern number = Pattern.compile("[0-9]+");

  public static class BibToUpdate implements Comparable<BibToUpdate>{
    final int bibId;
    final Set<Change> changes;
    final boolean active;
    public BibToUpdate(int bibId, Set<Change> changes, boolean active) {
      this.bibId = bibId;
      this.changes = changes;
      this.active = active;
    }
    @Override public int compareTo(BibToUpdate o) {
      if ( o == null ) return -1;
      return Integer.compare(this.bibId,o.bibId);
    }
    @Override public boolean equals(Object o) {
      if ( o == null || ! o.getClass().equals(this.getClass() )) return false;
      return Objects.equals(this.bibId,((BibToUpdate)o).bibId);
    }
    @Override public int hashCode() { return Integer.hashCode(this.bibId); }
  }
}
