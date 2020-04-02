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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

public class ProcessBrowseQueue {

  public static void main(String[] args) throws IOException, SQLException, InterruptedException {

    Properties prop = new Properties();
    System.out.println(System.getenv("CONFIG_FILE"));
    File f = new File(System.getenv("CONFIG_FILE"));
    if (f.exists()) {
      try ( InputStream in = new FileInputStream(f) ) {
        prop.load( in );
      }
    } else {
      System.out.println("File at "+System.getenv("CONFIG_FILE")+" is missing or unreadable.");
      System.exit(1);
    }

    try (
        Connection inventory = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        Connection headings = DriverManager.getConnection(
            prop.getProperty("headingsDBUrl"),prop.getProperty("headingsDBUser"),prop.getProperty("headingsDBPass"));
        Statement stmt = inventory.createStatement();
        PreparedStatement nextInQueue = inventory.prepareStatement(
            "SELECT heading_id FROM browseQueue ORDER BY priority LIMIT 1");
        PreparedStatement queueItemsForHead = inventory.prepareStatement("SELECT id FROM browseQueue WHERE heading_id = ?");
        PreparedStatement deprioritize = inventory.prepareStatement("UPDATE browseQueue SET priority = 9 WHERE id = ?");
        PreparedStatement dequeue = inventory.prepareStatement("DELETE FROM browseQueue WHERE id = ?");
        ){

      queueItemsForHead.setFetchSize(10000);

      for (int i = 0; i < 1; i++){

        Set<Integer> ids = new HashSet<>();
        Integer headingId = null;

        stmt.execute("LOCK TABLES browseQueue WRITE");
        try (  ResultSet rs = nextInQueue.executeQuery() ) { while ( rs.next() ) { headingId = rs.getInt("heading_id"); } }

        if ( headingId == null ) { stmt.execute("UNLOCK TABLES"); Thread.sleep(3000); continue; }

        queueItemsForHead.setInt(1, headingId);
        try( ResultSet rs = queueItemsForHead.executeQuery() ) { while ( rs.next() ) ids.add( rs.getInt(1) ); }

        for ( int id : ids ) { deprioritize.setInt(1, id); deprioritize.addBatch(); }
        deprioritize.executeBatch();

        stmt.execute("UNLOCK TABLES");

        processHeading( headings, headingId );
      }
    }
  }

  private static PreparedStatement headingsCounts = null;
  private static void processHeading(Connection headings, Integer headingId) throws SQLException {

    if ( headingsCounts == null )
      headingsCounts = headings.prepareStatement("SELECT heading_category, count(*) FROM bib2heading WHERE heading_id = ?");

    Map<BrowseMode,Integer> categoryCounts = new HashMap<>();
    headingsCounts.setInt(1, headingId);
    try ( ResultSet rs = headingsCounts.executeQuery() ) {
      while ( rs.next() )
        categoryCounts.put(browseModes[rs.getInt(1)], rs.getInt(2));
    }
    Map<String,Map<String,Reference>> outgoingReferences = getOutgoingReferences( headings, headingId );
    Map<String,Map<String,Reference>> incomingReferences = getIncomingReferences( headings, headingId );
    for ( BrowseMode bm : BrowseMode.values() ) {
      
      Map<String,Map<String,Reference>> outgoingForBrowseMode = filterOutgoingReferences( outgoingReferences, bm );
      if ( categoryCounts.containsKey(bm) || ! outgoingForBrowseMode.isEmpty() )
        deleteHeadingFromSolr();
    }
  }


  private static Map<String, Map<String, Reference>> filterOutgoingReferences(
      Map<String, Map<String, Reference>> outgoingReferences, BrowseMode bm) {

    Map<String, Map<String, Reference>> filtered = new HashMap<>();
    for ( Entry<String, Map<String, Reference>> referenceType : outgoingReferences.entrySet() ) {
      Map<String, Reference> filteredForType = new HashMap<>();
      for ( Entry<String,Reference> ref : referenceType.getValue().entrySet() ) {
        Reference r = ref.getValue();
        if ( r.headingType.browses.contains(bm) && r.counts.get(bm.countType) > 0)
          filteredForType.put(ref.getKey(), r);
      }
      if (! filteredForType.isEmpty()) filtered.put(referenceType.getKey(), filteredForType);
    }
    return filtered;
  }


  private static void deleteHeadingFromSolr() {
    // TODO Auto-generated method stub
    
  }

  private static PreparedStatement outgoingReferences = null;
  private static Map<String,Map<String,Reference>> getOutgoingReferences(
      Connection headings, Integer headingId) throws SQLException {

    if ( outgoingReferences == null ) {
      outgoingReferences = headings.prepareStatement(
          "SELECT *, heading.id AS heading_id FROM reference, heading WHERE from_heading = ? AND to_heading = heading.id");
      outgoingReferences.setFetchSize(1000);
    }

    outgoingReferences.setInt(1, headingId);
    Map<String,Map<String,Reference>> references = new HashMap<>();
    try ( ResultSet rs = outgoingReferences.executeQuery() ) {
      while ( rs.next() ) {
        if ( rs.getInt("works") == 0 && rs.getInt("works_about") == 0 && rs.getInt("works_by") == 0)
          continue;
        String referenceType = rs.getString("ref_desc");
        if ( referenceType.isEmpty() )
          referenceType = ( rs.getInt("ref_type") == ReferenceType.SEE.ordinal() ) ? "See" : "See Also";
        if ( references.containsKey(referenceType) ) references.put(referenceType, new TreeMap<>());
        references.get(referenceType).put(rs.getString("sort"), new Reference(rs.getString("heading"),rs.getInt("heading_id"),
            headingTypes[rs.getInt("heading_type")],rs.getInt("works"),rs.getInt("works_by"),rs.getInt("works_about")));
      }
    }
    return references;
  }

  private static PreparedStatement incomingReferences = null;
  private static Map<String,Map<String,Reference>> getIncomingReferences(
      Connection headings, Integer headingId) throws SQLException {

    if ( incomingReferences == null ) {
      incomingReferences = headings.prepareStatement(
          "SELECT * FROM reference, heading WHERE to_heading = ? AND from_heading = heading.id");
      incomingReferences.setFetchSize(1000);
    }

    incomingReferences.setInt(1, headingId);
    Map<String,Map<String,Reference>> references = new HashMap<>();
    try ( ResultSet rs = incomingReferences.executeQuery() ) {
      while ( rs.next() ) {
        String referenceType = rs.getString("ref_desc");
        if ( referenceType.isEmpty() )
          referenceType = ( rs.getInt("ref_type") == ReferenceType.SEE.ordinal() ) ? "See" : "See Also";
        if ( references.containsKey(referenceType) ) references.put(referenceType, new TreeMap<>());
        references.get(referenceType).put(rs.getString("sort"), new Reference(rs.getString("Heading"),rs.getInt("heading_id")));
      }
    }
    return references;
  }

  public static class Reference {

    final String heading;
    final Integer headingId;
    final HeadingType headingType;
    final Map<String,Integer> counts;

    public Reference( String heading, int id) {
      this.heading = heading;
      this.headingId = id;
      this.headingType = null;
      this.counts = null;
    }

    public Reference( String heading, int id, HeadingType ht, int works, int works_by, int works_about ) {
      this.heading = heading;
      this.headingId = id;
      this.headingType = ht;
      Map<String,Integer> counts = new HashMap<>();
      counts.put("works", works);
      counts.put("works_by", works_by);
      counts.put("works_about", works_about);
      this.counts = Collections.unmodifiableMap(counts);
    }

  }

  public enum BrowseMode {
    AUTHOR("works_by"), SUBJECT("works_about"), AUTHORTITLE("works"), TITLE("works");
    final String countType;
    private BrowseMode( final String countType ) { this.countType = countType; }
  }
  private static BrowseMode[] browseModes = BrowseMode.values();

  public enum ReferenceType { ALTERNATEFORM,SEE,SEEALSO,SEEALSO2; }
  public enum HeadingType {
    PERS (BrowseMode.AUTHOR,BrowseMode.SUBJECT),
    CORP (BrowseMode.AUTHOR,BrowseMode.SUBJECT),
    EVENT(BrowseMode.AUTHOR,BrowseMode.SUBJECT),
    TITLE(BrowseMode.TITLE,BrowseMode.SUBJECT),
    SUBJECT(BrowseMode.SUBJECT),
    GEO(BrowseMode.SUBJECT),
    CHRON(BrowseMode.SUBJECT),
    GENRE(BrowseMode.SUBJECT),
    MEDIUM(BrowseMode.SUBJECT),
    AUTHORTITLE(BrowseMode.AUTHORTITLE,BrowseMode.SUBJECT),
    ;

    final Set<BrowseMode> browses;

    private HeadingType(final BrowseMode browse) {
      Set<BrowseMode> browses = new HashSet<>();
      browses.add(browse);
      this.browses = Collections.unmodifiableSet(browses);
    }

    private HeadingType(final BrowseMode browse, final BrowseMode browse2) {
      Set<BrowseMode> browses = new HashSet<>();
      browses.add(browse);
      browses.add(browse2);
      this.browses = Collections.unmodifiableSet(browses);
    }
  }
  private static HeadingType[] headingTypes = HeadingType.values();


}
