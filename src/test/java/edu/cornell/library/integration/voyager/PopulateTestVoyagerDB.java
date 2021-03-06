package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is a support utility for caching Voyager data into a static SQLite database for testing. Because
 * it's only intended to be invoked while adding to or updating the suite of JUnit tests, it isn't
 * polished to the point of accepting outside config or arguments. Instead, variables in the code need
 * to be tweaked before executing.<br/><br/>
 * 
 * When creating new tests, they should be run against a live Voyager Oracle instance until debugged. Then,
 * related records can be added to an existing voyagerTest database by listing the bibs in the testBibs
 * array below and running with replaceDBContents = false. Alternatively, and likely a better idea for
 * non-Cornell records that might conflict with the Cornell location table etc., would be to change
 * the connection string to a new database file name and run the script with replaceDBContents = true.
 * Then the new test db can be used for new tests without the risk of breaking tests using the old test
 * database.<br/><br/>
 * 
 * NOTE: This will create a sqlite DB file out of the SQL file provided in testDbSQLFile. The DB file will
 * have the same name but with the .db extension instead of .sql. Because I wasn't able to export the
 * database back to sql in the application, this needs to be done manually, and <i>must</i> be done before
 * running this program to insert any more bibs' records, and the database file created must be removed.
 * To do this on the command line, these commands executed in the <code>src/test/resources</code> directory
 * will do the job: <br/>
 * <pre>
 * $ sqlite3 voyagerTest.db
 * SQLite version 3.x.x yyyy-mm-dd hh:mm:ss
 * Enter ".help" for usage hints.
 * sqlite> .output temp.sql
 * sqlite> .dump
 * sqlite> .quit
 * $ mv temp.sql voyagerTest.sql
 * $ rm voyagerTest.db</pre>
 */
public class PopulateTestVoyagerDB {

  public static boolean replaceDBContents = false; // if false, will add specified bibs to existing tables
  public static String testDbSQLFile = "src/test/resources/voyagerTest.sql";
  private static List<Integer> testBibs = Arrays.asList(1157555, 1197440, 9262507);

  // Bibs in "jdbc:sqlite:src/test/resources/voyagerTest.db"
  // 330581,3212531,2248567,576519,3827392,1016847,969430,1799377,2095674,1575369,9520154,927983,
  // 342724,4442869,784908,6047653,9628566,3956404,9647384,306998,329763,2026746,4546769,10023626
  // 867,9295667,1282748,4888514,369282,833840,836782,9386182,10604045,10797795,10797341,10797688
  // 10705932,10757225,10825496,10663989,10005850,10602676,4345125,2813334,7187316,11062722,1936680
  // 1449673,11764,34985,5487364,301608, 11438152, 7259947,106808, 1681672,1157555,1197440,9262507

  private static Set<Integer> testMfhds = new HashSet<>();
  private static Set<Integer> testItems = new HashSet<>();
  private static Set<Integer> testLineItems = new HashSet<>();
  private static Set<Integer> subscriptions = new HashSet<>();
  private static Set<Integer> components = new HashSet<>();
  private static Set<Integer> purchaseOrders = new HashSet<>();


  public static void main(String[] args) throws SQLException, IOException {

    try (
        VoyagerDBConnection testDB = new VoyagerDBConnection(testDbSQLFile,true);
        Statement testDbstmt = testDB.connection.createStatement();
        Connection voyager = VoyagerDBConnection.getLiveConnection("database.properties")
        ) {
      Connection testDb = testDB.connection;

      bib_master(testDb,voyager,testDbstmt);
      bib_mfhd(testDb,voyager,testDbstmt);
      bib_text(testDb,voyager,testDbstmt);
      bib_data(testDb,voyager,testDbstmt);
      mfhd_data(testDb,voyager,testDbstmt);
      mfhd_item(testDb,voyager,testDbstmt);
      mfhd_master(testDb,voyager,testDbstmt);
      item(testDb,voyager,testDbstmt);
      item_barcode(testDb,voyager,testDbstmt);
      item_status_type(testDb,voyager,testDbstmt);
      item_type(testDb,voyager,testDbstmt);
      item_status(testDb,voyager,testDbstmt);
      circ_transactions(testDb,voyager,testDbstmt);
      location(testDb,voyager,testDbstmt);
      circ_policy_group(testDb,voyager,testDbstmt);
      circ_policy_locs(testDb,voyager,testDbstmt);
      line_item(testDb,voyager,testDbstmt);
      line_item_copy_status(testDb,voyager,testDbstmt);
      subscriptionAndIssues(testDb,voyager,testDbstmt);
      component(testDb,voyager,testDbstmt);
      issues_received(testDb,voyager,testDbstmt);
      serial_issues(testDb,voyager,testDbstmt);
      purchase_order(testDb,voyager,testDbstmt);

    }
  }

  private static void bib_master(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists bib_master");
      testDbstmt.executeUpdate("create table bib_master ( bib_id int, suppress_in_opac string )");
    }
    System.out.println("Loading bib_master data for "+testBibs.size()+" bibs.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT suppress_in_opac FROM bib_master WHERE bib_id = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO bib_master (bib_id, suppress_in_opac) VALUES (?,?)")){
      for (int bib : testBibs) {
        readStmt.setInt(1, bib);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, bib);
            writeStmt.setString(2, new String(rs.getBytes(1),StandardCharsets.UTF_8));
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void bib_mfhd(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists BIB_MFHD");
      testDbstmt.executeUpdate("create table BIB_MFHD ( BIB_ID int, MFHD_ID int )");
    }
    System.out.println("Loading bib_mfhd data for "+testBibs.size()+" bibs.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT MFHD_ID FROM BIB_MFHD WHERE BIB_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO BIB_MFHD (BIB_ID, MFHD_ID) VALUES (?,?)")){
      for (int bib : testBibs) {
        readStmt.setInt(1, bib);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, bib);
            writeStmt.setInt(2, rs.getInt(1));
            writeStmt.addBatch();
            testMfhds.add(rs.getInt(1));
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void bib_text(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists BIB_TEXT");
      testDbstmt.executeUpdate("create table BIB_TEXT ( BIB_ID int, TITLE_BRIEF text )");
    }
    System.out.println("Loading bib_text data for "+testBibs.size()+" bibs.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT TITLE_BRIEF FROM BIB_TEXT WHERE BIB_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO BIB_TEXT ( BIB_ID, TITLE_BRIEF ) VALUES (?,?)")){
      for (int bib : testBibs) {
        readStmt.setInt(1, bib);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, bib);
            writeStmt.setString(2, rs.getString(1));
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void bib_data(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists BIB_DATA");
      testDbstmt.executeUpdate("create table BIB_DATA ( RECORD_SEGMENT blob, BIB_ID int, SEQNUM int )");
    }
    System.out.println("Loading bib_data data for "+testBibs.size()+" bibs.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT RECORD_SEGMENT, SEQNUM FROM BIB_DATA WHERE BIB_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO BIB_DATA (RECORD_SEGMENT, SEQNUM, BIB_ID) VALUES (?,?,?)")){
      for (int bib : testBibs) {
        readStmt.setInt(1, bib);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setString(1, new String(rs.getBytes(1),StandardCharsets.UTF_8));
            writeStmt.setInt(2, rs.getInt(2));
            writeStmt.setInt(3, bib);
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void mfhd_data(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists MFHD_DATA");
      testDbstmt.executeUpdate("create table MFHD_DATA ( RECORD_SEGMENT blob, MFHD_ID int, SEQNUM int )");
    }
    System.out.println("Loading mfhd_data data for "+testMfhds.size()+" mfhds.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT RECORD_SEGMENT, SEQNUM FROM MFHD_DATA WHERE MFHD_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO MFHD_DATA (RECORD_SEGMENT, SEQNUM, MFHD_ID) VALUES (?,?,?)")){
      for (int mfhd : testMfhds) {
        readStmt.setInt(1, mfhd);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setString(1, new String(rs.getBytes(1),StandardCharsets.UTF_8));
            writeStmt.setInt(2, rs.getInt(2));
            writeStmt.setInt(3, mfhd);
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void mfhd_item(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists MFHD_ITEM");
      testDbstmt.executeUpdate("create table MFHD_ITEM ( MFHD_ID int, ITEM_ID int, ITEM_ENUM text,"
          + " CHRON text, YEAR text, CAPTION text )");
    }
    System.out.println("Loading mfhd_item data for "+testMfhds.size()+" mfhds.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT ITEM_ID, ITEM_ENUM, CHRON, YEAR, CAPTION FROM MFHD_ITEM WHERE MFHD_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO MFHD_ITEM (MFHD_ID, ITEM_ID, ITEM_ENUM, CHRON, YEAR, CAPTION ) VALUES (?,?,?,?,?,?)")){
      for (int mfhd : testMfhds) {
        readStmt.setInt(1, mfhd);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, mfhd);
            writeStmt.setInt(2, rs.getInt(1));
            writeStmt.setString(3, rs.getString(2));
            writeStmt.setString(4, rs.getString(3));
            writeStmt.setString(5, rs.getString(4));
            writeStmt.setString(6, rs.getString(5));
            writeStmt.addBatch();
            testItems.add(rs.getInt(1));
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void mfhd_master(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists MFHD_MASTER");
      testDbstmt.executeUpdate("create table MFHD_MASTER ( MFHD_ID int, create_date date, update_date date, suppress_in_opac text )");
    }
    System.out.println("Loading mfhd_master data for "+testMfhds.size()+" mfhds.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT update_date, create_date, suppress_in_opac FROM MFHD_MASTER WHERE MFHD_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO MFHD_MASTER (MFHD_ID, update_date, create_date, suppress_in_opac ) VALUES (?,?,?,?)")){
      for (int mfhd : testMfhds) {
        readStmt.setInt(1, mfhd);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, mfhd);
            writeStmt.setTimestamp(2, rs.getTimestamp(1));
            writeStmt.setTimestamp(3, rs.getTimestamp(2));
            writeStmt.setString(4, rs.getString(3));
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void item(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists ITEM");
      testDbstmt.executeUpdate("create table ITEM ( ITEM_ID int, COPY_NUMBER int, ITEM_SEQUENCE_NUMBER int, "
          +     " HOLDS_PLACED int, RECALLS_PLACED int, ON_RESERVE text, TEMP_LOCATION int, PERM_LOCATION int,"
          +     " TEMP_ITEM_TYPE_ID int, ITEM_TYPE_ID int, MODIFY_DATE date, CREATE_DATE date )");
    }
    System.out.println("Loading item data for "+testItems.size()+" items.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT COPY_NUMBER, ITEM_SEQUENCE_NUMBER, "
        +     " HOLDS_PLACED, RECALLS_PLACED, ON_RESERVE, TEMP_LOCATION, PERM_LOCATION,"
        +     " TEMP_ITEM_TYPE_ID, ITEM_TYPE_ID, MODIFY_DATE, CREATE_DATE"
        +" FROM ITEM WHERE ITEM_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO ITEM (ITEM_ID, COPY_NUMBER, ITEM_SEQUENCE_NUMBER, "
        +     " HOLDS_PLACED, RECALLS_PLACED, ON_RESERVE, TEMP_LOCATION, PERM_LOCATION,"
        +     " TEMP_ITEM_TYPE_ID, ITEM_TYPE_ID, MODIFY_DATE, CREATE_DATE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
        ){
      for (int item : testItems) {
        readStmt.setInt(1, item);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, item);
            writeStmt.setInt(2, rs.getInt(1));//copy num
            writeStmt.setInt(3, rs.getInt(2));//seq num
            writeStmt.setInt(4, rs.getInt(3));//holds
            writeStmt.setInt(5, rs.getInt(4));//recalls
            writeStmt.setString(6, rs.getString(5));//reserve
            writeStmt.setInt(7, rs.getInt(6));//temp loc
            writeStmt.setInt(8, rs.getInt(7));//loc
            writeStmt.setInt(9, rs.getInt(8));//temp type
            writeStmt.setInt(10, rs.getInt(9));//type
            writeStmt.setTimestamp(11, rs.getTimestamp(10));//modify
            writeStmt.setTimestamp(12, rs.getTimestamp(11));//create
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void item_barcode(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists ITEM_BARCODE");
      testDbstmt.executeUpdate("create table ITEM_BARCODE ( ITEM_ID int, ITEM_BARCODE text, barcode_status string )");
    }
    System.out.println("Loading item_barcode data for "+testItems.size()+" items.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT ITEM_BARCODE, barcode_status FROM ITEM_BARCODE WHERE ITEM_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO ITEM_BARCODE ( ITEM_ID, ITEM_BARCODE, barcode_status ) VALUES (?,?,?)")){
      for (int item : testItems) {
        readStmt.setInt(1, item);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, item);
            writeStmt.setString(2, rs.getString(1));
            writeStmt.setString(3, rs.getString(2));
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void item_status_type(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (! replaceDBContents) return ;
    testDbstmt.executeUpdate("drop table if exists item_status_type");
    testDbstmt.executeUpdate("create table item_status_type ( item_status_type int, item_status_desc text )");
    System.out.println("Loading item_status_type data.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT item_status_type, item_status_desc FROM item_status_type");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO item_status_type ( item_status_type, item_status_desc ) VALUES (?,?)")){
      try (ResultSet rs = readStmt.executeQuery()) {
        while (rs.next()) {
          writeStmt.setInt(1, rs.getInt(1));
          writeStmt.setString(2, rs.getString(2));
          writeStmt.addBatch();
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void item_type(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (! replaceDBContents) return ;
    testDbstmt.executeUpdate("drop table if exists item_type");
    testDbstmt.executeUpdate("create table item_type ( item_type_id int, item_type_name text )");
    System.out.println("Loading item_type data.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT item_type_id, item_type_name FROM item_type");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO item_type ( item_type_id, item_type_name ) VALUES (?,?)")){
      try (ResultSet rs = readStmt.executeQuery()) {
        while (rs.next()) {
          writeStmt.setInt(1, rs.getInt(1));
          writeStmt.setString(2, rs.getString(2));
          writeStmt.addBatch();
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void item_status(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists item_status");
      testDbstmt.executeUpdate("create table item_status ( ITEM_ID int, item_status int, item_status_date date )");
    }
    System.out.println("Loading item_status data for "+testItems.size()+" items.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT item_status, item_status_date FROM item_status WHERE ITEM_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO item_status ( ITEM_ID, item_status, item_status_date ) VALUES (?,?,?)")){
      for (int item : testItems) {
        readStmt.setInt(1, item);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, item);
            writeStmt.setInt(2, rs.getInt(1));
            writeStmt.setTimestamp(3, rs.getTimestamp(2));
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void circ_transactions(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists circ_transactions");
      testDbstmt.executeUpdate("create table circ_transactions ( ITEM_ID int, current_due_date date )");
    }
    System.out.println("Loading circ_transactions data for "+testItems.size()+" items.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT current_due_date FROM circ_transactions WHERE ITEM_ID = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO circ_transactions ( ITEM_ID, current_due_date ) VALUES (?,?)")){
      for (int item : testItems) {
        readStmt.setInt(1, item);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, item);
            writeStmt.setTimestamp(2, rs.getTimestamp(1));
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void location(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (! replaceDBContents) return;
    testDbstmt.executeUpdate("drop table if exists location");
    testDbstmt.executeUpdate("create table location ( location_code string, location_id int,"
        + " location_display_name string, location_name string )");
    System.out.println("Loading location data.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT location_code, location_id, location_display_name, location_name FROM location");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO location ( location_code, location_id, location_display_name, location_name )"
            + " VALUES (?,?,?,?)")){
      try (ResultSet rs = readStmt.executeQuery()) {
        while (rs.next()) {
          writeStmt.setString(1, rs.getString(1));
          writeStmt.setInt(2, rs.getInt(2));
          writeStmt.setString(3, rs.getString(3));
          writeStmt.setString(4, rs.getString(4));
          writeStmt.addBatch();
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void circ_policy_group(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (! replaceDBContents) return;
    testDbstmt.executeUpdate("drop table if exists circ_policy_group");
    testDbstmt.executeUpdate("create table circ_policy_group ( circ_group_name string, circ_group_id int )");
    System.out.println("Loading circ_policy_group data.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT circ_group_name, circ_group_id FROM circ_policy_group");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO circ_policy_group ( circ_group_name, circ_group_id ) VALUES (?,?)")){
      try (ResultSet rs = readStmt.executeQuery()) {
        while (rs.next()) {
          writeStmt.setString(1, rs.getString(1));
          writeStmt.setInt(2, rs.getInt(2));
          writeStmt.addBatch();
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void circ_policy_locs(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (! replaceDBContents) return;
    testDbstmt.executeUpdate("drop table if exists circ_policy_locs");
    testDbstmt.executeUpdate("create table circ_policy_locs ( location_id int, circ_group_id int )");
    System.out.println("Loading circ_policy_locs data.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT location_id, circ_group_id FROM circ_policy_locs");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO circ_policy_locs ( location_id, circ_group_id ) VALUES (?,?)")){
      try (ResultSet rs = readStmt.executeQuery()) {
        while (rs.next()) {
          writeStmt.setInt(1, rs.getInt(1));
          writeStmt.setInt(2, rs.getInt(2));
          writeStmt.addBatch();
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void line_item(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists line_item");
      testDbstmt.executeUpdate("create table line_item ( line_item_id int, bib_id int, quantity int, po_id int )");
    }
    System.out.println("Loading line_item data for "+testBibs.size()+" bibs.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT line_item_id, quantity, po_id FROM line_item WHERE bib_id = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO line_item ( line_item_id, bib_id, quantity, po_id ) VALUES (?,?,?,?)")){
      for (int bib : testBibs) {
        readStmt.setInt(1, bib);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, rs.getInt(1));
            writeStmt.setInt(2, bib);
            writeStmt.setInt(3, rs.getInt(2));
            writeStmt.setInt(4, rs.getInt(3));
            writeStmt.addBatch();
            testLineItems.add(rs.getInt(1));
            purchaseOrders.add(rs.getInt(3));
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void line_item_copy_status(Connection testDb, Connection voyager, Statement testDbstmt)
      throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists line_item_copy_status");
      testDbstmt.executeUpdate(
          "create table line_item_copy_status ("
          + " line_item_id int, mfhd_id int, location_id int, line_item_status int, status_date date )");
    }
    System.out.println("Loading line_item_copy_status data for "+testMfhds.size()+" mfhds.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT line_item_id, location_id, line_item_status, status_date"
        + " FROM line_item_copy_status WHERE mfhd_id = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO line_item_copy_status"
            + " ( line_item_id, mfhd_id, location_id, line_item_status, status_date )"
            + " VALUES (?,?,?,?,?)")){
      for (int mfhd : testMfhds) {
        readStmt.setInt(1, mfhd);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, rs.getInt("line_item_id"));
            writeStmt.setInt(2, mfhd);
            writeStmt.setInt(3, rs.getInt("location_id"));
            writeStmt.setInt(4, rs.getInt("line_item_status"));
            writeStmt.setTimestamp(5, rs.getTimestamp("status_date"));
            writeStmt.addBatch();
            testLineItems.add(rs.getInt("line_item_id"));
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void subscriptionAndIssues(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {
    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists subscription");
      testDbstmt.executeUpdate("create table subscription ( line_item_id int, subscription_id int )");
    }
    System.out.println("Loading subscription data for "+testLineItems.size()+" line_item_ids.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT subscription_id FROM subscription WHERE line_item_id = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO subscription (line_item_id, subscription_id) VALUES (?,?)")){
      for (int li : testLineItems) {
        readStmt.setInt(1, li);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, li);
            writeStmt.setInt(2, rs.getInt(1));
            writeStmt.addBatch();
            subscriptions.add(rs.getInt(1));
          }
        }
      }
      writeStmt.executeBatch();
    }
  }
  
  private static void component(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {

    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists component");
      testDbstmt.executeUpdate("create table component ( component_id int, subscription_id int )");
    }
    System.out.println("Loading component data for "+subscriptions.size()+" subscriptions.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT component_id FROM component WHERE subscription_id = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO component (component_id, subscription_id) VALUES (?,?)")){
      for (int subscription : subscriptions) {
        readStmt.setInt(1, subscription);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, rs.getInt(1));
            writeStmt.setInt(2, subscription);
            writeStmt.addBatch();
            components.add(rs.getInt(1));
          }
        }
      }
      writeStmt.executeBatch();
    }

  }

  private static void issues_received(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {

    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists issues_received");
      testDbstmt.executeUpdate("create table issues_received ( component_id int, issue_id int, opac_suppressed int )");
    }
    System.out.println("Loading issues_received data for "+components.size()+" components.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT issue_id, opac_suppressed FROM issues_received WHERE component_id = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO issues_received (component_id, issue_id, opac_suppressed) VALUES (?,?,?)")){
      for (int component : components) {
        readStmt.setInt(1, component);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, component);
            writeStmt.setInt(2, rs.getInt(1));
            writeStmt.setInt(3, rs.getInt(2));
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void serial_issues(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {

    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists serial_issues");
      testDbstmt.executeUpdate("create table serial_issues ( component_id int, issue_id int, enumchron string, receipt_date date )");
    }
    System.out.println("Loading serial_issues data for "+components.size()+" components.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT issue_id, enumchron, receipt_date FROM serial_issues WHERE component_id = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO serial_issues (component_id, issue_id, enumchron, receipt_date) VALUES (?,?,?,?)")){
      for (int component : components) {
        readStmt.setInt(1, component);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setInt(1, component);
            writeStmt.setInt(2, rs.getInt(1));
            writeStmt.setString(3, rs.getString(2));
            writeStmt.setTimestamp(4, rs.getTimestamp(3));
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }

  private static void purchase_order(Connection testDb, Connection voyager, Statement testDbstmt) throws SQLException {

    if (replaceDBContents) {
      testDbstmt.executeUpdate("drop table if exists purchase_order");
      testDbstmt.executeUpdate("create table purchase_order ( po_approve_date date, po_id int, po_type int )");
    }
    System.out.println("Loading purchase_order data for "+purchaseOrders.size()+" purchase orders.");
    try (PreparedStatement readStmt = voyager.prepareStatement(
        "SELECT po_approve_date, po_type FROM purchase_order WHERE po_id = ?");
        PreparedStatement writeStmt = testDb.prepareStatement(
            "INSERT INTO purchase_order (po_approve_date, po_id, po_type) VALUES (?,?,?)")){
      for (int po : purchaseOrders) {
        readStmt.setInt(1, po);
        try (ResultSet rs = readStmt.executeQuery()) {
          while (rs.next()) {
            writeStmt.setTimestamp(1,rs.getTimestamp(1));
            writeStmt.setInt(2, po);
            writeStmt.setInt(3, rs.getInt(2));
            writeStmt.addBatch();
          }
        }
      }
      writeStmt.executeBatch();
    }
  }
/*
drop table line_item;
CREATE TABLE line_item ( line_item_id int, bib_id int, quantity int, po_id int);
INSERT INTO line_item VALUES(17081,369282,NULL,NULL);
.quit
 */
}
