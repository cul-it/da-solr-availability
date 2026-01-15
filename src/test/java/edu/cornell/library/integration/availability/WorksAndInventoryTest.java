package edu.cornell.library.integration.availability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.naming.AuthenticationException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.db_test.DbBaseTest;

public class WorksAndInventoryTest  extends DbBaseTest{


  static Connection testConnection = null;

  @BeforeClass
  public static void connect() throws SQLException, IOException, AuthenticationException {
    setup();
    testConnection = getConnection();
  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    if (testConnection != null) {
      testConnection.close();
    }
  }

  @Test
  public void worksTest() throws SQLException {

    // The test should be set up with five bibs matching the work id, but one flagged not active
    int workId = 1215984;
    try (PreparedStatement stmt = testConnection.prepareStatement("SELECT * FROM bib2work WHERE work_id = ?")) {
      stmt.setInt(1, workId);
      Map<String,Boolean> allHrids = new HashMap<>();
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next())
          allHrids.put(rs.getString("hrid"), rs.getBoolean("active"));
      }
      assertEquals(5, allHrids.size());
      assertFalse(allHrids.get("17186721"));
      assertTrue(allHrids.get("968406"));
      assertTrue(allHrids.get("1907751"));
      assertTrue(allHrids.get("2360967"));
      assertTrue(allHrids.get("2360968"));
    }

    // The query used in production to identify linkable "other forms of this work" should pay attention to 'active'
    try (PreparedStatement bibs4WorkStmt = testConnection.prepareStatement(WorksAndInventory.selectB2W2)) {
      bibs4WorkStmt.setInt(1, workId);
      Set<String> matchingHrids = new HashSet<>();
      try (ResultSet rs = bibs4WorkStmt.executeQuery()) {
        while (rs.next()) matchingHrids.add(rs.getString(1));
      }
      assertEquals(4 , matchingHrids.size());
      assertFalse(matchingHrids.contains("17186721"));
      assertTrue(matchingHrids.contains("968406"));
      assertTrue(matchingHrids.contains("1907751"));
      assertTrue(matchingHrids.contains("2360967"));
      assertTrue(matchingHrids.contains("2360968"));
    }
  }
}
