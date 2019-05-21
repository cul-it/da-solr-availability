package edu.cornell.library.integration.voyager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;

import javax.xml.stream.XMLStreamException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class BoundWithTest {

  String expectedBoundWithJson4102195 =
  "{\"masterItemId\":4102195,"
  +"\"masterBibId\":2248567,"
  +"\"masterTitle\":\"Black biographical dictionaries, 1790-1950\","
  +"\"masterEnum\":\"title# 291 fiche 3/5\","
  +"\"thisEnum\":\"fiche 3/5\","
  +"\"status\":{\"available\":true,"
  +            "\"code\":{\"1\":\"Not Charged\"}}}";

  String expectedBoundWithJson1726636 =
  "{\"masterItemId\":1726636,"
  +"\"masterBibId\":3827392,"
  +"\"masterTitle\":\"Monograph of the Palaeontographical Society.\","
  +"\"masterEnum\":\"v.14\","
  +"\"thisEnum\":\"v.14\","
  +"\"status\":{\"available\":true,"
  +            "\"code\":{\"1\":\"Not Charged\"}}}";

  static VoyagerDBConnection testDB = null;
  static Connection voyagerTest = null;
  static Connection voyagerLive = null;

  @BeforeClass
  public static void connect() throws SQLException, IOException {
    testDB = new VoyagerDBConnection("src/test/resources/voyagerTest.sql");
    voyagerTest = testDB.connection;
//    voyagerLive = VoyagerDBConnection.getLiveConnection("database.properties");
  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    testDB.close();
  }

  @Test
  public void boundWithMainConstructor() throws SQLException, JsonProcessingException {
    DataField f = new DataField(6,"876",' ',' ',"‡3 fiche 3/5 ‡p 31924064096195");
    BoundWith b = BoundWith.from876Field(voyagerTest, f);
    assertEquals(expectedBoundWithJson4102195,b.toJson());
    f = new DataField(6,"876",' ',' ',"‡3 v.14 ‡p 31924004546812");
    b = BoundWith.from876Field(voyagerTest, f);
    assertEquals(expectedBoundWithJson1726636,b.toJson());
  }

  @Test
  public void boundWithDedupe() throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 833840);
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 1016218);

    // before dedupe, boundwith reference in holding block, empty item looks available
    assertNotNull(holdings.get(1016218).boundWiths);
    assertTrue( items.getItem(1016218,9621977).status.available );

    EnumSet<BoundWith.Flag> flags = BoundWith.dedupeBoundWithReferences(holdings, items);
    assertEquals( EnumSet.of(BoundWith.Flag.EMPTY_ITEMS,
                             BoundWith.Flag.HOLDING_REFS,
                             BoundWith.Flag.DEDUPED,
                             BoundWith.Flag.REF_STATUS),
                  flags);

    // after dedupe, no boundwith reference in holding block, empty item looks unavailable
    assertNull(holdings.get(1016218).boundWiths);
    assertFalse( items.getItem(1016218,9621977).status.available );
  }

}
// 4690713 2473239 7301315 2098051 2305477 3212523