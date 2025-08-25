package edu.cornell.library.integration.folio;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.naming.AuthenticationException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BoundWithTest {

  static Connection inventory = null;
  static Locations locations = null;
  static OkapiClient testOkapiClient = null;

  @BeforeClass
  public static void connect() throws SQLException, IOException, AuthenticationException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }

    inventory = DriverManager.getConnection(
        prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));

    testOkapiClient = new StaticOkapiClient();
    locations = new Locations(testOkapiClient);
  }

  @Test
  public void BasicFromNote() throws SQLException, IOException {
    Map<String,BoundWith> boundWiths = BoundWith.fromNote(inventory, "31924101472383",null);
    String expected =
    "{\"8048352\":{"
    + "\"masterItemId\":\"99b26736-5213-4d50-a441-784c612ad94c\","
    + "\"masterBibId\":\"1904477\","
    + "\"masterTitle\":\"Factors related to diet of freshman women at Iowa State University\","
    + "\"masterEnum\":\"Film 1533 to 1538 Titles no.1-6\","
    + "\"barcode\":\"31924101472383\","
    + "\"status\":{\"status\":\"Available\"}}}";
    assertEquals(expected,mapper.writeValueAsString(boundWiths));
  }

  @Test
  public void EnumeratedFromNote() throws SQLException, IOException {
    Map<String,BoundWith> boundWiths = BoundWith.fromNote(inventory, "v.8 (1930) 31924101472474",null);
    String expected =
    "{\"8048237\":{"
    + "\"masterItemId\":\"f6a8d4cb-23e8-461e-a032-a48d5be95e2c\","
    + "\"masterBibId\":\"1904533\","
    + "\"masterTitle\":\"A study of factors influencing the oxidation of fat in dry whole milk\","
    + "\"masterEnum\":\"Film 1606-1614\","
    + "\"thisEnum\":\"v.8 (1930)\","
    + "\"barcode\":\"31924101472474\","
    + "\"status\":{\"status\":\"Available\"}}}";
    assertEquals(expected,mapper.writeValueAsString(boundWiths));
  }

  @Test
  public void EnumeratedPipedFromNote() throws SQLException, IOException {
    Map<String,BoundWith> boundWiths = BoundWith.fromNote(inventory, "v.8 (1930)|31924101472474",null);
    String expected =
    "{\"8048237\":{"
    + "\"masterItemId\":\"f6a8d4cb-23e8-461e-a032-a48d5be95e2c\","
    + "\"masterBibId\":\"1904533\","
    + "\"masterTitle\":\"A study of factors influencing the oxidation of fat in dry whole milk\","
    + "\"masterEnum\":\"Film 1606-1614\","
    + "\"thisEnum\":\"v.8 (1930)\","
    + "\"barcode\":\"31924101472474\","
    + "\"status\":{\"status\":\"Available\"}}}";
    assertEquals(expected,mapper.writeValueAsString(boundWiths));
  }

  @Test
  public void EnumeratedExtraneousSpaceFromNote() throws SQLException, IOException {
    Map<String,BoundWith> boundWiths = BoundWith.fromNote(
        inventory, "\\nv.8 (1930) | 31924101472474\\n\\n  ",null);
    String expected =
    "{\"8048237\":{"
    + "\"masterItemId\":\"f6a8d4cb-23e8-461e-a032-a48d5be95e2c\","
    + "\"masterBibId\":\"1904533\","
    + "\"masterTitle\":\"A study of factors influencing the oxidation of fat in dry whole milk\","
    + "\"masterEnum\":\"Film 1606-1614\","
    + "\"thisEnum\":\"v.8 (1930)\","
    + "\"barcode\":\"31924101472474\","
    + "\"status\":{\"status\":\"Available\"}}}";
    assertEquals(expected,mapper.writeValueAsString(boundWiths));
  }

  @Test
  public void MultipleInOneFromNote() throws SQLException, IOException {
    Map<String,BoundWith> boundWiths = BoundWith.fromNote(inventory,
        "pt.1=v.20    31924061381269\\npt.2=v.24    31924061381277\\n"
        + "pt.3=v.29    31924061381293\\npt.4-pt.5=v.33-35  31924061381319  \\n\\n",null);
    String expected =
    "{\"3697490\":{\"masterItemId\":\"f8b12212-96ff-474f-b29c-20d9b24e7a24\","
                + "\"masterBibId\":\"1948271\","
                + "\"masterTitle\":\"Opuscula entomologica\","
                + "\"masterEnum\":\"v.19-21\","
                + "\"thisEnum\":\"pt.1=v.20\","
                + "\"barcode\":\"31924061381269\","
                + "\"status\":{\"status\":\"Available\"}},"
    +"\"3697491\":{\"masterItemId\":\"c1948f1d-f069-4ae1-a5dd-d8ee648a86b2\","
                + "\"masterBibId\":\"1948271\","
                + "\"masterTitle\":\"Opuscula entomologica\","
                + "\"masterEnum\":\"v.22-25\","
                + "\"barcode\":\"31924061381277\","
                + "\"status\":{\"status\":\"Available\"}},"
    +"\"3697492\":{\"masterItemId\":\"a0ca98db-d3f8-4ccd-8a21-0cac04530365\","
                + "\"masterBibId\":\"1948271\","
                + "\"masterTitle\":\"Opuscula entomologica\","
                + "\"masterEnum\":\"v.26-29\","
                + "\"barcode\":\"31924061381293\","
                + "\"status\":{\"status\":\"Available\"}},"
    +"\"3697494\":{\"masterItemId\":\"026a7279-9f85-461b-9d8e-a363838f23a0\","
                + "\"masterBibId\":\"1948271\","
                + "\"masterTitle\":\"Opuscula entomologica\","
                + "\"masterEnum\":\"v.33-36\","
                + "\"thisEnum\":\"33-35\","
                + "\"barcode\":\"31924061381319\","
                + "\"status\":{\"status\":\"Available\"}}}";
    assertEquals(expected,mapper.writeValueAsString(boundWiths));
  }

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

}
