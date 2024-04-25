package edu.cornell.library.integration.folio;

import java.io.File;
import java.io.IOException;

import edu.cornell.library.integration.db_test.TestUtil;

public class StaticOkapiClient extends OkapiClient {
  @Override
  public String post(final String endPoint, final String json) throws IOException {
    return "";
  }

  @Override
  public String put(final String endPoint, final String uuid, final String json) throws IOException {
    return "";
  }

  @Override
  public String delete(final String endPoint, final String uuid) throws IOException {
    return "";
  }

  @Override
  public String deleteAll(final String endPoint, final String notDeletedQuery, final boolean verbose) throws IOException {
    return "";
  }

  @Override
  public String getRecord(final String endPoint, final String uuid) throws IOException {
    return TestUtil.loadResourceFile("getRecord" + File.separator + uuid);
  }

  @Override
  public String query(final String endPoint, final String query, final Integer limit) throws IOException {
    // should we use query?
    // should we enforce limit?
    String data = "static_okapi_data" + endPoint.replace("/", File.separator) + ".json";
    try {
      return TestUtil.loadResourceFile(data);
    } catch (Exception e) {
      throw new IOException("Failed to load static data file: " + data + System.lineSeparator() + e.getMessage());
    }
  }

  @Override
  public String query(final String endPointQuery) throws IOException {
    String data = "static_okapi_data" + endPointQuery.replace("/", File.separator) + ".json";
    try {
      return TestUtil.loadResourceFile(data);
    } catch (Exception e) {
      throw new IOException("Failed to load static data file: " + data + System.lineSeparator() + e.getMessage());
    }
  }
}
