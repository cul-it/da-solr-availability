package edu.cornell.library.integration.db_test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 * Requirements to run these tests with local MySQL:
 * 1. Docker must be installed on the machine running these tests.
 * 2. UseTestContainers env var needs to be set.
 * 
 * UseTestContainers=true
 * 
 * - As long as UseTestContainers variable is set with any value, DB container will start.
 * 
 * To run tests using SQLITE, set the following env var:
 * 
 * UseSqlite=true
 * 
 * - If UseTestContainers and UseSqlite are not set, it will use the VoyagerToSolrConfig to connect to DB directly.
 */
public class DbBaseTest {
  protected static final String USE_TEST_CONTAINERS = "UseTestContainers";
  protected static final String USE_SQLITE = "UseSqlite";
  protected static final String USE_VOYAGER = "UseVoyager";
  protected static String useTestContainers = null;
  protected static String useSqlite = null;
  protected static String useVoyager = null;
  protected static final String TEST_RESOURCE_PATH = Path.of("src", "test", "resources").toString();
  protected static final String FOLIO_MYSQL_CREATE_STATEMENTS_PATH = new File(TEST_RESOURCE_PATH,
      "folio_mysql_create_statements.sql").getAbsolutePath();
  protected static final String FOLIO_SQLITE_CREATE_STATEMENTS_PATH = new File(TEST_RESOURCE_PATH,
      "folio_sqlite_create_statements.sql").getAbsolutePath();
  protected static final String VOYAGER_SQL_PATH = Path.of("src", "test", "resources", "voyagerTest.sql").toString();
  protected static final String FOLIO_INSERT_STATEMENTS_PATH = new File(TEST_RESOURCE_PATH, "folio_data.sql")
      .getAbsolutePath();

  public static void setup() throws IOException, SQLException {
    useTestContainers = System.getenv(USE_TEST_CONTAINERS);
    useSqlite = System.getenv(USE_SQLITE);
    useVoyager = System.getenv(USE_VOYAGER);

    if (useTestContainers != null) {
      System.out.println("TestContainers init");
      AbstractContainerBaseTest.init(getInitSqls(FOLIO_MYSQL_CREATE_STATEMENTS_PATH));
    } else if (useSqlite != null) {
      System.out.println("Sqlite init");
      SqliteBaseTest.init(getInitSqls(FOLIO_SQLITE_CREATE_STATEMENTS_PATH));
    } else if (useVoyager != null) {
      SqliteBaseTest.init(Arrays.asList(VOYAGER_SQL_PATH));
    }
  }

  public static Connection getConnection() throws SQLException, IOException {
    if (useTestContainers != null) {
      return AbstractContainerBaseTest.getConnection();
    } else if (useSqlite != null) {
      return SqliteBaseTest.getConnection();
    } else if (useVoyager != null) {
      return SqliteBaseTest.getConnection();
    }

    return null;
  }

  protected static List<String> getInitSqls(String createPath) {
    List<String> sqls = new ArrayList<>();
    sqls.add(createPath);
    sqls.add(FOLIO_INSERT_STATEMENTS_PATH);
    return sqls;
  }
}
