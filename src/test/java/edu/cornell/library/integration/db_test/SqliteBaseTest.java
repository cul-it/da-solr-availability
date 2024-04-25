package edu.cornell.library.integration.db_test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.junit.platform.commons.util.StringUtils;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

public class SqliteBaseTest {
  protected static final String DBNAME = "test";
  protected static final String DBUID = "test_user";
  protected static final String DBPWD = "test_pwd";
  protected static final String INIT_DB_PATH = Path.of("src", "test", "resources", "base.db")
      .toString();
  protected static final String TEST_DB_PATH = Path.of("src", "test", "resources", "copy.db")
      .toString();
  protected static Properties PROPS = null;
  protected static File sourceInitDb = null;
  protected static File testPropertiesFile = null;

  public static void init(List<String> sqlFiles)
      throws SQLException, UnsupportedEncodingException, FileNotFoundException, IOException {
    if (sourceInitDb != null) { return; }

    sourceInitDb = new File(INIT_DB_PATH);
    sourceInitDb.deleteOnExit();
    try ( Connection conn = DriverManager.getConnection("jdbc:sqlite:" + sourceInitDb.getAbsolutePath());
          Statement stmt = conn.createStatement();) {
      for (String sql : sqlFiles)
        for (String line : Files.readAllLines(Paths.get(sql)))
          if (!StringUtils.isBlank(line))
            stmt.executeUpdate(line);
    }
  }

  public static Connection getConnection() throws IOException, SQLException {
    File dbCopy = createSqliteData();
    return DriverManager.getConnection("jdbc:sqlite:" + dbCopy.getAbsolutePath());
  }

  /*
   * Return a fresh copy of INIT_DB_PATH so each test will have a clean slate.
   */
  protected static File createSqliteData() throws IOException {
    File db = new File(TEST_DB_PATH);
    FileUtils.copyFile(sourceInitDb, db);
    db.deleteOnExit();

    return db;
  }
}
