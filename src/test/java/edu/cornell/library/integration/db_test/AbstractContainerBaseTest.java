package edu.cornell.library.integration.db_test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.junit.platform.commons.util.StringUtils;
import org.testcontainers.containers.MySQLContainer;

public class AbstractContainerBaseTest {
  protected static final String DBNAME = "test";
  protected static final String DBUID = "test_user";
  protected static final String DBPWD = "test_pwd";
  protected static Properties PROPS = null;
  @SuppressWarnings("rawtypes")
  protected final static MySQLContainer mysqlContainer;
  protected static File testPropertiesFile = null;
  protected static boolean initialized = false;

  static {
    mysqlContainer = new MySQLContainer<>("mysql:latest")
      .withDatabaseName(DBNAME)
      .withUsername(DBUID)
      .withPassword(DBPWD)
      .withDatabaseName(DBNAME);
    mysqlContainer.start();
  }

  public static void init(List<String> sqlFiles)
      throws SQLException, UnsupportedEncodingException, FileNotFoundException, IOException {
    if (!initialized) {
      try ( Connection conn = getConnection();
            Statement stmt = conn.createStatement() ) {
        for (String sql : sqlFiles)
          for (String line : Files.readAllLines(Paths.get(sql)))
            if (!StringUtils.isBlank(line))
              stmt.executeUpdate(line);
      }

      initialized = true;
    }
  }
  
  public static Connection getConnection() throws SQLException {
    String jdbc_str = mysqlContainer.getJdbcUrl() + "?user=" + DBUID + "&password=" + DBPWD;
    System.out.println(jdbc_str);
    return DriverManager.getConnection(jdbc_str);
  }
}
