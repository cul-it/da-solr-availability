package edu.cornell.library.integration.voyager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class VoyagerDBConnection implements AutoCloseable {

  public final Connection connection;
  final File tempDB;

  public VoyagerDBConnection( String sqlFile ) throws SQLException, IOException {
    this(sqlFile,false);
  }

  public VoyagerDBConnection( String sqlFile, Boolean keepDBFile ) throws SQLException, IOException {

    if ( keepDBFile ) {
      String dbFile = sqlFile.substring(0, sqlFile.length()-3)+"db";
      this.connection = DriverManager.getConnection("jdbc:sqlite:"+dbFile);
      this.tempDB = null;
    } else {
      this.tempDB = File.createTempFile("da-solr-availability-", "-test.db");
      this.tempDB.deleteOnExit();
      this.connection = DriverManager.getConnection("jdbc:sqlite:"+this.tempDB.getPath());
    }

    try (Statement stmt = this.connection.createStatement();
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sqlFile),"UTF-8"))) {
      String line;
      while ((line = br.readLine()) != null)
        stmt.executeUpdate(line);
    }
  }

  @Override
  public void close() throws SQLException {
    this.connection.close();
    if ( this.tempDB != null )
      this.tempDB.delete();
  }

  public static Connection getLiveConnection(String propsFile) throws SQLException, IOException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(propsFile)){
      prop.load(in);
    }

    return DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
  }
}
