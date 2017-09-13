package edu.cornell.library.integration.voyager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;

public class DownloadMARC {

  /**
   * Retrieve specified MARC record and return MARC21 format as string.
   * 
   * @param voyager Database connection to Oracle
   * @param type
   *          (RecordType.BIBLIOGRAPHIC, RecordType.HOLDINGS, RecordType.AUTHORITY)
   * @param id
   * @return String MARC21 encoded MARC file; null if id not found in Voyager
   * @throws SQLException
   * @throws IOException
   */
  public static String downloadMrc(Connection voyager, RecordType type, Integer id)
      throws SQLException, IOException {
    try ( PreparedStatement pstmt = prepareStatement(voyager, type) ) {
      return queryVoyager(pstmt, id);
    }
  }

  /**
   * Retrieve specified MARC record and return MARC XML format as string.
   * 
   * @param voyager Database connection to Oracle
   * @param type
   *          (RecordType.BIBLIOGRAPHIC, RecordType.HOLDINGS, RecordType.AUTHORITY)
   * @param id
   * @return String XML encoded MARC file; null if id not found in Voyager
   * @throws SQLException
   * @throws IOException
   */
  public static String downloadXml(Connection voyager, RecordType type, Integer id)
      throws SQLException, IOException {
    return MarcRecord.marcToXml(downloadMrc(voyager, type, id));
  }

  private static PreparedStatement prepareStatement(Connection voyager, RecordType type) throws SQLException {
    if (type.equals(RecordType.BIBLIOGRAPHIC))
      return voyager.prepareStatement("SELECT * FROM BIB_DATA WHERE BIB_DATA.BIB_ID = ? ORDER BY BIB_DATA.SEQNUM");
    else if (type.equals(RecordType.HOLDINGS))
      return voyager.prepareStatement("SELECT * FROM MFHD_DATA WHERE MFHD_DATA.MFHD_ID = ? ORDER BY MFHD_DATA.SEQNUM");
    else
      return voyager.prepareStatement("SELECT * FROM AUTH_DATA WHERE AUTH_DATA.AUTH_ID = ? ORDER BY AUTH_DATA.SEQNUM");
  }

  private static String queryVoyager(PreparedStatement pstmt, Integer id) throws SQLException, IOException {
    pstmt.setInt(1, id);
    String marcRecord = null;

    try ( ByteArrayOutputStream bb = new ByteArrayOutputStream();
          ResultSet rs = pstmt.executeQuery() ) {
      while (rs.next())
        bb.write(rs.getBytes("RECORD_SEGMENT"));
      if (bb.size() == 0)
        return null;
      bb.close();
      marcRecord = new String(bb.toByteArray(), StandardCharsets.UTF_8);
    }
    return marcRecord;
  }
}
