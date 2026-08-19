package edu.cornell.library.integration.folio;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.AuthenticationException;

import edu.cornell.library.integration.marc.MarcRecord;

public class FolioDataExportTest {

  public static void main(String[] args)
      throws FileNotFoundException, IOException, AuthenticationException, InterruptedException {
    Map<String, String> env = System.getenv();
    String configFile = env.get("configFile");
    String folioConfig = env.get("target_folio");
    System.out.format("%s: %s\n", configFile, folioConfig);

    // load config
    if (configFile == null)
      throw new IllegalArgumentException("configFile must be set in environment to valid file path.");
    Properties prop = new Properties();
    File f = new File(configFile);
    if (f.exists()) {
      try ( InputStream is = new FileInputStream(f) ) { prop.load( is ); }
    } else System.out.println("File does not exist: "+configFile);

    if (folioConfig == null)
      throw new IllegalArgumentException("target_folio must be set in environment to name of target Folio instance.");

    FolioClient folio = new FolioClient(prop, folioConfig);
    folio.printLoginStatus();

    List<String> instanceIds = new ArrayList<>();
    instanceIds.add("c8e69b87-6771-4a79-895c-119a92916fa7");
    instanceIds.add("ed5a920b-bfaa-4cec-b932-e09d7cb08624");
    instanceIds.add("1db8b362-eda7-4777-b09d-48de554c533b");
    List<MarcRecord> records = DataExport.retrieveMarcByUuid(folio, instanceIds);
    for (MarcRecord r : records)
      System.out.println(r.toString());
  }

}
