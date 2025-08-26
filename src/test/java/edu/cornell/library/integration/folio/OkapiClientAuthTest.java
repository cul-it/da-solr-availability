package edu.cornell.library.integration.folio;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.naming.AuthenticationException;

public class OkapiClientAuthTest {

  public static void main(String[] args) throws IOException, AuthenticationException, InterruptedException {

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

    OkapiClient folio = new OkapiClient(prop, folioConfig);
    folio.printLoginStatus(folio);

    Instant testCompletedAt = Instant.now().plus(17, ChronoUnit.MINUTES);
    System.out.println("\nWe're logged in. The access tokens are meant to last 10 minutes, so we will make"
        + " periodic queries for the next 17, ending at "+testCompletedAt+ ". This should give enough time to"
        + " require two refreshes of the access token.");
    System.out.println("We'll just be retrieving statistical codes each time. The values won't be verified,"
        + " just that they are retrievable.");
    
    Random generator = new Random();
    retrieveStatCodes(folio, generator);
    while (Instant.now().isBefore(testCompletedAt)) {
      int seconds = 30 + generator.nextInt(30);
      System.out.format("\nWaiting %d seconds\n", seconds);
      Thread.sleep(seconds*1000);
      folio.printLoginStatus(folio);
      retrieveStatCodes(folio, generator);
    }
  }

  private static void retrieveStatCodes(OkapiClient folio, Random generator) throws IOException, AuthenticationException {
    ReferenceData statCodes = new ReferenceData(folio,"/statistical-codes","code");
    Object[] values = statCodes.dataByName.entrySet().toArray();
    System.out.format("codes: %d; random code: %s\n", values.length,
        values[generator.nextInt(values.length)]);

  }

}
