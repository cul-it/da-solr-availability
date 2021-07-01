package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

public interface ChangeDetector {

  public Map<String,Set<Change>> detectChanges(
      Connection inventory, OkapiClient okapi, Timestamp since) throws SQLException, IOException;
}
