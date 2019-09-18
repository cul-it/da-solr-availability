package edu.cornell.library.integration.changes;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

public interface ChangeDetector {

  public Map<Integer,Set<Change>> detectChanges(Connection voyager, Timestamp since) throws SQLException;
}
