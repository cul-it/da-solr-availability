package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.AuthenticationException;

public class ServicePoints {

  public static void initialize( OkapiClient okapi ) throws IOException, AuthenticationException {
    if (_byUuid.isEmpty()) populateServicePoints(okapi);
  }

  public final static ServicePoint getByUuid( String uuid ) {
    if (_byUuid.containsKey(uuid)) return _byUuid.get(uuid);
    return null;
  }

  public static class ServicePoint {
    public final String uuid;
    public final String displayName;
    public final Integer shelvingLagTime;
    public ServicePoint(String id, String displayName, Integer shelvingLagTime) {
      this.uuid = id;
      this.displayName = displayName;
      this.shelvingLagTime = shelvingLagTime;
    }
  }

  private static void populateServicePoints( OkapiClient okapi ) throws IOException, AuthenticationException {
    List<Map<String,Object>> servicePoints = okapi.queryAsList("/service-points",null,500 );
    for ( Map<String,Object> sp : servicePoints ) {
      String id = (String)sp.get("id");
      String displayName = (String)sp.get("discoveryDisplayName");
      Integer shelvingLagTime = (Integer)sp.get("shelvingLagTime");
      _byUuid.put(id, new ServicePoint(id,displayName,shelvingLagTime));
    }
  }

  private static final Map<String,ServicePoint> _byUuid = new HashMap<>();
}
