package com.yahoo.ycsb.workloads;

import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by frojala on 16/08/16.
 */
public class GeodeWorkload extends Workload {

  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";
  public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";
  public static String table;

  /**
   * The name of the property for the field length distribution. Options are "uniform", "zipfian"
   * (favouring short records), "constant", and "histogram".
   * <p>
   * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
   * fieldlength property. If "histogram", then the histogram will be read from the filename
   * specified in the "fieldlengthhistogram" property.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

  /**
   * The name of the property for the length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY = "fieldlength";
  public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

  /**
   * The name of a property that specifies the filename containing the field length histogram (only
   * used if fieldlengthdistribution is "histogram").
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

  /**
   * Generator object that produces field lengths.  The value of this depends on the properties that
   * start with "FIELD_LENGTH_".
   */
  NumberGenerator fieldlengthgenerator;

  /**
   * The name of the property for deciding whether to read one field (false) or all fields (true) of
   * a record.
   */
  public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";
  public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";
  boolean readallfields;

  /**
   * The name of the property for deciding whether to write one field (false) or all fields (true)
   * of a record.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
  public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";
  boolean writeallfields;


  /**
   * The name of the property for deciding whether to check all returned
   * data against the formation template to ensure data integrity.
   */
  public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";
  public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "false";

  /**
   * Set to true if want to check correctness of reads. Must also
   * be set to true during loading phase to function.
   */
  private boolean dataintegrity;

  /**
   * The name of the property for the proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * LTE event specific properties
   */
  public static final String ATTACH_PROPORTION_PROPERTY = "attach_proportion";
  public static final String ATTACH_PROPORTION_PROPERTY_DEFAULT = "0.01";
  public static final String ATTACH_OPERATION = "ATTACH";
  public static final String DETACH_PROPORTION_PROPERTY = "detach_proportion";
  public static final String DETACH_PROPORTION_PROPERTY_DEFAULT = "0.01";
  public static final String DETACH_OPERATION = "DETACH";
  public static final String SERVICE_REQUEST_PROPORTION_PROPERTY = "service_request_proportion";
  public static final String SERVICE_REQUEST_PROPORTION_PROPERTY_DEFAULT = "0.31";
  public static final String SERVICE_REQUEST_OPERATION = "SERVICE_REQUEST";
  public static final String S1_RELEASE_PROPORTION_PROPERTY = "s1_release_proportion";
  public static final String S1_RELEASE_PROPORTION_PROPERTY_DEFAULT = "0.31";
  public static final String S1_RELEASE_OPERATION = "S1_RELEASE";
  public static final String TAU_PROPORTION_PROPERTY = "tau_proportion";
  public static final String TAU_PROPORTION_PROPERTY_DEFAULT = "0.09";
  public static final String TAU_OPERATION = "TAU";
  public static final String HANDOVER_PROPORTION_PROPERTY = "handover_proportion";
  public static final String HANDOVER_PROPORTION_PROPERTY_DEFAULT = "0.12";
  public static final String HANDOVER_OPERATION = "HANDOVER";
  public static final String CELL_RESELECT_PROPORTION_PROPERTY = "cell_reselect_proportion";
  public static final String CELL_RESELECT_PROPORTION_PROPERTY_DEFAULT = "0.07";
  public static final String CELL_RESELECT_OPERATION = "CELL_RESELECT";
  public static final String SESSION_MANAGEMENT_PROPORTION_PROPERTY = "session_management_proportion";
  public static final String SESSION_MANAGEMENT_PROPORTION_PROPERTY_DEFAULT = "0.07";
  public static final String SESSION_MANAGEMENT_OPERATION = "SESSION_MANAGEMENT";

  /**
   * The name of the property for the the distribution of requests across the keyspace. Options are
   * "uniform", "zipfian" and "latest"
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
  public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for adding zero padding to record numbers in order to match
   * string sort order. Controls the number of 0s to left pad with.
   */
  public static final String ZERO_PADDING_PROPERTY = "zeropadding";
  public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";


  /**
   * The name of the property for the max scan length (number of records).
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
  public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

  /**
   * The name of the property for the scan length distribution. Options are "uniform" and "zipfian"
   * (favoring short scans)
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for the order to insert records. Options are "ordered" or "hashed"
   */
  public static final String INSERT_ORDER_PROPERTY = "insertorder";
  public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

  /**
   * Percentage data items that constitute the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

  /**
   * Percentage operations that access the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

  /**
   * How many times to retry when insertion of a single item to a DB fails.
   */
  public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
  public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

  /**
   * On average, how long to wait between the retries, in seconds.
   */
  public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
  public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";


//  NumberGenerator keysequence;
//  NumberGenerator fieldchooser;
//  NumberGenerator scanlength;
//  int zeropadding;

  private DiscreteGenerator operationchooser;
  private NumberGenerator keychooser;
  private AcknowledgedCounterGenerator transactioninsertkeysequence;
  private boolean orderedinserts;
  private int recordcount;
  private int insertionRetryLimit;
  private int insertionRetryInterval;

  private Measurements _measurements = Measurements.getMeasurements();

  /**
   * We need to instantiate a DB object at the thread level as the interface does not provide the proper implementations.
   */

  /** property name of the port where Geode server is listening for connections. */
  private static final String SERVERPORT_PROPERTY_NAME = "geode.serverport";

  /** property name of the host where Geode server is running. */
  private static final String SERVERHOST_PROPERTY_NAME = "geode.serverhost";

  /** default value of {@link #SERVERHOST_PROPERTY_NAME}. */
  private static final String SERVERHOST_PROPERTY_DEFAULT = "localhost";

  /** property name to specify a Geode locator. This property can be used in both
   * client server and p2p topology */
  private static final String LOCATOR_PROPERTY_NAME = "geode.locator";

  /** property name to specify Geode topology. */
  private static final String TOPOLOGY_PROPERTY_NAME = "geode.topology";

  /** value of {@value #TOPOLOGY_PROPERTY_NAME} when peer to peer topology should be used.
   *  (client-server topology is default) */
  private static final String TOPOLOGY_P2P_VALUE = "p2p";

  private GemFireCache cache;

  /** true if ycsb client runs as a client to a Geode cache server. */
  private boolean isClient;
  /** Keep the region object at hand so it does not have to be created each time, saves on time. */
  private Region<String, UE> ueRegion;
  /** Keep the inserted ueIDs at hand to prevent trying to add a UE with the same ID (IMSI). Also useful for choosing a random UE. */
  private ConcurrentHashMap<String, Boolean> ueIDs;
  /** Keep the ueIDs as a list, so it can be indexed fast, used for choosing the next UE at random. */
  private List<String> ueIDsAsList;
  private Random random;

  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }

    String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);

    int insertstart = Integer.parseInt(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    int insertcount = Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));

    // Confirm valid values for insertstart and insertcount in relation to recordcount
    if (recordcount < (insertstart + insertcount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }

    dataintegrity = Boolean.parseBoolean(p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));

    // Confirm that fieldlengthgenerator returns a constant if data
    // integrity check requested.
    if (dataintegrity && !(p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
      System.err.println("Must have constant field size to check data integrity.");
      System.exit(-1);
    }

    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      orderedinserts = false;
    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY, ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY, ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordcount * frac);
    } else {
      orderedinserts = true;
    }

    operationchooser = createOperationGenerator(p);
    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);

    if (requestdistrib.compareTo("uniform") == 0) {
      keychooser = new UniformIntegerGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("sequential") == 0) {
      keychooser = new SequentialGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("zipfian") == 0) {
      // it does this by generating a random "next key" in part by taking the modulus over the
      // number of keys.
      // If the number of keys changes, this would shift the modulus, and we don't want that to
      // change which keys are popular so we'll actually construct the scrambled zipfian generator
      // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
      // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
      // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
      // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
      // the keyspace doesn't change from the perspective of the scrambled zipfian generator
      final double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
      int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor
      keychooser = new ScrambledZipfianGenerator(insertstart, insertstart + insertcount + expectednewkeys);
    } else if (requestdistrib.compareTo("latest") == 0) {
      keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
    } else if (requestdistrib.equals("hotspot")) {
      double hotsetfraction = Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction = Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(insertstart, insertstart + insertcount - 1, hotsetfraction, hotopnfraction);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
    }

    insertionRetryLimit = Integer.parseInt(p.getProperty(INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
    ueIDs = new ConcurrentHashMap<>(insertcount);
    random = new Random();
    random.setSeed(System.currentTimeMillis());
  }

  @Override
  public Object initThread(Properties props, int myThreadId, int threadCount) throws WorkloadException {
    // hostName where Geode cacheServer is running
    String serverHost = null;
    // port of Geode cacheServer
    int serverPort = 0;
    String locatorStr = null;

    if (props != null && !props.isEmpty()) {
      String serverPortStr = props.getProperty(SERVERPORT_PROPERTY_NAME);
      if (serverPortStr != null) {
        serverPort = Integer.parseInt(serverPortStr);
      }
      serverHost = props.getProperty(SERVERHOST_PROPERTY_NAME, SERVERHOST_PROPERTY_DEFAULT);
      locatorStr = props.getProperty(LOCATOR_PROPERTY_NAME);

      String topology = props.getProperty(TOPOLOGY_PROPERTY_NAME);
      if (topology != null && topology.equals(TOPOLOGY_P2P_VALUE)) {
        CacheFactory cf = new CacheFactory();
        if (locatorStr != null) {
          cf.set("locators", locatorStr);
        }
        cache = cf.create();
        ueRegion = getRegion(table);
        isClient = false;
        return null;
      }
    }
    isClient = true;
    DistributionLocatorId locator = null;
    if (locatorStr != null) {
      locator = new DistributionLocatorId(locatorStr);
    }
    ClientCacheFactory ccf = new ClientCacheFactory();
    if (serverPort != 0) {
      ccf.addPoolServer(serverHost, serverPort);
    } else if (locator != null) {
      ccf.addPoolLocator(locator.getHost().getCanonicalHostName(), locator.getPort());
    }
    cache = ccf.create();
    ueRegion = getRegion(table);
    return null;
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    UE ue = new UE();
    while (!ueIDs.put(ue.getIMSI(), false)) ue = new UE();
    Status status;
    int numOfRetries = 0;
    do {
      ueRegion.putIfAbsent(ue.getIMSI(), ue);
      status = Status.OK; // TODO: check if we can use the return value from the put above.
      if (status == Status.OK) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
          "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return (status == Status.OK);
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    if (ueIDsAsList == null) ueIDsAsList = Collections.list(ueIDs.keys());
    switch (operationchooser.nextString()) {
      case ATTACH_OPERATION:
        doInitialAttach();
        break;
      case DETACH_OPERATION:
        doDetach();
        break;
      case SERVICE_REQUEST_OPERATION:
        doServiceRequest();
        break;
      case S1_RELEASE_OPERATION:
        doS1release();
        break;
      case TAU_OPERATION:
        doTrackingAreaUpdate();
        break;
      case HANDOVER_OPERATION:
        doHandover();
        break;
      case CELL_RESELECT_OPERATION:
        doCellReSelection();
        break;
      case SESSION_MANAGEMENT_OPERATION:
        doSessionManagement();
        break;
      case "INSERT":
        doInsert(db, threadstate);
        ueIDsAsList = Collections.list(ueIDs.keys());
        break;
      default:
        doDetach();
    }
    return true;
  }

  private void doSessionManagement() {
    int ueIDindex = random.nextInt(ueIDsAsList.size());
    String ueID = ueIDsAsList.get(ueIDindex);
    long start = System.currentTimeMillis();
    UE ue = ueRegion.get(ueID);
    if (ue != null) {
      ue.session_management();
    }
    ueRegion.put(ueID, ue);
    long end = System.currentTimeMillis();
    _measurements.measure(SESSION_MANAGEMENT_OPERATION, (int) (end - start) );
  }

  private void doCellReSelection() {
    int ueIDindex = random.nextInt(ueIDsAsList.size());
    String ueID = ueIDsAsList.get(ueIDindex);
    long start = System.currentTimeMillis();
    UE ue = ueRegion.get(ueID);
    if (ue != null) {
      ue.cell_reselect();
    }
    ueRegion.put(ueID, ue);
    long end = System.currentTimeMillis();
    _measurements.measure(CELL_RESELECT_OPERATION, (int) (end - start) );
  }

  private void doHandover() {
    int ueIDindex = random.nextInt(ueIDsAsList.size());
    String ueID = ueIDsAsList.get(ueIDindex);
    long start = System.currentTimeMillis();
    UE ue = ueRegion.get(ueID);
    if (ue != null) {
      ue.S1_handover();
    }
    ueRegion.put(ueID, ue);
    long end = System.currentTimeMillis();
    _measurements.measure(HANDOVER_OPERATION, (int) (end - start) );
  }

  private void doTrackingAreaUpdate() {
    int ueIDindex = random.nextInt(ueIDsAsList.size());
    String ueID = ueIDsAsList.get(ueIDindex);
    long start = System.currentTimeMillis();
    UE ue = ueRegion.get(ueID);
    if (ue != null) {
      ue.tracking_area_update();
    }
    ueRegion.put(ueID, ue);
    long end = System.currentTimeMillis();
    _measurements.measure(TAU_OPERATION, (int) (end - start) );
  }

  private void doS1release() {
    int ueIDindex = random.nextInt(ueIDsAsList.size());
    String ueID = ueIDsAsList.get(ueIDindex);
    long start = System.currentTimeMillis();
    UE ue = ueRegion.get(ueID);
    if (ue != null) {
      ue.S1_release();
    }
    ueRegion.put(ueID, ue);
    long end = System.currentTimeMillis();
    _measurements.measure(S1_RELEASE_OPERATION, (int) (end - start) );
  }

  private void doServiceRequest() {
    int ueIDindex = random.nextInt(ueIDsAsList.size());
    String ueID = ueIDsAsList.get(ueIDindex);
    long start = System.currentTimeMillis();
    UE ue = ueRegion.get(ueID);
    if (ue != null) {
      ue.service_request();
    }
    ueRegion.put(ueID, ue);
    long end = System.currentTimeMillis();
    _measurements.measure(SERVICE_REQUEST_OPERATION, (int) (end - start) );
  }

  private void doDetach() {
    int ueIDindex = random.nextInt(ueIDsAsList.size());
    String ueID = ueIDsAsList.get(ueIDindex);
    long start = System.currentTimeMillis();
    UE ue = ueRegion.get(ueID);
    if (ue != null) {
      ue.detach();
    }
    ueRegion.put(ueID, ue);
    long end = System.currentTimeMillis();
    _measurements.measure(DETACH_OPERATION, (int) (end - start) );
  }

  private void doInitialAttach() {
    int ueIDindex = random.nextInt(ueIDsAsList.size());
    String ueID = ueIDsAsList.get(ueIDindex);
    long start = System.currentTimeMillis();
    UE ue = ueRegion.get(ueID);
    if (ue != null) {
      ue.initial_attach();
    }
    ueRegion.put(ueID, ue);
    long end = System.currentTimeMillis();
    _measurements.measure(ATTACH_OPERATION, (int) (end - start) );
  }

  @Override
  public void cleanup() throws WorkloadException {

  }


  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  private static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double attachproportion = Double.parseDouble(p.getProperty(ATTACH_PROPORTION_PROPERTY, ATTACH_PROPORTION_PROPERTY_DEFAULT));
    final double detachproportion = Double.parseDouble(p.getProperty(DETACH_PROPORTION_PROPERTY, DETACH_PROPORTION_PROPERTY_DEFAULT));
    final double servicerequestproportion = Double.parseDouble(p.getProperty(SERVICE_REQUEST_PROPORTION_PROPERTY, SERVICE_REQUEST_PROPORTION_PROPERTY_DEFAULT));
    final double s1releaseproportion = Double.parseDouble(p.getProperty(S1_RELEASE_PROPORTION_PROPERTY, S1_RELEASE_PROPORTION_PROPERTY_DEFAULT));
    final double tauproprotion = Double.parseDouble(p.getProperty(TAU_PROPORTION_PROPERTY, TAU_PROPORTION_PROPERTY_DEFAULT));
    final double handoverproportion = Double.parseDouble(p.getProperty(HANDOVER_PROPORTION_PROPERTY, HANDOVER_PROPORTION_PROPERTY_DEFAULT));
    final double cellreselectionproportion = Double.parseDouble(p.getProperty(CELL_RESELECT_PROPORTION_PROPERTY, CELL_RESELECT_PROPORTION_PROPERTY_DEFAULT));
    final double sessionmanagementproportion = Double.parseDouble(p.getProperty(SESSION_MANAGEMENT_PROPORTION_PROPERTY, SESSION_MANAGEMENT_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (attachproportion > 0) operationchooser.addValue(attachproportion, ATTACH_OPERATION);
    if (detachproportion > 0) operationchooser.addValue(detachproportion, DETACH_OPERATION);
    if (servicerequestproportion > 0) operationchooser.addValue(servicerequestproportion, SERVICE_REQUEST_OPERATION);
    if (s1releaseproportion > 0) operationchooser.addValue(s1releaseproportion, S1_RELEASE_OPERATION);
    if (tauproprotion > 0) operationchooser.addValue(tauproprotion, TAU_OPERATION);
    if (handoverproportion > 0) operationchooser.addValue(handoverproportion, HANDOVER_OPERATION);
    if (cellreselectionproportion > 0) operationchooser.addValue(cellreselectionproportion, CELL_RESELECT_OPERATION);
    if (sessionmanagementproportion > 0) operationchooser.addValue(sessionmanagementproportion, SESSION_MANAGEMENT_OPERATION);
    if (insertproportion > 0) operationchooser.addValue(insertproportion, "INSERT");

    return operationchooser;
  }

  private Region<String, UE> getRegion(String table) {
    Region<String, UE> r = cache.getRegion(table);
    if (r == null) {
      try {
        if (isClient) {
          ClientRegionFactory<String, UE> crf =
            ((ClientCache) cache).createClientRegionFactory(ClientRegionShortcut.PROXY);
          r = crf.create(table);
        } else {
          RegionFactory<String, UE> rf = ((Cache) cache).createRegionFactory(RegionShortcut.PARTITION);
          r = rf.create(table);
        }
      } catch (RegionExistsException e) {
        // another thread created the region
        r = cache.getRegion(table);
      }
    }
    return r;
  }

}

class UE implements com.gemstone.gemfire.Delta, Serializable {
  /**
   *  Static entity codes, will never change in this benchmark.
   */
  private static final String MNC = "244";
  private static final String MCC = "921";
  private static final String PLMN_ID = MNC + MCC;
  private static final String MMEGI = "M";
  private static final String MMEC = "C";
  private static final String MMEI = MMEGI + MMEC;
  private static final String GUMMEI = PLMN_ID + MMEI;
  public static final String PGW_ID = "ubiquitous.internet";
  private static final int TAI_SIZE = 15;

  /**
   *  Semi-static entity codes. Once generated on insert, will not be changed.
   */
  private String IMEI;
  private String MSIN;
  private String IMSI; // = PLMN_ID + MSIN;
  private String EPS_KEY; // = 128 bit (8 chars)
  private String Cipher_KEY; // = 128 bit
  private String Encryption_KEY; // = 128 bit

  /**
   *  Dynamic entities, will experience change on a regular basis.
   */
  private byte status; // 0 = Detached, 1 = Attached, 2 = active, 3 = idle;
  private int M_TMSI;
  private String GUTI; // = GUMMEI + M_TMSI;
  private String TIN;
  private int IP;
  private short C_RNTI;
  private int eNB_UE_S1AP;
  private int MME_YE_S1AP;
  private int OLD_eNB_UE_X2;
  private int NEW_eNB_UE_X2;
  private int ECI;
  private String ECGI; // = PLMN_ID + ECI;
  private int TAI;
  private List<Integer> TAI_list;
  private String PDN_ID;
  private byte EPS_bearer;
  private byte E_RA_bearer;
  private byte DR_bearer;
  private int S1_TEID_UL;
  private int S1_TEID_DL;
  private int S5_TEID_UL;
  private int S5_TEID_DL;
  private String K_ASME; // = 256 bit (16 chars)
  private String K_ENB; // = 256 bit
  private String K_NASint; // = 256 bit
  private String K_NASenc; // = 256 bit
  private String K_RRCint; // = 256 bit
  private String K_RRCenc; // = 256 bit
  private String K_UPenc; // = 256 bit

  /**
   * The transients to support deltas.
   */
  private transient boolean master_ch;
  private transient boolean status_ch;
  private transient boolean M_TMSI_ch;
  private transient boolean GUTI_ch;
  private transient boolean TIN_ch;
  private transient boolean IP_ch;
  private transient boolean C_RNTI_ch;
  private transient boolean eNB_UE_S1AP_ch;
  private transient boolean MME_YE_S1AP_ch;
  private transient boolean OLD_eNB_UE_X2_ch;
  private transient boolean NEW_eNB_UE_X2_ch;
  private transient boolean ECI_ch;
  private transient boolean ECGI_ch;
  private transient boolean TAI_ch;
  private transient boolean TAI_list_ch;
  private transient boolean PDN_ID_ch;
  private transient boolean EPS_bearer_ch;
  private transient boolean E_RA_bearer_ch;
  private transient boolean DR_bearer_ch;
  private transient boolean S1_TEID_UL_ch;
  private transient boolean S1_TEID_DL_ch;
  private transient boolean S5_TEID_UL_ch;
  private transient boolean S5_TEID_DL_ch;
  private transient boolean K_ASME_ch;
  private transient boolean K_ENB_ch;
  private transient boolean K_NASint_ch;
  private transient boolean K_NASenc_ch;
  private transient boolean K_RRCint_ch;
  private transient boolean K_RRCenc_ch;
  private transient boolean K_UPenc_ch;

  public UE() {
    Random rand = new Random();
    this.IMEI = randomString(15, rand);
    this.MSIN = randomString(10, rand);
    this.IMSI = PLMN_ID + MSIN;
    this.EPS_KEY = randomString(8, rand);
    this.Cipher_KEY = randomString(8, rand);
    this.Encryption_KEY = randomString(8, rand);
    this.status = 0; // default detached
  }

  public void initial_attach() {
    Random rand = new Random();
    this.status = 2;  /* Active */                                this.status_ch = true;
    this.M_TMSI = rand.nextInt();                                 this.M_TMSI_ch = true;
    this.GUTI = GUMMEI + M_TMSI;                                  this.GUTI_ch = true;
    this.TIN = "";                                                this.TIN_ch = true;
    this.IP = rand.nextInt();                                     this.IP_ch = true;
    this.C_RNTI = (short) rand.nextInt(Short.MAX_VALUE - 1);      this.C_RNTI_ch = true;
    this.eNB_UE_S1AP = rand.nextInt();                            this.eNB_UE_S1AP_ch = true;
    this.MME_YE_S1AP = rand.nextInt();                            this.MME_YE_S1AP_ch = true;
    this.ECI = rand.nextInt();                                    this.ECI_ch = true;
    this.ECGI = PLMN_ID + ECI;                                    this.ECGI_ch = true;
    this.TAI = rand.nextInt();                                    this.TAI_ch = true;
    this.TAI_list = new ArrayList<>();                            this.TAI_list_ch = true;
    for (int i = 0; i < TAI_SIZE; i++) TAI_list.add(rand.nextInt());
    this.PDN_ID = rand.nextInt() + ".apn.epc.mnc" + MNC + ".mcc" + MCC + rand.nextInt() + "3gppnetwork.org";
                                                                  this.PDN_ID_ch = true;
    this.EPS_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);    this.EPS_bearer_ch = true;
    this.E_RA_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);   this.E_RA_bearer_ch = true;
    this.DR_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);     this.DR_bearer_ch = true;
    this.S1_TEID_UL = rand.nextInt();                             this.S1_TEID_UL_ch = true;
    this.S1_TEID_DL = rand.nextInt();                             this.S1_TEID_DL_ch = true;
    this.S5_TEID_UL = rand.nextInt();                             this.S5_TEID_UL_ch = true;
    this.S5_TEID_DL = rand.nextInt();                             this.S5_TEID_DL_ch = true;
    this.K_ASME = randomString(16, rand);                         this.K_ASME_ch = true;
    this.K_ENB = randomString(16, rand);                          this.K_ENB_ch = true;
    this.K_NASint = randomString(16, rand);                       this.K_NASint_ch = true;
    this.K_NASenc = randomString(16, rand);                       this.K_NASenc_ch = true;
    this.K_RRCint = randomString(16, rand);                       this.K_RRCint_ch = true;
    this.K_RRCenc = randomString(16, rand);                       this.K_RRCenc_ch = true;
    this.K_UPenc = randomString(16, rand);                        this.K_UPenc_ch = true;
    this.master_ch = true;
  }

  public void detach() {
    this.status = 0; /* Detached */                  this.status_ch = true;
    this.IP = 0;                                     this.IP_ch = true;
    this.C_RNTI = 0;                                 this.C_RNTI_ch = true;
    this.eNB_UE_S1AP = 0;                            this.eNB_UE_S1AP_ch = true;
    this.MME_YE_S1AP = 0;                            this.MME_YE_S1AP_ch = true;
    this.ECI = 0;                                    this.ECI_ch = true;
    this.ECGI = PLMN_ID + ECI;                       this.ECGI_ch = true;
    this.PDN_ID = "";                                this.PDN_ID_ch = true;
    this.EPS_bearer = 0;                             this.EPS_bearer_ch = true;
    this.E_RA_bearer = 0;                            this.E_RA_bearer_ch = true;
    this.DR_bearer = 0;                              this.DR_bearer_ch = true;
    this.S1_TEID_UL = 0;                             this.S1_TEID_UL_ch = true;
    this.S1_TEID_DL = 0;                             this.S1_TEID_DL_ch = true;
    this.S5_TEID_UL = 0;                             this.S5_TEID_UL_ch = true;
    this.S5_TEID_DL = 0;                             this.S5_TEID_DL_ch = true;
    this.K_ASME = "";                                this.K_ASME_ch = true;
    this.K_ENB = "";                                 this.K_ENB_ch = true;
    this.K_RRCint = "";                              this.K_RRCint_ch = true;
    this.K_RRCenc = "";                              this.K_RRCenc_ch = true;
    this.K_UPenc = "";                               this.K_UPenc_ch = true;
    this.master_ch = true;
  }

  public void S1_release() {
    this.status = 3; /* Idle */                      this.status_ch = true;
    this.C_RNTI = 0;                                 this.C_RNTI_ch = true;
    this.eNB_UE_S1AP = 0;                            this.eNB_UE_S1AP_ch = true;
    this.MME_YE_S1AP = 0;                            this.MME_YE_S1AP_ch = true;
    this.ECI = 0;                                    this.ECI_ch = true;
    this.ECGI = PLMN_ID + ECI;                       this.ECGI_ch = true;
    this.DR_bearer = 0;                              this.DR_bearer_ch = true;
    this.S1_TEID_DL = 0;                             this.S1_TEID_DL_ch = true;
    this.K_ENB = "";                                 this.K_ENB_ch = true;
    this.K_RRCint = "";                              this.K_RRCint_ch = true;
    this.K_RRCenc = "";                              this.K_RRCenc_ch = true;
    this.K_UPenc = "";                               this.K_UPenc_ch = true;
    this.master_ch = true;
  }

  public void service_request() {
    Random rand = new Random();
    this.status = 2; /* Active */                                 this.status_ch = true;
    this.C_RNTI = (short) rand.nextInt(Short.MAX_VALUE - 1);      this.C_RNTI_ch = true;
    this.eNB_UE_S1AP = rand.nextInt();                            this.eNB_UE_S1AP_ch = true;
    this.MME_YE_S1AP = rand.nextInt();                            this.MME_YE_S1AP_ch = true;
    this.ECI = rand.nextInt();                                    this.ECI_ch = true;
    this.ECGI = PLMN_ID + ECI;                                    this.ECGI_ch = true;
    this.DR_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);     this.DR_bearer_ch = true;
    this.S1_TEID_DL = rand.nextInt();                             this.S1_TEID_DL_ch = true;
    this.K_ENB = randomString(16, rand);                          this.K_ENB_ch = true;
    this.K_RRCint = randomString(16, rand);                       this.K_RRCint_ch = true;
    this.K_RRCenc = randomString(16, rand);                       this.K_RRCenc_ch = true;
    this.K_UPenc = randomString(16, rand);                        this.K_UPenc_ch = true;
    this.master_ch = true;
  }

  public void tracking_area_update() {
    Random rand = new Random();
    this.M_TMSI = rand.nextInt();           this.M_TMSI_ch = true;
    this.GUTI = GUMMEI + M_TMSI;            this.GUTI_ch = true;
    this.TIN = GUTI;                        this.TIN_ch = true;
    this.TAI = rand.nextInt();              this.TAI_ch = true;
    this.TAI_list = new ArrayList<>();      this.TAI_list_ch = true;
    for (int i = 0; i < TAI_SIZE; i++) { TAI_list.add(rand.nextInt()); }
    this.master_ch = true;
  }

  public void S1_handover() {

  }

  public void X2_handover() {

  }

  public void session_management() {

  }

  public void cell_reselect() {

  }

  public int getStatus() {
    return this.status;
  }

  public String getIMSI() { return this.IMSI; }

  private String randomString(int chars, Random rand) {
    String s = "";
    String hexa = "0123456789ABCDEF";
    for (int i = 0; i < chars; i++) {
      s += hexa.charAt(rand.nextInt(16));
    }
    return s;
  }

  @Override
  public boolean hasDelta() {
    return master_ch;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    out.writeBoolean(status_ch);                if (status_ch){ out.writeByte(status); this.status_ch = false; }
    out.writeBoolean(M_TMSI_ch);                if (M_TMSI_ch) { out.writeInt(M_TMSI); this.M_TMSI_ch = false; }
    out.writeBoolean(GUTI_ch);                  if (GUTI_ch) { out.writeUTF(GUTI); this.GUTI_ch = false; }
    out.writeBoolean(TIN_ch);                   if (TIN_ch) { out.writeUTF(TIN); this.TIN_ch = false; }
    out.writeBoolean(IP_ch);                    if (IP_ch) { out.writeInt(IP); this.IP_ch = false; }
    out.writeBoolean(C_RNTI_ch);                if (C_RNTI_ch) { out.writeShort(C_RNTI); this.C_RNTI_ch = false; }
    out.writeBoolean(eNB_UE_S1AP_ch);           if (eNB_UE_S1AP_ch) { out.writeInt(eNB_UE_S1AP); this.eNB_UE_S1AP_ch = false; }
    out.writeBoolean(MME_YE_S1AP_ch);           if (MME_YE_S1AP_ch) { out.writeInt(MME_YE_S1AP); this.MME_YE_S1AP_ch = false; }
    out.writeBoolean(OLD_eNB_UE_X2_ch);         if (OLD_eNB_UE_X2_ch) { out.writeInt(OLD_eNB_UE_X2); this.OLD_eNB_UE_X2_ch = false; }
    out.writeBoolean(NEW_eNB_UE_X2_ch);         if (NEW_eNB_UE_X2_ch) { out.writeInt(NEW_eNB_UE_X2); this.NEW_eNB_UE_X2_ch = false; }
    out.writeBoolean(ECI_ch);                   if (ECI_ch) { out.writeInt(ECI); this.ECI_ch = false; }
    out.writeBoolean(ECGI_ch);                  if (ECGI_ch) { out.writeUTF(ECGI); this.ECGI_ch = false; }
    out.writeBoolean(TAI_ch);                   if (TAI_ch) { out.writeInt(TAI); this.TAI_ch = false; }
    out.writeBoolean(TAI_list_ch);              if (TAI_list_ch) { out.writeInt(TAI_list.size()); for (int TAI:TAI_list) out.writeInt(TAI); this.TAI_list_ch = false; }
    out.writeBoolean(PDN_ID_ch);                if (PDN_ID_ch) { out.writeUTF(PDN_ID); this.PDN_ID_ch = false; }
    out.writeBoolean(EPS_bearer_ch);            if (EPS_bearer_ch) { out.writeByte(EPS_bearer); this.EPS_bearer_ch = false; }
    out.writeBoolean(E_RA_bearer_ch);           if (E_RA_bearer_ch) { out.writeByte(E_RA_bearer); this.E_RA_bearer_ch = false; }
    out.writeBoolean(DR_bearer_ch);             if (DR_bearer_ch) { out.writeByte(DR_bearer); this.DR_bearer_ch = false; }
    out.writeBoolean(S1_TEID_DL_ch);            if (S1_TEID_DL_ch) { out.writeInt(S1_TEID_DL); this.S1_TEID_DL_ch = false; }
    out.writeBoolean(S1_TEID_UL_ch);            if (S1_TEID_UL_ch) { out.writeInt(S1_TEID_UL); this.S1_TEID_UL_ch = false; }
    out.writeBoolean(S5_TEID_DL_ch);            if (S5_TEID_DL_ch) { out.writeInt(S5_TEID_DL); this.S5_TEID_DL_ch = false; }
    out.writeBoolean(S5_TEID_UL_ch);            if (S5_TEID_UL_ch) { out.writeInt(S5_TEID_UL); this.S5_TEID_UL_ch = false; }
    out.writeBoolean(K_ASME_ch);                if (K_ASME_ch) { out.writeUTF(K_ASME); this.K_ASME_ch = false; }
    out.writeBoolean(K_ENB_ch);                 if (K_ENB_ch) { out.writeUTF(K_ENB); this.K_ENB_ch = false; }
    out.writeBoolean(K_NASint_ch);              if (K_NASint_ch) { out.writeUTF(K_NASint); this.K_NASint_ch = false; }
    out.writeBoolean(K_NASenc_ch);              if (K_NASenc_ch) { out.writeUTF(K_NASenc); this.K_NASenc_ch = false; }
    out.writeBoolean(K_RRCint_ch);              if (K_RRCint_ch) { out.writeUTF(K_RRCint); this.K_RRCint_ch = false; }
    out.writeBoolean(K_RRCenc_ch);              if (K_RRCenc_ch) { out.writeUTF(K_RRCenc); this.K_RRCenc_ch = false; }
    out.writeBoolean(K_UPenc_ch);               if (K_UPenc_ch) { out.writeUTF(K_UPenc); this.K_UPenc_ch = false; }
    this.master_ch = false;
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    if (in.readBoolean()) this.status = in.readByte();
    if (in.readBoolean()) this.M_TMSI = in.readInt();
    if (in.readBoolean()) this.GUTI = in.readUTF();
    if (in.readBoolean()) this.TIN = in.readUTF();
    if (in.readBoolean()) this.IP = in.readInt();
    if (in.readBoolean()) this.C_RNTI = in.readShort();
    if (in.readBoolean()) this.eNB_UE_S1AP = in.readInt();
    if (in.readBoolean()) this.MME_YE_S1AP = in.readInt();
    if (in.readBoolean()) this.OLD_eNB_UE_X2 = in.readInt();
    if (in.readBoolean()) this.NEW_eNB_UE_X2 = in.readInt();
    if (in.readBoolean()) this.ECI = in.readInt();
    if (in.readBoolean()) this.ECGI = in.readUTF();
    if (in.readBoolean()) this.TAI = in.readInt();
    if (in.readBoolean()) {
      int size = in.readInt();
      this.TAI_list = new ArrayList<>();
      for (int i = 0; i < size; i++) this.TAI_list.add(in.readInt());
    }
    if (in.readBoolean()) this.PDN_ID = in.readUTF();
    if (in.readBoolean()) this.EPS_bearer = in.readByte();
    if (in.readBoolean()) this.E_RA_bearer = in.readByte();
    if (in.readBoolean()) this.DR_bearer = in.readByte();
    if (in.readBoolean()) this.S1_TEID_DL = in.readInt();
    if (in.readBoolean()) this.S1_TEID_UL = in.readInt();
    if (in.readBoolean()) this.S5_TEID_DL = in.readInt();
    if (in.readBoolean()) this.S5_TEID_UL = in.readInt();
    if (in.readBoolean()) this.K_ASME = in.readUTF();
    if (in.readBoolean()) this.K_ENB = in.readUTF();
    if (in.readBoolean()) this.K_NASint = in.readUTF();
    if (in.readBoolean()) this.K_NASenc = in.readUTF();
    if (in.readBoolean()) this.K_RRCint = in.readUTF();
    if (in.readBoolean()) this.K_RRCenc = in.readUTF();
    if (in.readBoolean()) this.K_UPenc = in.readUTF();
  }

}
