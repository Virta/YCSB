package com.yahoo.ycsb.workloads;

import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

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

  /**
   * property name of the port where Geode server is listening for connections.
   */
  private static final String SERVERPORT_PROPERTY_NAME = "geode.serverport";

  /**
   * property name of the host where Geode server is running.
   */
  private static final String SERVERHOST_PROPERTY_NAME = "geode.serverhost";

  /**
   * default value of {@link #SERVERHOST_PROPERTY_NAME}.
   */
  private static final String SERVERHOST_PROPERTY_DEFAULT = "localhost";

  /**
   * property name to specify a Geode locator. This property can be used in both
   * client server and p2p topology
   */
  private static final String LOCATOR_PROPERTY_NAME = "geode.locator";

  /**
   * property name to specify Geode topology.
   */
  private static final String TOPOLOGY_PROPERTY_NAME = "geode.topology";

  /**
   * value of {@value #TOPOLOGY_PROPERTY_NAME} when peer to peer topology should be used.
   * (client-server topology is default)
   */
  private static final String TOPOLOGY_P2P_VALUE = "p2p";

  private GemFireCache cache;

  /**
   * true if ycsb client runs as a client to a Geode cache server.
   */
  private boolean isClient;
  /**
   * Keep the region object at hand so it does not have to be created each time, saves on time.
   */
  private Region<String, UE> ueRegion;
  /**
   * Keep the ueIDs as a list, so it can be indexed fast, used for choosing the next UE at random.
   */
  private List<String> ueIDsAsList;
  private Random random;

  FileWriter fw;
  BufferedWriter bw;
  PrintWriter out;

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
    random = new Random();
    random.setSeed(System.currentTimeMillis());
    try{
      fw = new FileWriter("outfilename", true);
      bw = new BufferedWriter(fw);
      out = new PrintWriter(bw))
    } catch (IOException e) {
      //exception handling left as an exercise for the reader
    }
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
    while (ueRegion.get(ue.getIMSI()) != null) ue = new UE();
    Status status;
    int numOfRetries = 0;
    do {
      ueRegion.putIfAbsent(ue.getIMSI(), ue);
      out.println(ue.getIMSI());
      status = Status.OK;
      //more codestatus = Status.OK; // TODO: check if we can use the return value from the put above.
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
    if (ueIDsAsList == null) {
      getRegionKeyData();
    }

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
        getRegionKeyData();
        break;
      default:
        doDetach();
    }
    return true;
  }

  private void getRegionKeyData() {
    QueryService serv = cache.getQueryService();
    Query q = serv.newQuery("SELECT key FROM /" + table + ".entrySet");
    List<String> results = null;
    try {
      results = ((SelectResults<String>) q.execute()).asList();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    if (results != null) {
      ueIDsAsList = results;
    } else {
      System.out.println("Could not initialise ueID list from query results");
      System.exit(1);
    }
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
    _measurements.measure(SESSION_MANAGEMENT_OPERATION, (int) (end - start));
    _measurements.measureIntended(SESSION_MANAGEMENT_OPERATION, (int) (end - start));
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
    _measurements.measure(CELL_RESELECT_OPERATION, (int) (end - start));
    _measurements.measureIntended(CELL_RESELECT_OPERATION, (int) (end - start));
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
    _measurements.measure(HANDOVER_OPERATION, (int) (end - start));
    _measurements.measureIntended(HANDOVER_OPERATION, (int) (end - start));
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
    _measurements.measure(TAU_OPERATION, (int) (end - start));
    _measurements.measureIntended(TAU_OPERATION, (int) (end - start));
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
    _measurements.measure(S1_RELEASE_OPERATION, (int) (end - start));
    _measurements.measureIntended(S1_RELEASE_OPERATION, (int) (end - start));
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
    _measurements.measure(SERVICE_REQUEST_OPERATION, (int) (end - start));
    _measurements.measureIntended(SERVICE_REQUEST_OPERATION, (int) (end - start));
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
    _measurements.measure(DETACH_OPERATION, (int) (end - start));
    _measurements.measureIntended(DETACH_OPERATION, (int) (end - start));
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
    _measurements.measure(ATTACH_OPERATION, (int) (end - start));
    _measurements.measureIntended(ATTACH_OPERATION, (int) (end - start));
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
    if (sessionmanagementproportion > 0)
      operationchooser.addValue(sessionmanagementproportion, SESSION_MANAGEMENT_OPERATION);
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