import org.eclipse.paho.client.mqttv3.*;

import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Remote MQTT worker: subscribes to a wildcard task topic that matches every
 * session's tasks, solves each sub-problem, and publishes the result to the
 * per-session result topic embedded in the task payload.
 *
 * <h3>Session isolation</h3>
 * Workers subscribe to {@code $share/tsp-workers/tsp/+/tasks}, where {@code +}
 * is a single-level wildcard that matches any session UUID. Each task payload
 * contains a {@code resultTopic} field that tells the worker exactly where to
 * publish its answer (e.g. {@code tsp/abc-123/results}). This means:
 * <ul>
 *   <li>Workers automatically handle tasks from any session without reconfiguration.</li>
 *   <li>If the orchestrator was restarted, its old session UUID is gone; results
 *       published to the old {@code resultTopic} have no subscriber and are
 *       silently dropped — they never pollute the new run.</li>
 * </ul>
 *
 * <h3>Unique worker IDs</h3>
 * {@code hostname + "-" + UUID} guarantees global uniqueness with zero config.
 *
 * <h3>Running standalone</h3>
 * <pre>
 *   java -cp .:paho-mqtt.jar RemoteWorker [brokerUrl] [workerCount]
 *
 *   Examples:
 *     java -cp .:paho-mqtt.jar RemoteWorker
 *     java -cp .:paho-mqtt.jar RemoteWorker tcp://broker.hivemq.com:1883 4
 *     java -cp .:paho-mqtt.jar RemoteWorker tcp://192.168.1.10:1883 2
 * </pre>
 *
 * @author javiergs / ryanschmitt
 * @version 4.0
 */
public class RemoteWorker implements Runnable, MqttCallback, AutoCloseable {

  // Wildcard subscription — catches tasks from any session UUID.
  // The shared-subscription prefix ensures each message goes to exactly one worker.
  public static final String TASK_SUBSCRIPTION = "$share/tsp-workers/tsp/+/tasks";

  private static final String DEFAULT_BROKER = "tcp://broker.hivemq.com:1883";

  private final String brokerUrl;
  private final String workerId;
  private final MqttClient client;
  private volatile boolean running = true;

  // -------------------------------------------------------------------------
  // Constructor
  // -------------------------------------------------------------------------

  public RemoteWorker(String brokerUrl, String workerId) throws MqttException {
    this.brokerUrl = brokerUrl;
    this.workerId  = workerId;
    this.client    = new MqttClient(brokerUrl, workerId);
    this.client.setCallback(this);
  }

  /** Builds a globally unique worker ID: {@code hostname-UUID}. */
  public static String generateUniqueId() {
    String host;
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      host = "unknown-host";
    }
    return host + "-" + UUID.randomUUID();
  }

  // -------------------------------------------------------------------------
  // Runnable
  // -------------------------------------------------------------------------

  @Override
  public void run() {
    try {
      MqttConnectOptions options = new MqttConnectOptions();
      options.setAutomaticReconnect(true);
      options.setCleanSession(true);

      client.connect(options);
      client.subscribe(TASK_SUBSCRIPTION, 1);

      System.out.println("[" + workerId + "] Connected to broker    : " + brokerUrl);
      System.out.println("[" + workerId + "] Subscribed to tasks    : " + TASK_SUBSCRIPTION);
      System.out.println("[" + workerId + "] Result topic read from task payload (per-session)");
      System.out.println("[" + workerId + "] Ready — waiting for tasks...");

      // Keep alive — work is done in messageArrived()
      while (running && client.isConnected()) {
        try { Thread.sleep(1000); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
      }

    } catch (MqttException e) {
      System.err.println("[" + workerId + "] Failed to start: " + e.getMessage());
      e.printStackTrace();
    } finally {
      try { close(); } catch (Exception ignored) { }
      System.out.println("[" + workerId + "] Stopped.");
    }
  }

  public void stop() { running = false; }

  // -------------------------------------------------------------------------
  // MqttCallback
  // -------------------------------------------------------------------------

  @Override
  public void connectionLost(Throwable cause) {
    System.err.println("[" + workerId + "] Connection lost: " +
        (cause == null ? "unknown" : cause.getMessage()));
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) {
    try {
      String payload    = new String(message.getPayload(), StandardCharsets.UTF_8);
      WorkItem workItem = decodeWorkItem(payload);

      System.out.printf("[RECEIVED] worker=%s  requestId=%s  problem=%s  start=%d  cities=%d  replyTo=%s%n",
          workerId, workItem.requestId, workItem.problem.getName(),
          workItem.problem.getStartIndex(), workItem.problem.getCities().size(),
          workItem.resultTopic);

      // Solve from the designated starting city
      List<Integer> tour = NearestNeighborSolver.solve(
          workItem.problem.getCities(), workItem.problem.getStartIndex());
      double length = NearestNeighborSolver.length(workItem.problem.getCities(), tour);

      System.out.printf("[SOLVED]   worker=%s  requestId=%s  problem=%s  start=%d  length=%.3f%n",
          workerId, workItem.requestId, workItem.problem.getName(),
          workItem.problem.getStartIndex(), length);

      // Publish result to the session-specific result topic from the task payload
      String resultPayload = encodeResult(workItem.requestId, workerId,
                                          workItem.problem, tour, length);
      MqttMessage resultMsg = new MqttMessage(resultPayload.getBytes(StandardCharsets.UTF_8));
      resultMsg.setQos(1);
      client.publish(workItem.resultTopic, resultMsg);

      System.out.printf("[PUBLISHED] worker=%s  requestId=%s  topic=%s%n",
          workerId, workItem.requestId, workItem.resultTopic);

    } catch (Exception e) {
      System.err.println("[" + workerId + "] Error processing task: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) { /* no-op */ }

  @Override
  public void close() throws MqttException {
    running = false;
    if (client.isConnected()) client.disconnect();
    client.close();
  }

  // -------------------------------------------------------------------------
  // Wire encoding / decoding
  // -------------------------------------------------------------------------

  private static String encodeResult(String requestId, String workerId, TspProblem problem,
                                     List<Integer> tour, double length) {
    StringBuilder sb = new StringBuilder();
    sb.append("requestId=").append(escape(requestId)).append('\n');
    sb.append("workerId=").append(escape(workerId)).append('\n');
    sb.append("name=").append(escape(problem.getName())).append('\n');
    sb.append("length=").append(length).append('\n');
    sb.append("startIndex=").append(problem.getStartIndex()).append('\n');
    sb.append("cityCount=").append(problem.getCities().size()).append('\n');
    for (City city : problem.getCities()) {
      sb.append(city.getId()).append('|')
        .append(city.getX()).append('|')
        .append(city.getY()).append('\n');
    }
    sb.append("tour=");
    for (int i = 0; i < tour.size(); i++) {
      if (i > 0) sb.append(',');
      sb.append(tour.get(i));
    }
    sb.append('\n');
    return sb.toString();
  }

  private static WorkItem decodeWorkItem(String payload) {
    String[] lines    = payload.split("\\R");
    String requestId  = unescape(valueOf(lines[0], "requestId="));
    String name       = unescape(valueOf(lines[1], "name="));
    int startIndex    = Integer.parseInt(valueOf(lines[2], "startIndex="));
    String resultTopic = unescape(valueOf(lines[3], "resultTopic="));
    int cityCount     = Integer.parseInt(valueOf(lines[4], "cityCount="));

    List<City> cities = new ArrayList<>();
    int idx = 5;
    for (int i = 0; i < cityCount; i++, idx++) {
      String[] parts = lines[idx].split("\\|");
      cities.add(new City(
          Integer.parseInt(parts[0]),
          Double.parseDouble(parts[1]),
          Double.parseDouble(parts[2])));
    }
    return new WorkItem(requestId, new TspProblem(name, cities, startIndex), resultTopic);
  }

  private static String valueOf(String line, String prefix) {
    if (!line.startsWith(prefix))
      throw new IllegalArgumentException("Expected '" + prefix + "' in: " + line);
    return line.substring(prefix.length());
  }

  private static String escape(String v)   { return URLEncoder.encode(v, StandardCharsets.UTF_8); }
  private static String unescape(String v) { return URLDecoder.decode(v, StandardCharsets.UTF_8); }

  // -------------------------------------------------------------------------
  // Inner type
  // -------------------------------------------------------------------------

  private static class WorkItem {
    final String requestId;
    final TspProblem problem;
    final String resultTopic;  // session-specific, read from task payload

    WorkItem(String requestId, TspProblem problem, String resultTopic) {
      this.requestId   = requestId;
      this.problem     = problem;
      this.resultTopic = resultTopic;
    }
  }

  // -------------------------------------------------------------------------
  // Standalone entry point
  // -------------------------------------------------------------------------

  /**
   * Launch one or more remote workers from any machine.
   *
   * <pre>
   *   Usage: java -cp .:paho-mqtt.jar RemoteWorker [brokerUrl] [workerCount]
   *
   *   brokerUrl   — MQTT broker URI          (default: tcp://broker.hivemq.com:1883)
   *   workerCount — parallel worker threads  (default: 1)
   * </pre>
   */
  public static void main(String[] args) throws Exception {
    String brokerUrl   = args.length > 0 ? args[0] : DEFAULT_BROKER;
    int    workerCount = args.length > 1 ? Integer.parseInt(args[1]) : 1;

    System.out.println("╔══════════════════════════════════════╗");
    System.out.println("║       TSP Distributed RemoteWorker   ║");
    System.out.println("╠══════════════════════════════════════╣");
    System.out.printf( "║  Broker      : %-22s ║%n", brokerUrl);
    System.out.printf( "║  Workers     : %-22d ║%n", workerCount);
    System.out.printf( "║  Subscribing : %-22s ║%n", TASK_SUBSCRIPTION);
    System.out.println("╚══════════════════════════════════════╝");

    for (int i = 0; i < workerCount; i++) {
      String id = generateUniqueId();
      RemoteWorker worker = new RemoteWorker(brokerUrl, id);
      Thread t = new Thread(worker, "RemoteWorker-" + i);
      t.setDaemon(false);  // keep JVM alive
      t.start();
      System.out.println("[main] Started worker: " + id);
    }
  }
}
