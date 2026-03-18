import org.eclipse.paho.client.mqttv3.*;

import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteWorker implements Runnable, MqttCallback, AutoCloseable {

  private static final int TASK_BATCH_SIZE = 4;

  private static final String DEFAULT_BROKER = "tcp://broker.hivemq.com:1883";
  private static final String DEFAULT_SESSION_ID = "dolphin27";
  // --------------------------------------------------------------------------

  private static final AtomicInteger NEXT_ID = new AtomicInteger(1);

  private final String brokerUrl;
  private final String sessionId;
  private final String label; // e.g. "Remote-1"
  private final String mqttId;
  private final int batchSize;
  private final String requestTopic; // tsp/<session>/requests
  private final String myTaskTopic; // tsp/<session>/worker/<label>/tasks
  private final String resultTopic; // tsp/<session>/results
  private final MqttClient client;
  private volatile boolean running = true;

  private final AtomicInteger pendingInBatch = new AtomicInteger(0);

  public RemoteWorker(String brokerUrl, String sessionId, String label,
      String mqttId, int batchSize) throws MqttException {
    this.brokerUrl = brokerUrl;
    this.sessionId = sessionId;
    this.label = label;
    this.mqttId = mqttId;
    this.batchSize = batchSize;
    this.requestTopic = "tsp/" + sessionId + "/requests";
    this.myTaskTopic = "tsp/" + sessionId + "/worker/" + label + "/tasks";
    this.resultTopic = "tsp/" + sessionId + "/results";
    this.client = new MqttClient(brokerUrl, mqttId);
    this.client.setCallback(this);
  }

  public static RemoteWorker create(String brokerUrl, String sessionId,
      int batchSize) throws MqttException {
    String host;
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      host = "host";
    }
    String label = "Remote-" + NEXT_ID.getAndIncrement();
    String mqttId = host + "-" + UUID.randomUUID();
    return new RemoteWorker(brokerUrl, sessionId, label, mqttId, batchSize);
  }

  @Override
  public void run() {
    try {
      MqttConnectOptions options = new MqttConnectOptions();
      options.setAutomaticReconnect(true);
      options.setCleanSession(true);
      client.connect(options);

      client.subscribe(myTaskTopic, 1);

      System.out.printf("[%s] Connected to %s%n", label, brokerUrl);
      System.out.printf("[%s] Session    : %s%n", label, sessionId);
      System.out.printf("[%s] Task topic : %s%n", label, myTaskTopic);
      System.out.printf("[%s] Batch size : %d%n", label, batchSize);

      publishRequest();

      while (running && client.isConnected()) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }

    } catch (MqttException e) {
      System.err.printf("[%s] Failed to start: %s%n", label, e.getMessage());
      e.printStackTrace();
    } finally {
      try {
        close();
      } catch (Exception ignored) {
      }
      System.out.printf("[%s] Stopped.%n", label);
    }
  }

  private void publishRequest() throws MqttException {
    String payload = "workerId=" + escape(label) + "\n"
        + "capacity=" + batchSize + "\n"
        + "taskTopic=" + escape(myTaskTopic) + "\n";
    MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
    msg.setQos(1);
    client.publish(requestTopic, msg);
    System.out.printf("[%s] Requested %d tasks from [%s]%n",
        label, batchSize, sessionId);
  }

  public void stop() {
    running = false;
  }

  @Override
  public void connectionLost(Throwable cause) {
    System.err.printf("[%s] Connection lost: %s%n", label,
        cause == null ? "unknown" : cause.getMessage());
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) {
    try {
      String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
      WorkItem workItem = decodeWorkItem(payload);
      pendingInBatch.incrementAndGet();

      System.out.printf("[Receive]  (remote) %-12s  [%s]  problem=%s  start=%d%n",
          label, sessionId, workItem.problem.getName(), workItem.problem.getStartIndex());

      List<Integer> tour = NearestNeighborSolver.solve(
          workItem.problem.getCities(), workItem.problem.getStartIndex());
      double length = NearestNeighborSolver.length(workItem.problem.getCities(), tour);

      System.out.printf("[Solving]  (remote) %-12s  [%s]  problem=%s  start=%d%n",
          label, sessionId, workItem.problem.getName(), workItem.problem.getStartIndex());

      String resultPayload = encodeResult(workItem.requestId, label,
          workItem.problem, tour, length);
      MqttMessage resultMsg = new MqttMessage(resultPayload.getBytes(StandardCharsets.UTF_8));
      resultMsg.setQos(1);
      client.publish(resultTopic, resultMsg);

      System.out.printf("[Submit]   (remote) %-12s  [%s]  problem=%s  start=%d  length=%.3f%n",
          label, sessionId, workItem.problem.getName(), workItem.problem.getStartIndex(), length);

      if (pendingInBatch.decrementAndGet() == 0) {
        System.out.printf("[%s] Batch complete — requesting next %d tasks%n", label, batchSize);
        publishRequest();
      }

    } catch (Exception e) {
      System.err.printf("[%s] Error processing task: %s%n", label, e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
    /* no-op */ }

  @Override
  public void close() throws MqttException {
    running = false;
    if (client.isConnected())
      client.disconnect();
    client.close();
  }

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
      if (i > 0)
        sb.append(',');
      sb.append(tour.get(i));
    }
    sb.append('\n');
    return sb.toString();
  }

  private static WorkItem decodeWorkItem(String payload) {
    String[] lines = payload.split("\\R");
    String requestId = unescape(valueOf(lines[0], "requestId="));
    String name = unescape(valueOf(lines[1], "name="));
    int startIndex = Integer.parseInt(valueOf(lines[2], "startIndex="));
    int cityCount = Integer.parseInt(valueOf(lines[3], "cityCount="));

    List<City> cities = new ArrayList<>();
    int idx = 4;
    for (int i = 0; i < cityCount; i++, idx++) {
      String[] parts = lines[idx].split("\\|");
      cities.add(new City(
          Integer.parseInt(parts[0]),
          Double.parseDouble(parts[1]),
          Double.parseDouble(parts[2])));
    }
    return new WorkItem(requestId, new TspProblem(name, cities, startIndex));
  }

  private static String valueOf(String line, String prefix) {
    if (!line.startsWith(prefix))
      throw new IllegalArgumentException("Expected '" + prefix + "' in: " + line);
    return line.substring(prefix.length());
  }

  private static String escape(String v) {
    return URLEncoder.encode(v, StandardCharsets.UTF_8);
  }

  private static String unescape(String v) {
    return URLDecoder.decode(v, StandardCharsets.UTF_8);
  }

  private static class WorkItem {
    final String requestId;
    final TspProblem problem;

    WorkItem(String requestId, TspProblem problem) {
      this.requestId = requestId;
      this.problem = problem;
    }
  }

  public static void main(String[] args) throws Exception {
    String brokerUrl = DEFAULT_BROKER;
    String sessionId = "dophin27";
    int batchSize = 4;
    int workerCount = 11;

    System.out.printf("TSP RemoteWorker  broker=%s  session=%s  batchSize=%d  workers=%d%n",
        brokerUrl, sessionId, batchSize, workerCount);

    for (int i = 0; i < workerCount; i++) {
      RemoteWorker worker = RemoteWorker.create(brokerUrl, sessionId, batchSize);
      Thread t = new Thread(worker, worker.label);
      t.setDaemon(false);
      t.start();
    }
  }
}
