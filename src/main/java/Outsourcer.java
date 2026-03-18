import org.eclipse.paho.client.mqttv3.*;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Outsourcer: listens for worker task requests and fulfils them by dequeuing
 * tasks from the repository and publishing directly to each worker's personal
 * topic.
 *
 * <h3>Protocol</h3>
 * Workers publish to {@code tsp/<session>/requests} saying how many tasks they
 * want
 * and which topic to deliver them to. The Outsourcer dequeues that many tasks
 * and
 * publishes each one directly to the worker's personal topic. Results come back
 * on
 * {@code tsp/<session>/results} as before.
 *
 * <h3>Local solver competition</h3>
 * The Outsourcer still competes with local TspSolver threads for tasks from the
 * shared queue. Each task the Outsourcer wins goes to a remote worker; tasks
 * local
 * solvers win are solved in-process. Timeout fallback still applies per-task.
 *
 * @author javiergs / ryanschmitt
 * @version 8.0
 */
public class Outsourcer implements Runnable, MqttCallback, AutoCloseable {

  private static final long REMOTE_TIMEOUT_MS = 10_000;

  private final TspProblemRepository repository;
  private final String brokerUrl;
  private final String outsourcerId;
  private final String sessionId;
  private final String sessionTag;
  private final String requestTopic;
  private final String resultTopic;
  private final MqttClient client;
  private volatile boolean running = true;

  private final LinkedBlockingQueue<WorkerRequest> pendingRequests = new LinkedBlockingQueue<>();

  private final ConcurrentHashMap<String, InFlightTask> inFlight = new ConcurrentHashMap<>();

  private final ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "Outsourcer-Timeout");
    t.setDaemon(true);
    return t;
  });

  public Outsourcer(TspProblemRepository repository, String brokerUrl,
      String outsourcerId, String sessionId) throws MqttException {
    this.repository = repository;
    this.brokerUrl = brokerUrl;
    this.outsourcerId = outsourcerId;
    this.sessionId = sessionId;
    this.sessionTag = sessionId.length() >= 8 ? sessionId.substring(0, 8) : sessionId;
    this.requestTopic = "tsp/" + sessionId + "/requests";
    this.resultTopic = "tsp/" + sessionId + "/results";
    this.client = new MqttClient(brokerUrl, outsourcerId);
    this.client.setCallback(this);
  }

  @Override
  public void run() {
    try {
      MqttConnectOptions options = new MqttConnectOptions();
      options.setAutomaticReconnect(true);
      options.setCleanSession(true);
      client.connect(options);
      client.subscribe(requestTopic, 1);
      client.subscribe(resultTopic, 1);

      System.out.println("[Outsourcer] Session  : " + sessionId);
      System.out.println("[Outsourcer] Broker   : " + brokerUrl);
      System.out.println("[Outsourcer] Requests : " + requestTopic);
      System.out.println("[Outsourcer] Results  : " + resultTopic);
      System.out.println("[Outsourcer] Timeout  : " + REMOTE_TIMEOUT_MS + " ms");

      // Block waiting for a worker request, then fulfil it
      while (running && !Thread.currentThread().isInterrupted()) {
        WorkerRequest req = pendingRequests.poll(500, TimeUnit.MILLISECONDS);
        if (req == null)
          continue;

        System.out.printf("[Outsourcer] Fulfilling request from %s — %d tasks → %s%n",
            req.workerId, req.capacity, req.taskTopic);

        for (int i = 0; i < req.capacity; i++) {
          if (!running)
            break;

          TspProblem problem = repository.getNextProblem();
          String requestId = UUID.randomUUID().toString();

          InFlightTask inFlightTask = new InFlightTask(problem, requestId, req.workerId);
          inFlight.put(requestId, inFlightTask);

          inFlightTask.timeoutFuture = timeoutScheduler.schedule(
              () -> handleTimeout(requestId), REMOTE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

          String payload = encodeWorkItem(requestId, problem);
          MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
          msg.setQos(1);
          client.publish(req.taskTopic, msg);

          System.out.printf("[Submit]   (remote) Outsourcer   [%s]  problem=%s  start=%d  → %s%n",
              sessionTag, problem.getName(), problem.getStartIndex(), req.workerId);
        }
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println("[Outsourcer] Interrupted — stopping.");
    } catch (Exception e) {
      repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null,
          "Outsourcer failed: " + e.getMessage());
      System.err.println("[Outsourcer] Failure: " + e.getMessage());
      e.printStackTrace();
    } finally {
      try {
        close();
      } catch (Exception ignored) {
      }
      System.out.println("[Outsourcer] Stopped.");
    }
  }

  private void handleTimeout(String requestId) {
    InFlightTask task = inFlight.remove(requestId);
    if (task == null)
      return;

    System.out.printf("[Fallback] (remote) Outsourcer   [%s]  problem=%s  start=%d  — timeout, queuing locally%n",
        sessionTag, task.problem.getName(), task.problem.getStartIndex());

    try {
      repository.requeueLocally(task.problem);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void stop() {
    running = false;
  }

  @Override
  public void connectionLost(Throwable cause) {
    String msg = "Outsourcer connection lost: " + (cause == null ? "unknown" : cause.getMessage());
    repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null, msg);
    System.err.println("[Outsourcer] " + msg);
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) {
    try {
      String payload = new String(message.getPayload(), StandardCharsets.UTF_8);

      if (topic.equals(requestTopic)) {
        // Awaiting task
        WorkerRequest req = decodeRequest(payload);
        System.out.printf("[Outsourcer] Request received from %s — wants %d tasks%n",
            req.workerId, req.capacity);
        pendingRequests.add(req);

      } else if (topic.equals(resultTopic)) {
        // Finished task
        DecodedResult decoded = decodeResult(payload);

        InFlightTask task = inFlight.remove(decoded.requestId);
        if (task == null) {
          System.out.printf("[Discard]  (remote) %-12s  [%s]  problem=%s  start=%d  — late arrival%n",
              decoded.workerId, sessionTag, decoded.problem.getName(), decoded.startIndex);
          return;
        }

        if (task.timeoutFuture != null)
          task.timeoutFuture.cancel(false);

        System.out.printf("[Solved]   (remote) %-12s  [%s]  problem=%s  start=%d  length=%.3f%n",
            decoded.workerId, sessionTag,
            decoded.problem.getName(), decoded.startIndex, decoded.length);

        repository.submitResult(new TspResult(decoded.problem, decoded.tour,
            decoded.length, decoded.startIndex));
      }

    } catch (Exception e) {
      repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null,
          "Outsourcer message error: " + e.getMessage());
      System.err.println("[Outsourcer] Message error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
    /* no-op */ }

  @Override
  public void close() throws MqttException {
    running = false;
    timeoutScheduler.shutdownNow();
    if (client.isConnected())
      client.disconnect();
    client.close();
  }

  private static String encodeWorkItem(String requestId, TspProblem problem) {
    StringBuilder sb = new StringBuilder();
    sb.append("requestId=").append(escape(requestId)).append('\n');
    sb.append("name=").append(escape(problem.getName())).append('\n');
    sb.append("startIndex=").append(problem.getStartIndex()).append('\n');
    sb.append("cityCount=").append(problem.getCities().size()).append('\n');
    for (City city : problem.getCities()) {
      sb.append(city.getId()).append('|')
          .append(city.getX()).append('|')
          .append(city.getY()).append('\n');
    }
    return sb.toString();
  }

  private static WorkerRequest decodeRequest(String payload) {
    String[] lines = payload.split("\\R");
    String workerId = unescape(valueOf(lines[0], "workerId="));
    int capacity = Integer.parseInt(valueOf(lines[1], "capacity="));
    String taskTopic = unescape(valueOf(lines[2], "taskTopic="));
    return new WorkerRequest(workerId, capacity, taskTopic);
  }

  private static DecodedResult decodeResult(String payload) {
    String[] lines = payload.split("\\R");
    String requestId = unescape(valueOf(lines[0], "requestId="));
    String workerId = unescape(valueOf(lines[1], "workerId="));
    String name = unescape(valueOf(lines[2], "name="));
    double length = Double.parseDouble(valueOf(lines[3], "length="));
    int startIndex = Integer.parseInt(valueOf(lines[4], "startIndex="));
    int cityCount = Integer.parseInt(valueOf(lines[5], "cityCount="));

    List<City> cities = new ArrayList<>();
    int idx = 6;
    for (int i = 0; i < cityCount; i++, idx++) {
      String[] parts = lines[idx].split("\\|");
      cities.add(new City(
          Integer.parseInt(parts[0]),
          Double.parseDouble(parts[1]),
          Double.parseDouble(parts[2])));
    }

    List<Integer> tour = new ArrayList<>();
    String tourLine = valueOf(lines[idx], "tour=");
    if (!tourLine.isBlank()) {
      for (String part : tourLine.split(","))
        tour.add(Integer.parseInt(part.trim()));
    }

    return new DecodedResult(requestId, workerId,
        new TspProblem(name, cities, startIndex), tour, length, startIndex);
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

  private static class WorkerRequest {
    final String workerId;
    final int capacity;
    final String taskTopic;

    WorkerRequest(String workerId, int capacity, String taskTopic) {
      this.workerId = workerId;
      this.capacity = capacity;
      this.taskTopic = taskTopic;
    }
  }

  private static class InFlightTask {
    final TspProblem problem;
    final String requestId;
    final String workerId;
    volatile ScheduledFuture<?> timeoutFuture;

    InFlightTask(TspProblem problem, String requestId, String workerId) {
      this.problem = problem;
      this.requestId = requestId;
      this.workerId = workerId;
    }
  }

  private static class DecodedResult {
    final String requestId, workerId;
    final TspProblem problem;
    final List<Integer> tour;
    final double length;
    final int startIndex;

    DecodedResult(String requestId, String workerId, TspProblem problem,
        List<Integer> tour, double length, int startIndex) {
      this.requestId = requestId;
      this.workerId = workerId;
      this.problem = problem;
      this.tour = tour;
      this.length = length;
      this.startIndex = startIndex;
    }
  }
}
