import org.eclipse.paho.client.mqttv3.*;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Outsourcer thread: competes with local {@link TspSolver} threads for tasks
 * from the shared queue. For each task it wins, it publishes to MQTT and
 * registers an asynchronous timeout. Local solvers continue pulling tasks
 * independently — both work in parallel.
 *
 * <h3>Non-blocking publish loop</h3>
 * The Outsourcer never blocks waiting for a remote result. As soon as it
 * publishes a task it immediately dequeues the next one. Each in-flight task
 * has a {@link CompletableFuture} registered in {@code inFlight}. A shared
 * {@link ScheduledExecutorService} fires the timeout callback; if the future
 * hasn't completed by then, the task is moved to the fallback local queue.
 *
 * <h3>Race safety</h3>
 * If a result arrives after the timeout has already fired and requeued the
 * task locally, {@code messageArrived} finds no entry in {@code inFlight} and
 * logs {@code [DISCARDED]} — the result is harmlessly dropped.
 *
 * @author javiergs / ryanschmitt
 * @version 6.0
 */
public class Outsourcer implements Runnable, MqttCallback, AutoCloseable {

  /** How long to wait for a remote result before falling back to local. */
  private static final long REMOTE_TIMEOUT_MS = 10_000;

  public static final String WORKER_TASK_SUBSCRIPTION = "$share/tsp-workers/tsp/+/tasks";

  private final TspProblemRepository repository;
  private final String brokerUrl;
  private final String outsourcerId;
  private final String sessionId;
  private final String taskTopic;
  private final String resultTopic;
  private final MqttClient client;
  private volatile boolean running = true;

  /** In-flight tasks: requestId → future completed by messageArrived(). */
  private final ConcurrentHashMap<String, InFlightTask> inFlight = new ConcurrentHashMap<>();

  /** Single-thread scheduler for per-task timeouts — lightweight, one thread total. */
  private final ScheduledExecutorService timeoutScheduler =
      Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "Outsourcer-Timeout");
        t.setDaemon(true);
        return t;
      });

  public Outsourcer(TspProblemRepository repository, String brokerUrl,
                    String outsourcerId, String sessionId) throws MqttException {
    this.repository   = repository;
    this.brokerUrl    = brokerUrl;
    this.outsourcerId = outsourcerId;
    this.sessionId    = sessionId;
    this.taskTopic    = "tsp/" + sessionId + "/tasks";
    this.resultTopic  = "tsp/" + sessionId + "/results";
    this.client       = new MqttClient(brokerUrl, outsourcerId);
    this.client.setCallback(this);
  }

  @Override
  public void run() {
    try {
      MqttConnectOptions options = new MqttConnectOptions();
      options.setAutomaticReconnect(true);
      options.setCleanSession(true);
      client.connect(options);
      client.subscribe(resultTopic, 1);

      System.out.println("[Outsourcer] Connected to broker    : " + brokerUrl);
      System.out.println("[Outsourcer] Session ID             : " + sessionId);
      System.out.println("[Outsourcer] Publishing tasks to    : " + taskTopic);
      System.out.println("[Outsourcer] Listening for results  : " + resultTopic);
      System.out.println("[Outsourcer] Remote timeout         : " + REMOTE_TIMEOUT_MS + " ms");

      while (running && !Thread.currentThread().isInterrupted()) {
        // Compete with local solvers for the next task
        TspProblem problem   = repository.getNextProblem();
        String     requestId = UUID.randomUUID().toString();

        // Register BEFORE publishing so messageArrived() can never race ahead
        InFlightTask inFlightTask = new InFlightTask(problem, requestId);
        inFlight.put(requestId, inFlightTask);

        // Schedule the timeout callback (non-blocking)
        inFlightTask.timeoutFuture = timeoutScheduler.schedule(
            () -> handleTimeout(requestId), REMOTE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // Publish to MQTT — does not block
        String payload = encodeWorkItem(requestId, problem, resultTopic);
        MqttMessage msg = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        msg.setQos(1);
        client.publish(taskTopic, msg);

        System.out.printf("[SUBMITTED] (remote) requestId=%s  problem=%s  start=%d  topic=%s%n",
            requestId, problem.getName(), problem.getStartIndex(), taskTopic);
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
      try { close(); } catch (Exception ignored) { }
      System.out.println("[Outsourcer] Stopped.");
    }
  }

  /** Called by the timeout scheduler when a remote task takes too long. */
  private void handleTimeout(String requestId) {
    InFlightTask task = inFlight.remove(requestId);
    if (task == null) return; // already completed by messageArrived — nothing to do

    System.out.printf("[FALLBACK]  requestId=%s  problem=%s  start=%d  — timeout, queuing locally%n",
        requestId, task.problem.getName(), task.problem.getStartIndex());

    try {
      repository.requeueLocally(task.problem);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void stop() { running = false; }

  // -------------------------------------------------------------------------
  // MqttCallback
  // -------------------------------------------------------------------------

  @Override
  public void connectionLost(Throwable cause) {
    String msg = "Outsourcer connection lost: " + (cause == null ? "unknown" : cause.getMessage());
    repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null, msg);
    System.err.println("[Outsourcer] " + msg);
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) {
    try {
      String payload        = new String(message.getPayload(), StandardCharsets.UTF_8);
      DecodedResult decoded = decodeResult(payload);

      InFlightTask task = inFlight.remove(decoded.requestId);
      if (task == null) {
        // Timeout already fired and requeued locally — discard the late result
        System.out.printf("[DISCARDED] requestId=%s  — arrived after timeout, already handled locally%n",
            decoded.requestId);
        return;
      }

      // Cancel the pending timeout so it doesn't fire after we've already succeeded
      if (task.timeoutFuture != null) task.timeoutFuture.cancel(false);

      System.out.printf("[RECEIVED] (Remote)  requestId=%s  worker=%s  problem=%s  start=%d  length=%.3f%n",
          decoded.requestId, decoded.workerId,
          decoded.problem.getName(), decoded.startIndex, decoded.length);

      TspResult result = new TspResult(decoded.problem, decoded.tour,
                                        decoded.length, decoded.startIndex);
      repository.submitResult(result);

    } catch (Exception e) {
      repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null,
          "Failed to decode remote result: " + e.getMessage());
      System.err.println("[Outsourcer] Decode error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) { /* no-op */ }

  @Override
  public void close() throws MqttException {
    running = false;
    timeoutScheduler.shutdownNow();
    if (client.isConnected()) client.disconnect();
    client.close();
  }

  // -------------------------------------------------------------------------
  // Wire encoding / decoding
  // -------------------------------------------------------------------------

  private static String encodeWorkItem(String requestId, TspProblem problem, String resultTopic) {
    StringBuilder sb = new StringBuilder();
    sb.append("requestId=").append(escape(requestId)).append('\n');
    sb.append("name=").append(escape(problem.getName())).append('\n');
    sb.append("startIndex=").append(problem.getStartIndex()).append('\n');
    sb.append("resultTopic=").append(escape(resultTopic)).append('\n');
    sb.append("cityCount=").append(problem.getCities().size()).append('\n');
    for (City city : problem.getCities()) {
      sb.append(city.getId()).append('|')
        .append(city.getX()).append('|')
        .append(city.getY()).append('\n');
    }
    return sb.toString();
  }

  private static DecodedResult decodeResult(String payload) {
    String[] lines   = payload.split("\\R");
    String requestId = unescape(valueOf(lines[0], "requestId="));
    String workerId  = unescape(valueOf(lines[1], "workerId="));
    String name      = unescape(valueOf(lines[2], "name="));
    double length    = Double.parseDouble(valueOf(lines[3], "length="));
    int startIndex   = Integer.parseInt(valueOf(lines[4], "startIndex="));
    int cityCount    = Integer.parseInt(valueOf(lines[5], "cityCount="));

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

  private static String escape(String v)   { return URLEncoder.encode(v, StandardCharsets.UTF_8); }
  private static String unescape(String v) { return URLDecoder.decode(v, StandardCharsets.UTF_8); }

  // -------------------------------------------------------------------------
  // Inner types
  // -------------------------------------------------------------------------

  private static class InFlightTask {
    final TspProblem problem;
    final String requestId;
    volatile ScheduledFuture<?> timeoutFuture;

    InFlightTask(TspProblem problem, String requestId) {
      this.problem   = problem;
      this.requestId = requestId;
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
      this.requestId  = requestId;
      this.workerId   = workerId;
      this.problem    = problem;
      this.tour       = tour;
      this.length     = length;
      this.startIndex = startIndex;
    }
  }
}
