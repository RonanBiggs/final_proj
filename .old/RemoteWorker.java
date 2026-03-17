import org.eclipse.paho.client.mqttv3.*;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Remote MQTT worker that performs the same solve logic as TspSolver.
 */
public class RemoteWorker implements Runnable, MqttCallback, AutoCloseable {

  public static final String TASK_TOPIC = "$share/tsp-workers/tsp/tasks";
  public static final String RESULT_TOPIC = "tsp/results";

  private final String brokerUrl;
  private final String workerId;
  private final MqttClient client;
  private volatile boolean running = true;

  public RemoteWorker(String brokerUrl, String workerId) throws MqttException {
    this.brokerUrl = brokerUrl;
    this.workerId = workerId;
    this.client = new MqttClient(brokerUrl, workerId);
    this.client.setCallback(this);
  }

  @Override
  public void run() {
    try {
      MqttConnectOptions options = new MqttConnectOptions();
      options.setAutomaticReconnect(true);
      options.setCleanSession(true);

      client.connect(options);
      client.subscribe(TASK_TOPIC, 1);
      System.out.println("[" + workerId + "] Connected to broker: " + brokerUrl);
      System.out.println("[" + workerId + "] Waiting for remote work on " + TASK_TOPIC);

      while (running && client.isConnected()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    } catch (MqttException e) {
      System.err.println("[" + workerId + "] Failed to start remote worker: " + e.getMessage());
      e.printStackTrace();
    } finally {
      try {
        close();
      } catch (Exception ignored) {
      }
      System.out.println("[" + workerId + "] Remote worker stopped.");
    }
  }

  public void stop() {
    running = false;
  }

  @Override
  public void connectionLost(Throwable cause) {
    System.err.println("[" + workerId + "] Connection lost: " +
        (cause == null ? "unknown" : cause.getMessage()));
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) {
    try {
      String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
      WorkItem workItem = decodeWorkItem(payload);

      System.out.println("[" + workerId + "] Received remote task: " + workItem.problem.getName()
          + " (" + workItem.problem.getCities().size() + " cities)");

      List<Integer> tour = NearestNeighborSolver.solve(workItem.problem.getCities(), 0);
      double length = NearestNeighborSolver.length(workItem.problem.getCities(), tour);

      String resultPayload = encodeResult(workItem.requestId, workerId, workItem.problem, tour, length);
      MqttMessage resultMessage = new MqttMessage(resultPayload.getBytes(StandardCharsets.UTF_8));
      resultMessage.setQos(1);
      client.publish(RESULT_TOPIC, resultMessage);

      System.out.println("[" + workerId + "] Completed remote task: " + workItem.problem.getName()
          + " | length=" + String.format("%.3f", length));
    } catch (Exception e) {
      System.err.println("[" + workerId + "] Failed to process task: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
    // no-op
  }

  @Override
  public void close() throws MqttException {
    running = false;
    if (client.isConnected()) {
      client.disconnect();
    }
    client.close();
  }

  private static String encodeResult(String requestId, String workerId, TspProblem problem,
                                     List<Integer> tour, double length) {
    StringBuilder sb = new StringBuilder();
    sb.append("requestId=").append(escape(requestId)).append('\n');
    sb.append("workerId=").append(escape(workerId)).append('\n');
    sb.append("name=").append(escape(problem.getName())).append('\n');
    sb.append("length=").append(length).append('\n');
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
    String[] lines = payload.split("\\R");
    String requestId = unescape(valueOf(lines[0], "requestId="));
    String name = unescape(valueOf(lines[1], "name="));
    int cityCount = Integer.parseInt(valueOf(lines[2], "cityCount="));

    List<City> cities = new ArrayList<>();
    int lineIndex = 3;
    for (int i = 0; i < cityCount; i++, lineIndex++) {
      String[] parts = lines[lineIndex].split("\\|");
      cities.add(new City(
          Integer.parseInt(parts[0]),
          Double.parseDouble(parts[1]),
          Double.parseDouble(parts[2])
      ));
    }

    return new WorkItem(requestId, new TspProblem(name, cities));
  }

  private static String valueOf(String line, String prefix) {
    if (!line.startsWith(prefix)) {
      throw new IllegalArgumentException("Expected " + prefix + " in line: " + line);
    }
    return line.substring(prefix.length());
  }

  private static String escape(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String unescape(String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  private static class WorkItem {
    final String requestId;
    final TspProblem problem;

    WorkItem(String requestId, TspProblem problem) {
      this.requestId = requestId;
      this.problem = problem;
    }
  }
}
