import org.eclipse.paho.client.mqttv3.*;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Consumer/delegator thread that pulls work from the repository and sends it to remote workers.
 */
public class Outsourcer implements Runnable, MqttCallback, AutoCloseable {

  public static final String TASK_TOPIC = "tsp/tasks";
  public static final String RESULT_TOPIC = "tsp/results";

  private final TspProblemRepository repository;
  private final String brokerUrl;
  private final String outsourcerId;
  private final MqttClient client;
  private volatile boolean running = true;

  public Outsourcer(TspProblemRepository repository, String brokerUrl, String outsourcerId) throws MqttException {
    this.repository = repository;
    this.brokerUrl = brokerUrl;
    this.outsourcerId = outsourcerId;
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
      client.subscribe(RESULT_TOPIC, 1);
      System.out.println("[" + outsourcerId + "] Connected to broker: " + brokerUrl);
      System.out.println("[" + outsourcerId + "] Listening for remote results on " + RESULT_TOPIC);

      while (running && !Thread.currentThread().isInterrupted()) {
        TspProblem problem = repository.getNextProblem();
        String requestId = UUID.randomUUID().toString();

        System.out.println("[" + outsourcerId + "] Sending task to remote workers: " + problem.getName()
            + " (" + problem.getCities().size() + " cities)");

        String payload = encodeWorkItem(requestId, problem);
        MqttMessage message = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        client.publish(TASK_TOPIC, message);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println("[" + outsourcerId + "] Outsourcer interrupted and stopping.");
    } catch (Exception e) {
      repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null,
          "Outsourcer failed: " + e.getMessage());
      System.err.println("[" + outsourcerId + "] Outsourcer failure: " + e.getMessage());
      e.printStackTrace();
    } finally {
      try {
        close();
      } catch (Exception ignored) {
      }
      System.out.println("[" + outsourcerId + "] Outsourcer stopped.");
    }
  }

  public void stop() {
    running = false;
  }

  @Override
  public void connectionLost(Throwable cause) {
    String message = "Outsourcer connection lost: " + (cause == null ? "unknown" : cause.getMessage());
    repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null, message);
    System.err.println("[" + outsourcerId + "] " + message);
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) {
    try {
      String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
      DecodedResult decoded = decodeResult(payload);
      TspResult result = new TspResult(decoded.problem, decoded.tour, decoded.length);
      repository.firePropertyChange(TspProblemRepository.EVENT_RESULT, null, result);

      System.out.println("[" + outsourcerId + "] Received completed remote result from " + decoded.workerId
          + ": " + decoded.problem.getName() + " | length=" + String.format("%.3f", decoded.length));
    } catch (Exception e) {
      repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null,
          "Failed to decode remote result: " + e.getMessage());
      System.err.println("[" + outsourcerId + "] Failed to decode remote result: " + e.getMessage());
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

  private static String encodeWorkItem(String requestId, TspProblem problem) {
    StringBuilder sb = new StringBuilder();
    sb.append("requestId=").append(escape(requestId)).append('\n');
    sb.append("name=").append(escape(problem.getName())).append('\n');
    sb.append("cityCount=").append(problem.getCities().size()).append('\n');

    for (City city : problem.getCities()) {
      sb.append(city.getId()).append('|')
          .append(city.getX()).append('|')
          .append(city.getY()).append('\n');
    }
    return sb.toString();
  }

  private static DecodedResult decodeResult(String payload) {
    String[] lines = payload.split("\\R");
    String requestId = unescape(valueOf(lines[0], "requestId="));
    String workerId = unescape(valueOf(lines[1], "workerId="));
    String name = unescape(valueOf(lines[2], "name="));
    double length = Double.parseDouble(valueOf(lines[3], "length="));
    int cityCount = Integer.parseInt(valueOf(lines[4], "cityCount="));

    List<City> cities = new ArrayList<>();
    int lineIndex = 5;
    for (int i = 0; i < cityCount; i++, lineIndex++) {
      String[] parts = lines[lineIndex].split("\\|");
      cities.add(new City(
          Integer.parseInt(parts[0]),
          Double.parseDouble(parts[1]),
          Double.parseDouble(parts[2])
      ));
    }

    List<Integer> tour = new ArrayList<>();
    String tourLine = valueOf(lines[lineIndex], "tour=");
    if (!tourLine.isBlank()) {
      String[] parts = tourLine.split(",");
      for (String part : parts) {
        tour.add(Integer.parseInt(part.trim()));
      }
    }

    return new DecodedResult(requestId, workerId, new TspProblem(name, cities), tour, length);
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

  private static class DecodedResult {
    final String requestId;
    final String workerId;
    final TspProblem problem;
    final List<Integer> tour;
    final double length;

    DecodedResult(String requestId, String workerId, TspProblem problem, List<Integer> tour, double length) {
      this.requestId = requestId;
      this.workerId = workerId;
      this.problem = problem;
      this.tour = tour;
      this.length = length;
    }
  }
}
