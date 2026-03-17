import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main application window.
 *
 * @author javiergs / ryanschmitt
 * @version 5.0
 */
public class TspFrame extends JFrame implements PropertyChangeListener {

  private static final String MQTT_BROKER            = "tcp://broker.hivemq.com:1883";
  private static final int    LOCAL_SOLVER_COUNT      = 10;
  private static final int    REMOTE_WORKER_COUNT     = 0;  // in-process remote workers
  /** How many tasks each in-process remote worker requests at a time. */
  private static final int    REMOTE_TASK_BATCH_SIZE  = 5;

  private static final String SESSION_ID  = "dolphin27";
  private static final String SESSION_TAG = SESSION_ID.length() >= 8
      ? SESSION_ID.substring(0, 8) : SESSION_ID;

  private final TspProblemRepository repository = TspProblemRepository.getInstance();

  private final MapPanel  mapPanel      = new MapPanel();
  private final JTextArea log           = new JTextArea(10, 70);
  private final JLabel    statusLabel   = new JLabel("Ready");
  private final JLabel    progressLabel = new JLabel("");

  private List<City> currentCities = List.of();
  private final List<Thread>       workerThreads = new ArrayList<>();
  private final List<TspSolver>    localSolvers  = new ArrayList<>();
  private final List<RemoteWorker> remoteWorkers = new ArrayList<>();
  private Outsourcer outsourcer;

  private final Map<String, Integer> batchTotals    = new LinkedHashMap<>();
  private final Map<String, Integer> batchCompleted = new ConcurrentHashMap<>();

  public TspFrame() {
    super("TSP Solver — Distributed Nearest Neighbor");
    buildUI();
    repository.addPropertyChangeListener(this);
    startWorkers();
  }

  private void buildUI() {
    log.setEditable(false);
    log.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 11));
    log.setBackground(new Color(245, 255, 245));

    JButton loadBtn  = new JButton("Load .tsp");
    JButton clearBtn = new JButton("Clear tour");
    loadBtn.addActionListener(e -> onLoad());
    clearBtn.addActionListener(e -> onClear());

    JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
    top.add(loadBtn);
    top.add(clearBtn);
    top.add(Box.createHorizontalStrut(16));
    top.add(statusLabel);
    top.add(Box.createHorizontalStrut(16));
    top.add(progressLabel);

    setLayout(new BorderLayout());
    add(top, BorderLayout.NORTH);
    add(mapPanel, BorderLayout.CENTER);
    add(new JScrollPane(log), BorderLayout.SOUTH);

    appendLog("Session  : " + SESSION_ID + "  [" + SESSION_TAG + "]");
    appendLog("Config   : " + LOCAL_SOLVER_COUNT + " local solver(s), 1 outsourcer, "
        + REMOTE_WORKER_COUNT + " in-process remote worker(s)");
    appendLog("Remote   : java -cp .:paho-mqtt.jar RemoteWorker " + MQTT_BROKER
        + " " + SESSION_ID + " <batchSize> <workerCount>");

    setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    addWindowListener(new java.awt.event.WindowAdapter() {
      @Override public void windowClosing(java.awt.event.WindowEvent e) { stopWorkers(); }
    });
    setSize(1000, 750);
    setLocationRelativeTo(null);
  }

  private void startWorkers() {
    for (int i = 0; i < LOCAL_SOLVER_COUNT; i++) {
      String label  = "Local-" + (i + 1);
      TspSolver solver = new TspSolver(repository, label, SESSION_TAG);
      Thread t = new Thread(solver, label);
      t.setDaemon(true);
      t.start();
      localSolvers.add(solver);
      workerThreads.add(t);
      appendLog("Started  : " + label);
    }

    try {
      outsourcer = new Outsourcer(repository, MQTT_BROKER,
          "Outsourcer-" + UUID.randomUUID(), SESSION_ID);
      Thread t = new Thread(outsourcer, "Outsourcer");
      t.setDaemon(true);
      t.start();
      workerThreads.add(t);
      appendLog("Started  : Outsourcer  [" + SESSION_TAG + "]");
    } catch (Exception e) {
      appendLog("ERROR starting outsourcer: " + e.getMessage());
      System.err.println("[TspFrame] Outsourcer error: " + e.getMessage());
    }

    for (int i = 0; i < REMOTE_WORKER_COUNT; i++) {
      try {
        RemoteWorker worker = RemoteWorker.create(MQTT_BROKER, SESSION_ID, REMOTE_TASK_BATCH_SIZE);
        Thread t = new Thread(worker, "RemoteWorker-" + i);
        t.setDaemon(true);
        t.start();
        remoteWorkers.add(worker);
        workerThreads.add(t);
        appendLog("Started  : Remote-" + (i + 1) + " (in-process)");
      } catch (Exception e) {
        appendLog("ERROR starting in-process remote worker: " + e.getMessage());
      }
    }
  }

  private void stopWorkers() {
    localSolvers.forEach(TspSolver::stop);
    if (outsourcer != null) {
      outsourcer.stop();
      try { outsourcer.close(); } catch (Exception ignored) { }
    }
    remoteWorkers.forEach(w -> {
      w.stop();
      try { w.close(); } catch (Exception ignored) { }
    });
    workerThreads.forEach(Thread::interrupt);
  }

  private void onLoad() {
    JFileChooser chooser = new JFileChooser();
    chooser.setDialogTitle("Select one or more TSPLIB .tsp files");
    chooser.setMultiSelectionEnabled(true);
    chooser.setFileFilter(new FileNameExtensionFilter("TSPLIB (*.tsp)", "tsp"));
    if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;

    for (File file : chooser.getSelectedFiles()) {
      appendLog("Loading  : " + file.getName());
      statusLabel.setText("Loading...");
      Thread t = new Thread(new TspProducer(file, repository), "TspProducer-" + file.getName());
      t.setDaemon(true);
      t.start();
    }
  }

  private void onClear() {
    mapPanel.setTour(List.of());
    progressLabel.setText("");
    appendLog("Tour cleared.");
  }

  @Override
  public void propertyChange(PropertyChangeEvent evt) {
    switch (evt.getPropertyName()) {

      case TspProblemRepository.EVENT_BATCH -> {
        TspProblemRepository.BatchInfo info = (TspProblemRepository.BatchInfo) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          batchTotals.put(info.name, info.total);
          batchCompleted.put(info.name, 0);
          currentCities = List.of();
          appendLog(String.format("Batch    : '%s'  %d sub-problems", info.name, info.total));
          statusLabel.setText("Solving " + info.name + "…");
          progressLabel.setText("0 / " + info.total);
        });
      }

      case TspProblemRepository.EVENT_PROBLEM -> { /* too noisy */ }

      case TspProblemRepository.EVENT_RESULT -> {
        TspResult result = (TspResult) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          String name = result.getProblem().getName();
          int done  = batchCompleted.merge(name, 1, Integer::sum);
          int total = batchTotals.getOrDefault(name, 0);
          progressLabel.setText(done + " / " + total);
        });
      }

      case TspProblemRepository.EVENT_BEST_RESULT -> {
        TspResult result = (TspResult) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          currentCities = result.getProblem().getCities();
          mapPanel.setCities(result.getProblem().getCities());
          mapPanel.setTour(result.getTour());

          String name  = result.getProblem().getName();
          int done     = batchCompleted.getOrDefault(name, 0);
          int total    = batchTotals.getOrDefault(name, 0);

          appendLog(String.format("★ Best   : %s  start=%d  length=%.3f  (%d/%d)",
              name, result.getStartIndex(), result.getLength(), done, total));
          statusLabel.setText(String.format("Best: %.3f", result.getLength()));

          if (total > 0 && done >= total) {
            appendLog(String.format("✔ Done   : '%s'  best=%.3f  (%d sub-problems)",
                name, result.getLength(), total));
            statusLabel.setText(String.format("Done — %.3f", result.getLength()));
          }
        });
      }

      case TspProblemRepository.EVENT_ERROR -> {
        String msg = (String) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          appendLog("ERROR    : " + msg);
          statusLabel.setText("Error");
        });
      }
    }
  }

  private void appendLog(String msg) {
    log.append(msg + "\n");
    log.setCaretPosition(log.getDocument().getLength());
  }
}
