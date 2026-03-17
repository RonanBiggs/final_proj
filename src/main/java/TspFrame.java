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
 * <h3>Architecture</h3>
 * <ul>
 *   <li>LOCAL_SOLVER_COUNT local {@link TspSolver} threads compete with
 *       remote workers for sub-problems.</li>
 *   <li>One {@link Outsourcer} dequeues sub-problems and publishes them to
 *       remote workers over MQTT.  Any machine can run {@link RemoteWorker}
 *       to contribute CPU cycles.</li>
 *   <li>The repository fires {@link TspProblemRepository#EVENT_BEST_RESULT}
 *       every time a shorter tour is found; the map updates immediately.</li>
 * </ul>
 *
 * <h3>Connecting additional remote workers</h3>
 * On any machine with Java and the Paho library:
 * <pre>
 *   java -cp .:paho-mqtt.jar RemoteWorker [brokerUrl] [threadCount]
 * </pre>
 *
 * @author javiergs / ryanschmitt
 * @version 4.0
 */
public class TspFrame extends JFrame implements PropertyChangeListener {

  // ---- Configuration -------------------------------------------------------
  private static final String MQTT_BROKER        = "tcp://broker.hivemq.com:1883";
  private static final int    LOCAL_SOLVER_COUNT  = 2;  // parallel local threads
  private static final int    REMOTE_WORKER_COUNT = 3;  // in-process remote workers

  /**
   * Unique ID for this application run. Scopes all MQTT topics so tasks and
   * results from a previous (possibly crashed) run are on different topics and
   * never bleed into this session.
   */
  private static final String SESSION_ID = UUID.randomUUID().toString();

  // ---- Shared state --------------------------------------------------------
  private final TspProblemRepository repository = TspProblemRepository.getInstance();

  // ---- UI ------------------------------------------------------------------
  private final MapPanel  mapPanel     = new MapPanel();
  private final JTextArea log          = new JTextArea(10, 70);
  private final JLabel    statusLabel  = new JLabel("Ready");
  private final JLabel    progressLabel = new JLabel("");

  // ---- Worker handles ------------------------------------------------------
  private List<City> currentCities = List.of();
  private final List<Thread>       workerThreads = new ArrayList<>();
  private final List<TspSolver>    localSolvers  = new ArrayList<>();
  private final List<RemoteWorker> remoteWorkers = new ArrayList<>();
  private Outsourcer outsourcer;

  // ---- Progress tracking per-problem (EDT-only) ----------------------------
  private final Map<String, Integer> batchTotals    = new LinkedHashMap<>();
  private final Map<String, Integer> batchCompleted = new ConcurrentHashMap<>();

  // =========================================================================
  public TspFrame() {
    super("TSP Solver — Distributed Nearest Neighbor");
    buildUI();
    repository.addPropertyChangeListener(this);
    startWorkers();
  }

  // -------------------------------------------------------------------------
  // UI construction
  // -------------------------------------------------------------------------

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

    appendLog("Session ID : " + SESSION_ID);
    appendLog("Ready — "
        + LOCAL_SOLVER_COUNT + " local solver(s), 1 outsourcer, "
        + REMOTE_WORKER_COUNT + " in-process remote worker(s).");
    appendLog("To join as a remote worker from any machine:");
    appendLog("  java -cp .:paho-mqtt.jar RemoteWorker " + MQTT_BROKER + " <threadCount>");

    setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    addWindowListener(new java.awt.event.WindowAdapter() {
      @Override public void windowClosing(java.awt.event.WindowEvent e) { stopWorkers(); }
    });
    setSize(1000, 750);
    setLocationRelativeTo(null);
  }

  // -------------------------------------------------------------------------
  // Worker lifecycle
  // -------------------------------------------------------------------------

  private void startWorkers() {
    // Local solvers
    for (int i = 0; i < LOCAL_SOLVER_COUNT; i++) {
      TspSolver solver = new TspSolver(repository);
      Thread t = new Thread(solver, "TspSolver-" + i);
      t.setDaemon(true);
      t.start();
      localSolvers.add(solver);
      workerThreads.add(t);
      appendLog("Started local solver: " + t.getName());
    }

    // Outsourcer
    try {
      outsourcer = new Outsourcer(repository, MQTT_BROKER,
          "Outsourcer-" + UUID.randomUUID(), SESSION_ID);
      Thread t = new Thread(outsourcer, "Outsourcer");
      t.setDaemon(true);
      t.start();
      workerThreads.add(t);
      appendLog("Started outsourcer — session topics: tsp/" + SESSION_ID + "/{tasks,results}");
    } catch (Exception e) {
      appendLog("ERROR starting outsourcer: " + e.getMessage());
      System.err.println("[TspFrame] Outsourcer error: " + e.getMessage());
    }

    // Optional in-process remote workers
    for (int i = 0; i < REMOTE_WORKER_COUNT; i++) {
      try {
        String id = RemoteWorker.generateUniqueId();
        RemoteWorker worker = new RemoteWorker(MQTT_BROKER, id);
        Thread t = new Thread(worker, "RemoteWorker-" + i);
        t.setDaemon(true);
        t.start();
        remoteWorkers.add(worker);
        workerThreads.add(t);
        appendLog("Started in-process remote worker: " + id);
      } catch (Exception e) {
        appendLog("ERROR starting remote worker " + i + ": " + e.getMessage());
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

  // -------------------------------------------------------------------------
  // Button handlers
  // -------------------------------------------------------------------------

  private void onLoad() {
    JFileChooser chooser = new JFileChooser();
    chooser.setDialogTitle("Select one or more TSPLIB .tsp files");
    chooser.setMultiSelectionEnabled(true);
    chooser.setFileFilter(new FileNameExtensionFilter("TSPLIB (*.tsp)", "tsp"));
    if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;

    for (File file : chooser.getSelectedFiles()) {
      appendLog("Enqueuing all starting cities for: " + file.getName());
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

  // -------------------------------------------------------------------------
  // PropertyChangeListener — all repository events arrive here
  // -------------------------------------------------------------------------

  @Override
  public void propertyChange(PropertyChangeEvent evt) {
    switch (evt.getPropertyName()) {

      case TspProblemRepository.EVENT_BATCH -> {
        TspProblemRepository.BatchInfo info = (TspProblemRepository.BatchInfo) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          batchTotals.put(info.name, info.total);
          batchCompleted.put(info.name, 0);
          currentCities = List.of(); // will be set on first result
          appendLog(String.format("Batch registered: '%s' — %d sub-problems (one per starting city)",
              info.name, info.total));
          statusLabel.setText("Solving " + info.name + "…");
          progressLabel.setText("0 / " + info.total);
        });
      }

      case TspProblemRepository.EVENT_PROBLEM -> {
        // Just a sub-problem enqueued — no UI update needed (too noisy)
      }

      case TspProblemRepository.EVENT_RESULT -> {
        TspResult result = (TspResult) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          String name = result.getProblem().getName();
          int done = batchCompleted.merge(name, 1, Integer::sum);
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

          appendLog(String.format("★ NEW BEST  %s  start=%d  length=%.3f  (%d/%d done)",
              name, result.getStartIndex(), result.getLength(), done, total));
          statusLabel.setText(String.format("Best so far: %.3f", result.getLength()));

          if (total > 0 && done >= total) {
            appendLog(String.format("✔ All %d sub-problems solved. Best tour for '%s': %.3f",
                total, name, result.getLength()));
            statusLabel.setText(String.format("Done — best: %.3f", result.getLength()));
          }
        });
      }

      case TspProblemRepository.EVENT_ERROR -> {
        String msg = (String) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          appendLog("ERROR: " + msg);
          statusLabel.setText("Error");
        });
      }
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void appendLog(String msg) {
    log.append(msg + "\n");
    log.setCaretPosition(log.getDocument().getLength());
  }
}
