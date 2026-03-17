import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Modified TspFrame that runs 1 local solver, 1 outsourcer, and 2 remote workers.
 */
public class TspFrame extends JFrame implements PropertyChangeListener {

  private static final String MQTT_BROKER = "tcp://broker.hivemq.com:1883";
  private static final int LOCAL_SOLVER_COUNT = 1;
  private static final int REMOTE_WORKER_COUNT = 0;
  private static boolean remoteOnly = false;

  private final TspProblemRepository repository = TspProblemRepository.getInstance();
  private final MapPanel mapPanel = new MapPanel();
  private final JTextArea log = new JTextArea(8, 60);
  private final JLabel statusLabel = new JLabel("Ready");

  private List<City> currentCities = List.of();
  private final List<Thread> workerThreads = new ArrayList<>();
  private final List<TspSolver> localSolvers = new ArrayList<>();
  private final List<RemoteWorker> remoteWorkers = new ArrayList<>();
  private Outsourcer outsourcer;

  public TspFrame() {
    super("TSP — Local + Remote Workers");
    buildUI();
    startWorkers();
    repository.addPropertyChangeListener(this);
  }

  private void buildUI() {
    log.setEditable(false);
    log.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
    log.setBackground(new Color(245, 255, 245));

    JButton loadBtn = new JButton("Load .tsp");
    JButton solveBtn = new JButton("Nearest Neighbor");
    JButton clearBtn = new JButton("Clear tour");
    loadBtn.addActionListener(e -> onLoad());
    solveBtn.addActionListener(e -> onSolve());
    clearBtn.addActionListener(e -> onClear());

    JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
    top.add(loadBtn);
    top.add(solveBtn);
    top.add(clearBtn);
    top.add(Box.createHorizontalStrut(20));
    top.add(statusLabel);

    setLayout(new BorderLayout());
    add(top, BorderLayout.NORTH);
    add(mapPanel, BorderLayout.CENTER);
    add(new JScrollPane(log), BorderLayout.SOUTH);

    appendLog("Ready — starting 1 local worker, 1 outsourcer, and 2 remote workers.");
    setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    addWindowListener(new java.awt.event.WindowAdapter() {
      @Override
      public void windowClosing(java.awt.event.WindowEvent e) {
        stopWorkers();
      }
    });
    setSize(900, 700);
    setLocationRelativeTo(null);
  }

  private void startWorkers() {
    for (int i = 0; i < LOCAL_SOLVER_COUNT; i++) {
      TspSolver solver = new TspSolver(repository);
      Thread thread = new Thread(solver, "TspSolver-" + i);
      thread.setDaemon(true);
      thread.start();
      localSolvers.add(solver);
      workerThreads.add(thread);
      appendLog("Started local worker thread: " + thread.getName());
      System.out.println("[TspFrame] Started local worker thread: " + thread.getName());
    }

    if (!remoteOnly) {
      try {
        outsourcer = new Outsourcer(repository, MQTT_BROKER, "Outsourcer-1");
        Thread thread = new Thread(outsourcer, "Outsourcer-1");
        thread.setDaemon(true);
        thread.start();
        workerThreads.add(thread);
        appendLog("Started outsourcer thread: " + thread.getName());
        System.out.println("[TspFrame] Started outsourcer thread: " + thread.getName());
      } catch (Exception e) {
        appendLog("ERROR starting outsourcer: " + e.getMessage());
        System.err.println("[TspFrame] ERROR starting outsourcer: " + e.getMessage());
      }
    }

    for (int i = 0; i < REMOTE_WORKER_COUNT; i++) {
      try {
        RemoteWorker worker = new RemoteWorker(MQTT_BROKER, "RemoteWorker-" + (i + 1));
        Thread thread = new Thread(worker, "RemoteWorker-" + (i + 1));
        thread.setDaemon(true);
        thread.start();
        remoteWorkers.add(worker);
        workerThreads.add(thread);
        appendLog("Started remote worker thread: " + thread.getName());
        System.out.println("[TspFrame] Started remote worker thread: " + thread.getName());
      } catch (Exception e) {
        appendLog("ERROR starting remote worker " + (i + 1) + ": " + e.getMessage());
        System.err.println("[TspFrame] ERROR starting remote worker " + (i + 1) + ": " + e.getMessage());
      }
    }
  }

  private void stopWorkers() {
    for (TspSolver solver : localSolvers) {
      solver.stop();
    }
    if (outsourcer != null) {
      outsourcer.stop();
      try {
        outsourcer.close();
      } catch (Exception ignored) {
      }
    }
    for (RemoteWorker worker : remoteWorkers) {
      worker.stop();
      try {
        worker.close();
      } catch (Exception ignored) {
      }
    }
    for (Thread thread : workerThreads) {
      thread.interrupt();
    }
    System.out.println("[TspFrame] All worker threads asked to stop.");
  }

  private void onLoad() {
    JFileChooser chooser = new JFileChooser();
    chooser.setDialogTitle("Select a TSPLIB .tsp file");
    chooser.setMultiSelectionEnabled(true);
    chooser.setFileFilter(new FileNameExtensionFilter("TSPLIB (*.tsp)", "tsp"));
    if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;

    for (File file : chooser.getSelectedFiles()) {
      appendLog("Enqueuing: " + file.getName() + " ...");
      statusLabel.setText("Loading...");
      Thread thread = new Thread(new TspProducer(file, repository), "TspProducer-" + file.getName());
      thread.setDaemon(true);
      thread.start();
    }
  }

  private void onSolve() {
    if (currentCities.isEmpty()) {
      appendLog("Load a file first.");
      return;
    }

    List<City> snapshot = currentCities;
    Thread thread = new Thread(() -> {
      try {
        repository.produce(new TspProblem("manual", snapshot));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }, "TspProducer-manual");
    thread.setDaemon(true);
    thread.start();
    appendLog("Manually enqueued current cities for solving...");
    statusLabel.setText("Solving...");
  }

  private void onClear() {
    mapPanel.setTour(List.of());
    appendLog("Tour cleared.");
  }

  @Override
  public void propertyChange(PropertyChangeEvent evt) {
    switch (evt.getPropertyName()) {
      case TspProblemRepository.EVENT_PROBLEM -> {
        TspProblem problem = (TspProblem) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          currentCities = problem.getCities();
          mapPanel.setCities(problem.getCities());
          appendLog("Queued: " + problem);
          statusLabel.setText("Solving...");
        });
      }

      case TspProblemRepository.EVENT_RESULT -> {
        TspResult result = (TspResult) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          currentCities = result.getProblem().getCities();
          mapPanel.setCities(result.getProblem().getCities());
          mapPanel.setTour(result.getTour());
          appendLog("Solved: " + result);
          statusLabel.setText("Done — " + result.getProblem().getName());
        });
      }

      case TspProblemRepository.EVENT_ERROR -> {
        String message = (String) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          appendLog("ERROR: " + message);
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
