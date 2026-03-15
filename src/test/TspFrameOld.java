import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.util.ArrayList;
import java.util.List;


/**
 * A simple Swing GUI for loading a TSPLIB .tsp file,
 * displaying the cities, and computing a tour using the nearest neighbor heuristic.
 *
 * @author javiergs
 * @version 1.0
 */

/**
 *   Modified version of the TspFrame class from the original code.
 *   Modified to use a DelegatePanel instead of a MapPanel.
 *   Modified to use a TspProblemRepository instead of a TspProblem.
 */
public class TspFrameOld extends JFrame implements PropertyChangeListener {

  // How many solver (consumer) threads to run in parallel.
  private static final int SOLVER_COUNT = Runtime.getRuntime().availableProcessors();

  private final TspProblemRepository repository = TspProblemRepository.getInstance();
  private final MapPanel mapPanel = new MapPanel();
  private final JTextArea log = new JTextArea(8, 60);
  private final JLabel statusLabel = new JLabel("Ready");

  // Last loaded cities — needed so the solve button knows what to re-enqueue.
  private List<City> currentCities = List.of();

  // Track running solver threads so we can interrupt them on close.
  private final List<Thread> solverThreads = new ArrayList<>();

  public TspFrameOld() {
    super("TSP — Producer-Consumer");
    buildUI();
    startSolvers();

    // Subscribe to repository events (result, error, new problem enqueued).
    repository.addPropertyChangeListener(this);
  }

  // -------------------------------------------------------------------------
  // UI setup
  // -------------------------------------------------------------------------

  private void buildUI() {
    log.setEditable(false);
    log.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
    log.setBackground(new Color(245, 255, 245));

    JButton loadBtn  = new JButton("Load .tsp");
    JButton solveBtn = new JButton("Nearest Neighbor");
    JButton clearBtn = new JButton("Clear tour");
    loadBtn.addActionListener(e  -> onLoad());
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

    appendLog("Ready — " + SOLVER_COUNT + " solver thread(s) started.");
    setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    addWindowListener(new java.awt.event.WindowAdapter() {
      @Override public void windowClosing(java.awt.event.WindowEvent e) {
        stopSolvers();
      }
    });
    setSize(900, 700);
    setLocationRelativeTo(null);
  }

  // -------------------------------------------------------------------------
  // Producer-Consumer wiring
  // -------------------------------------------------------------------------

  /**
   * Starts {@link #SOLVER_COUNT} consumer threads. Each runs in an infinite
   * loop, blocking on the repository until a problem is available.
   */
  private void startSolvers() {
    for (int i = 0; i < SOLVER_COUNT; i++) {
      TspSolver solver = new TspSolver(repository);
      Thread t = new Thread(solver, "TspSolver-" + i);
      t.setDaemon(true); // Don't prevent JVM exit if the window is closed.
      t.start();
      solverThreads.add(t);
    }
    appendLog("Solver threads: " + SOLVER_COUNT + " (one per CPU core).");
  }

  private void stopSolvers() {
    solverThreads.forEach(Thread::interrupt);
  }

  /**
   * "Load .tsp" button: shows a file chooser, then spawns a {@link TspProducer}
   * thread. The EDT returns immediately — the producer does the I/O off-thread.
   */
  private void onLoad() {
    JFileChooser chooser = new JFileChooser();
    chooser.setDialogTitle("Select a TSPLIB .tsp file");
    chooser.setMultiSelectionEnabled(true);
    chooser.setFileFilter(new FileNameExtensionFilter("TSPLIB (*.tsp)", "tsp"));
    if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;

    for (File f : chooser.getSelectedFiles()) {
      appendLog("Enqueuing: " + f.getName() + " …");
      statusLabel.setText("Loading…");
      // Spawn one producer thread per selected file.
      Thread t = new Thread(new TspProducer(f, repository), "TspProducer-" + f.getName());
      t.setDaemon(true);
      t.start();
    }
  }

  /**
   * "Nearest Neighbor" button: re-enqueues the currently displayed cities as a
   * new problem. The existing consumer threads pick it up automatically — the
   * button is just a manual trigger into the same Producer-Consumer pipeline.
   * Disabled if no file has been loaded yet.
   */
  private void onSolve() {
    if (currentCities.isEmpty()) {
      appendLog("Load a file first.");
      return;
    }
    try {
      // Wrap the current cities in a new problem and push it into the buffer.
      // produce() will block if the buffer is full (capacity = 10), which is
      // fine here because we are on the EDT only briefly for the guard check —
      // but to be safe we do this on a short-lived thread.
      List<City> snapshot = currentCities; // capture before thread starts
      Thread t = new Thread(() -> {
        try {
          repository.produce(new TspProblem("manual", snapshot));
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }, "TspProducer-manual");
      t.setDaemon(true);
      t.start();
      appendLog("Manually enqueued current cities for solving…");
      statusLabel.setText("Solving…");
    } catch (Exception ex) {
      appendLog("ERROR: " + ex.getMessage());
    }
  }

  private void onClear() {
    mapPanel.setTour(List.of());
    appendLog("Tour cleared.");
  }

  // -------------------------------------------------------------------------
  // PropertyChangeListener — receives events from any thread, dispatches to EDT
  // -------------------------------------------------------------------------

  @Override
  public void propertyChange(PropertyChangeEvent evt) {
    switch (evt.getPropertyName()) {

      case TspProblemRepository.EVENT_PROBLEM -> {
        TspProblem p = (TspProblem) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          currentCities = p.getCities(); // available immediately for the solve button
          mapPanel.setCities(p.getCities());
          appendLog("Queued: " + p);
          statusLabel.setText("Solving…");
        });
      }

      case TspProblemRepository.EVENT_RESULT -> {
        TspResult r = (TspResult) evt.getNewValue();
        SwingUtilities.invokeLater(() -> {
          currentCities = r.getProblem().getCities(); // keep in sync for solve button
          mapPanel.setCities(r.getProblem().getCities());
          mapPanel.setTour(r.getTour());
          appendLog("Solved: " + r);
          statusLabel.setText("Done — " + r.getProblem().getName());
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

  private void appendLog(String msg) {
    log.append(msg + "\n");
    log.setCaretPosition(log.getDocument().getLength());
  }
}