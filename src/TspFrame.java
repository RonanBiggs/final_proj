package tsp;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.io.File;
import java.util.List;

/**
 * A simple Swing GUI for loading a TSPLIB .tsp file,
 * displaying the cities, and computing a tour using the nearest neighbor heuristic.
 *
 * @author javiergs
 * @version 1.0
 */
public class TspFrame extends JFrame {

  private final MapPanel mapPanel = new MapPanel();
  private final JTextArea log = new JTextArea(8, 60);
  private List<City> cities = List.of();
  private List<Integer> tour = List.of();

  public TspFrame() {
    super("Demo (TSPLIB + Nearest Neighbor)");
    log.setEditable(false);
    log.setBackground(new Color(200, 255, 220));
    JButton loadBtn = new JButton("Load .tsp");
    JButton solveBtn = new JButton("Nearest Neighbor");
    JButton clearBtn = new JButton("Clear Tour");
    loadBtn.addActionListener(e -> onLoad());
    solveBtn.addActionListener(e -> onSolve());
    clearBtn.addActionListener(e -> onClear());
    JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
    top.add(loadBtn);
    top.add(solveBtn);
    top.add(clearBtn);
    setLayout(new BorderLayout());
    add(top, BorderLayout.NORTH);
    add(mapPanel, BorderLayout.CENTER);
    add(new JScrollPane(log), BorderLayout.SOUTH);
    log.append("Ready: Load a Waterloo TSPLIB file and draw cities.\n");
    setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    setSize(900, 650);
    setLocationRelativeTo(null);
  }

  private void onLoad() {
    JFileChooser chooser = new JFileChooser();
    chooser.setDialogTitle("Select a TSPLIB .tsp file");
    chooser.setFileFilter(new FileNameExtensionFilter("TSPLIB (*.tsp)", "tsp"));
    if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
    File f = chooser.getSelectedFile();
    try {
      cities = TspParser.load(f);
      tour = List.of();
      mapPanel.setCities(cities);
      log.append("\nLoaded: " + f.getAbsolutePath() + "\n");
      log.append("Cities: " + cities.size() + "\n");
    } catch (Exception ex) {
      log.append("\nERROR: " + ex.getMessage() + "\n");
    }
  }

  private void onSolve() {
    if (cities == null || cities.size() < 2) {
      log.append("\nLoad a file first.\n");
      return;
    }
    tour = NearestNeighborSolver.solve(cities, 0);
    double len = NearestNeighborSolver.length(cities, tour);
    mapPanel.setTour(tour);
    log.append("\nNearest-neighbor tour computed.\n");
    log.append("Tour length (Euclidean): " + String.format("%.3f", len) + "\n");
  }

  private void onClear() {
    tour = List.of();
    mapPanel.setTour(tour);
    log.append("\nTour cleared.\n");
  }

  public static void main(String[] args) {
    SwingUtilities.invokeLater(() -> new TspFrame().setVisible(true));
  }

}
