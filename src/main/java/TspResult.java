import java.util.List;

/**
 * Carries the result of solving a TSP problem: the problem itself,
 * the computed tour (as city indices), the total tour length, and
 * the starting city index used.
 *
 * Instances are produced by {@link TspSolver} / {@link RemoteWorker}
 * and fired as a PropertyChangeEvent on the repository so the GUI
 * can update itself without solver threads ever touching Swing directly.
 *
 * @author ryanschmitt
 * @version 2.0
 */
public class TspResult {

  private final TspProblem problem;
  private final List<Integer> tour;
  private final double length;
  private final int startIndex;

  public TspResult(TspProblem problem, List<Integer> tour, double length, int startIndex) {
    this.problem    = problem;
    this.tour       = List.copyOf(tour);
    this.length     = length;
    this.startIndex = startIndex;
  }

  /** Legacy constructor — startIndex defaults to 0. */
  public TspResult(TspProblem problem, List<Integer> tour, double length) {
    this(problem, tour, length, 0);
  }

  public TspProblem getProblem()  { return problem; }
  public List<Integer> getTour()  { return tour; }
  public double getLength()       { return length; }
  public int getStartIndex()      { return startIndex; }

  @Override
  public String toString() {
    return problem.getName()
        + " (start=" + startIndex + ")"
        + " — length: " + String.format("%.3f", length);
  }
}