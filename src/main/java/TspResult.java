import java.util.List;

/**
 * Carries the result of solving a TSP problem: the problem itself,
 * the computed tour (as city indices), and the total tour length.
 *
 * Instances are produced by {@link TspSolver} and fired as a
 * PropertyChangeEvent on the repository so the GUI can update itself
 * without the solver thread ever touching Swing directly.
 *
 * @author ryanschmitt
 * @version 1.0
 */
public class TspResult {

  private final TspProblem problem;
  private final List<Integer> tour;
  private final double length;

  public TspResult(TspProblem problem, List<Integer> tour, double length) {
    this.problem = problem;
    this.tour    = List.copyOf(tour);
    this.length  = length;
  }

  public TspProblem getProblem() { return problem; }
  public List<Integer> getTour() { return tour; }
  public double getLength()      { return length; }

  @Override
  public String toString() {
    return problem.getName() + " — length: " + String.format("%.3f", length);
  }
}