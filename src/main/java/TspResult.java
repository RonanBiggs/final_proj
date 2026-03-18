import java.util.List;

public class TspResult {

  private final TspProblem problem;
  private final List<Integer> tour;
  private final double length;
  private final int startIndex;

  public TspResult(TspProblem problem, List<Integer> tour, double length, int startIndex) {
    this.problem = problem;
    this.tour = List.copyOf(tour);
    this.length = length;
    this.startIndex = startIndex;
  }

  public TspResult(TspProblem problem, List<Integer> tour, double length) {
    this(problem, tour, length, 0);
  }

  public TspProblem getProblem() {
    return problem;
  }

  public List<Integer> getTour() {
    return tour;
  }

  public double getLength() {
    return length;
  }

  public int getStartIndex() {
    return startIndex;
  }

  @Override
  public String toString() {
    return problem.getName()
        + " (start=" + startIndex + ")"
        + " — length: " + String.format("%.3f", length);
  }
}