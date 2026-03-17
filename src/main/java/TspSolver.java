import java.util.List;

/**
 * Local consumer thread: competes with the Outsourcer for tasks from the
 * shared queue, and also drains the fallback local queue for tasks that
 * timed out on remote workers.
 *
 * @author ryanschmitt
 * @version 5.0
 */
public class TspSolver implements Runnable {

  private final TspProblemRepository repository;
  private final String label;      // e.g. "Local-1"
  private final String sessionTag; // first 8 chars of session UUID
  private volatile boolean running = true;

  public TspSolver(TspProblemRepository repository, String label, String sessionTag) {
    this.repository = repository;
    this.label      = label;
    this.sessionTag = sessionTag;
  }

  public void stop() { running = false; }

  @Override
  public void run() {
    while (running) {
      try {
        TspProblem problem = nextProblem();
        if (problem == null) continue;

        System.out.printf("[Solving]  (local)  %-10s  [%s]  problem=%s  start=%d%n",
            label, sessionTag, problem.getName(), problem.getStartIndex());

        List<Integer> tour = NearestNeighborSolver.solve(
            problem.getCities(), problem.getStartIndex());
        double length = NearestNeighborSolver.length(problem.getCities(), tour);

        System.out.printf("[Solved]   (local)  %-10s  [%s]  problem=%s  start=%d  length=%.3f%n",
            label, sessionTag, problem.getName(), problem.getStartIndex(), length);

        repository.submitResult(new TspResult(problem, tour, length, problem.getStartIndex()));

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private TspProblem nextProblem() throws InterruptedException {
    if (repository.localQueueSize() > 0) {
      try { return repository.getNextLocalFallback(); }
      catch (Exception ignored) { }
    }
    return repository.getNextProblem();
  }
}
