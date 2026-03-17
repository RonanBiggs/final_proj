import java.util.List;

/**
 * Local consumer thread: competes with the Outsourcer for tasks from the
 * shared queue, and also drains the fallback local queue for tasks that
 * timed out on remote workers.
 *
 * <p>Both queues are serviced by a single loop. The thread tries the shared
 * queue first (blocking until a task is available), solves it locally, then
 * immediately checks whether any fallback tasks have piled up. This keeps
 * local CPU fully utilized regardless of how many remote workers are active.
 *
 * @author ryanschmitt
 * @version 4.0
 */
public class TspSolver implements Runnable {

  private final TspProblemRepository repository;
  private volatile boolean running = true;

  public TspSolver(TspProblemRepository repository) {
    this.repository = repository;
  }

  public void stop() { running = false; }

  @Override
  public void run() {
    while (running) {
      try {
        // Pull from whichever queue has work — shared queue first, then fallback.
        // getNextProblem() blocks until a task is available.
        TspProblem problem = nextProblem();
        if (problem == null) continue;

        System.out.printf("[SOLVING]   (local) problem=%s  start=%d  cities=%d%n",
            problem.getName(), problem.getStartIndex(), problem.getCities().size());

        List<Integer> tour = NearestNeighborSolver.solve(
            problem.getCities(), problem.getStartIndex());
        double length = NearestNeighborSolver.length(problem.getCities(), tour);

        System.out.printf("[SOLVED]    (local) problem=%s  start=%d  length=%.3f%n",
            problem.getName(), problem.getStartIndex(), length);

        repository.submitResult(new TspResult(problem, tour, length, problem.getStartIndex()));

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  /**
   * Returns the next problem to solve. Drains the fallback local queue
   * (non-blocking) first; if empty, blocks on the shared queue.
   * This means timed-out remote tasks are handled immediately, while
   * normal throughput comes from the shared queue.
   */
  private TspProblem nextProblem() throws InterruptedException {
    // Non-blocking check of fallback queue first
    if (repository.localQueueSize() > 0) {
      try {
        return repository.getNextLocalFallback();
      } catch (Exception ignored) {
        // Race: another solver grabbed it first — fall through to shared queue
      }
    }
    // Block on the shared queue (competes with the Outsourcer)
    return repository.getNextProblem();
  }
}
