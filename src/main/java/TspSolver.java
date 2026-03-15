import java.util.List;

/**
 * Consumer thread: repeatedly dequeues TSP problems from the shared
 * {@link TspProblemRepository}, solves them with the Nearest Neighbor
 * heuristic, and fires the result as a PropertyChangeEvent.
 *
 * The consumer is gated by Semaphore B (filled slots) inside the repository:
 * it blocks on {@code getNextProblem()} when the buffer is empty, consuming
 * zero CPU while it waits. When a result is ready it fires a "result" property
 * change on the repository; {@link TspFrameOld} listens for this and updates the
 * UI on the EDT via SwingUtilities.invokeLater.
 *
 * Design note: this thread runs in an infinite loop with a volatile flag so
 * that the GUI can stop it cleanly (e.g. on window close) without using the
 * deprecated Thread.stop(). The loop will also exit naturally if interrupted.
 *
 * Multiple TspSolver instances can run concurrently — each will race to grab
 * the next problem from the buffer, giving true parallel solving.
 *
 * @author ryanschmitt
 * @version 1.0
 */
public class TspSolver implements Runnable {

  private final TspProblemRepository repository;
  private volatile boolean running = true;

  public TspSolver(TspProblemRepository repository) {
    this.repository = repository;
  }

  /** Ask the thread to stop after it finishes its current problem. */
  public void stop() {
    running = false;
  }

  @Override
  public void run() {
    while (running) {
      try {
        // 1. Acquire Semaphore B (blocks if buffer is empty)
        // 2. Lock, dequeue, unlock  — all inside getNextProblem()
        // 3. Semaphore A (empty slots) is released by getNextProblem()
        TspProblem problem = repository.getNextProblem();

        // Solve 
        List<Integer> tour = NearestNeighborSolver.solve(
            problem.getCities(), 0);
        double length = NearestNeighborSolver.length(
            problem.getCities(), tour);

        TspResult result = new TspResult(problem, tour, length);

        // Fire result back — TspFrame listens and calls SwingUtilities.invokeLater
        repository.firePropertyChange("result", null, result);

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}