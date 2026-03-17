import java.beans.PropertyChangeSupport;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// Note: TspResult and NearestNeighborSolver are intentionally NOT imported here.
// The repository is a pure buffer; solving is the consumer's (TspSolver's) job.

/**
 * Singleton buffer that holds TSP problems waiting to be solved.
 *
 * This class is the shared buffer in the Producer-Consumer pattern:
 * 
 *  {@link TspProducer} threads call {@link #produce} to enqueue work.</li>
 *  {@link TspSolver} threads call {@link #getNextProblem} to dequeue it.</li>
 * 
 *
 * <p>Thread safety is enforced by two semaphores and a reentrant lock,
 * mirroring the six-step semaphore dance from the lecture:
 * <ol>
 *   <li>Producer acquires {@code empty} (Semaphore A) — blocks if buffer full.</li>
 *   <li>Producer acquires the lock, enqueues, releases the lock.</li>
 *   <li>Producer releases {@code full} (Semaphore B) — signals consumer.</li>
 *   <li>Consumer acquires {@code full} (Semaphore B) — blocks if buffer empty.</li>
 *   <li>Consumer acquires the lock, dequeues, releases the lock.</li>
 *   <li>Consumer releases {@code empty} (Semaphore A) — signals producer.</li>
 * </ol>
 *
 * <p>This class also acts as the event bus for the whole application.
 * Threads fire named events here; the GUI subscribes and dispatches
 * UI updates to the EDT via {@code SwingUtilities.invokeLater}.
 * Known event property names are declared as public constants below.
 *
 * <h3>Changes from v1</h3>
 * <ul>
 *   <li>Added {@link #EVENT_RESULT} and {@link #EVENT_ERROR} constants so
 *       listeners can use string literals safely.</li>
 *   <li>Made {@link #firePropertyChange(String, Object, Object)} public so
 *       {@link TspProducer} and {@link TspSolver} can fire events without
 *       needing a separate event-bus reference.</li>
 *   <li>No changes to the buffer logic or semaphore protocol.</li>
 * </ul>
 *
 * @author ryanschmitt
 * @version 3.0
 */
public class TspProblemRepository extends PropertyChangeSupport {

  /** Fired by {@link TspSolver} when a problem has been solved. Value: {@link TspResult}. */
  public static final String EVENT_RESULT = "result";

  /** Fired by {@link TspProducer} when a file cannot be parsed. Value: error message String. */
  public static final String EVENT_ERROR = "error";

  /** Fired by {@link #produce} when a problem is enqueued. Value: {@link TspProblem}. */
  public static final String EVENT_PROBLEM = "problems";

  private static final int CAPACITY = 10;
  private static final TspProblemRepository instance = new TspProblemRepository();

  private final Queue<TspProblem> problems = new LinkedList<>();
  private final Semaphore empty = new Semaphore(CAPACITY); // Semaphore A: empty slots
  private final Semaphore full  = new Semaphore(0);        // Semaphore B: filled slots
  private final Lock lock = new ReentrantLock();

  private TspProblemRepository() {
    super(new Object());
  }

  public static TspProblemRepository getInstance() {
    return instance;
  }

  /**
   * Parses a .tsp file and enqueues it as a problem.
   *
   * @param file the TSPLIB file to load
   * @throws IOException          if the file cannot be parsed
   * @throws InterruptedException if the thread is interrupted while waiting for space
   */
  public void loadFromFile(File file) throws IOException, InterruptedException {
    String name = file.getName().replaceFirst("\\.tsp$", "");
    List<City> cities = TspParser.load(file);
    produce(new TspProblem(name, cities));
  }

  /**
   * Enqueues a pre-built problem. Blocks if the repository is at capacity.
   * (Step 1-3 of the semaphore protocol.)
   *
   * @throws InterruptedException if the thread is interrupted while waiting for space
   */
  public void produce(TspProblem problem) throws InterruptedException {
    empty.acquire();               // Step 1: wait for an empty slot (Semaphore A)
    lock.lock();                   // Step 2a: acquire buffer lock
    try {
      problems.add(problem);
      firePropertyChange(EVENT_PROBLEM, null, problem);
    } finally {
      lock.unlock();               // Step 2b: release buffer lock
    }
    full.release();                // Step 3: signal consumer (Semaphore B++)
  }

  /**
   * Dequeues and returns the next problem to solve. Blocks until one is available.
   * (Steps 4-6 of the semaphore protocol.)
   *
   * Note: solving is intentionally NOT done here. The repository is only a buffer —
   * keeping it free of business logic means it stays easy to test and reuse.
   * {@link TspSolver} does the heavy lifting after dequeuing.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public TspProblem getNextProblem() throws InterruptedException {
    full.acquire();                // Step 4: wait for a filled slot (Semaphore B)
    TspProblem problem;
    lock.lock();                   // Step 5a: acquire buffer lock
    try {
      problem = problems.remove();
    } finally {
      lock.unlock();               // Step 5b: release buffer lock
    }
    empty.release();               // Step 6: signal producer (Semaphore A++)
    return problem;
  }

  public int size() {
    lock.lock();
    try {
      return problems.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Exposed as public so producer/solver threads can fire events on the shared
   * repository without needing a separate event bus reference.
   */
  @Override
  public void firePropertyChange(String propertyName, Object oldValue, Object newValue) {
    super.firePropertyChange(propertyName, oldValue, newValue);
  }
}