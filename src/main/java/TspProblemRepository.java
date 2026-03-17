import java.beans.PropertyChangeSupport;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Singleton buffer holding TSP sub-problems in a single shared queue.
 *
 * <h3>Architecture</h3>
 * <pre>
 *   TspProducer ──► [sharedQueue] ──► TspSolver threads  (local, pull directly)
 *                        │
 *                        └──────────► Outsourcer          (also pulls, publishes to MQTT)
 *                                         │  result arrives
 *                                         ▼
 *                                   submitResult()  ◄── RemoteWorker (via MQTT)
 * </pre>
 *
 * Both local solvers and the Outsourcer compete for tasks from the same queue.
 * This means every task is attempted by whichever consumer grabs it first:
 * <ul>
 *   <li>If a local solver grabs it, it is solved locally.</li>
 *   <li>If the Outsourcer grabs it, it is published to a remote worker.
 *       The Outsourcer waits asynchronously; if no result arrives within the
 *       timeout it falls back to solving locally via {@link #requeueLocally}.</li>
 * </ul>
 * {@link #submitResult} uses a best-result tracker, so if both a local and a
 * remote worker somehow solve the same task (e.g. race before fallback fires)
 * only the shorter tour is kept — no double-counting occurs.
 *
 * <h3>Semaphore protocol (six-step)</h3>
 * <ol>
 *   <li>Producer acquires {@code empty} — blocks if queue full.</li>
 *   <li>Producer locks, enqueues, unlocks.</li>
 *   <li>Producer releases {@code full} — signals any consumer.</li>
 *   <li>Consumer (solver or outsourcer) acquires {@code full}.</li>
 *   <li>Consumer locks, dequeues, unlocks.</li>
 *   <li>Consumer releases {@code empty} — signals producer.</li>
 * </ol>
 * The fallback local queue uses an identical independent protocol.
 *
 * @author ryanschmitt
 * @version 6.0
 */
public class TspProblemRepository extends PropertyChangeSupport {

  public static final String EVENT_RESULT      = "result";
  public static final String EVENT_BEST_RESULT = "bestResult";
  public static final String EVENT_BATCH       = "batch";
  public static final String EVENT_ERROR       = "error";
  public static final String EVENT_PROBLEM     = "problems";

  private static final int CAPACITY = 2000;

  private static final TspProblemRepository instance = new TspProblemRepository();

  // ---- Shared queue (Producer → TspSolver threads AND Outsourcer) ----------
  private final Queue<TspProblem> sharedQueue = new LinkedList<>();
  private final Semaphore emptyShared = new Semaphore(CAPACITY);
  private final Semaphore fullShared  = new Semaphore(0);
  private final Lock      sharedLock  = new ReentrantLock();

  // ---- Fallback local queue (Outsourcer timeout → TspSolver threads) -------
  private final Queue<TspProblem> localQueue  = new LinkedList<>();
  private final Semaphore emptyLocal = new Semaphore(CAPACITY);
  private final Semaphore fullLocal  = new Semaphore(0);
  private final Lock      localLock  = new ReentrantLock();

  // ---- Best-result tracking ------------------------------------------------
  private final Map<String, TspResult>     bestResults     = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> totalCounts     = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> completedCounts = new ConcurrentHashMap<>();

  private TspProblemRepository() { super(new Object()); }

  public static TspProblemRepository getInstance() { return instance; }

  // -------------------------------------------------------------------------
  // Batch registration
  // -------------------------------------------------------------------------

  public void registerBatch(String problemName, int total) {
    totalCounts.put(problemName, new AtomicInteger(total));
    completedCounts.put(problemName, new AtomicInteger(0));
    bestResults.remove(problemName);
    firePropertyChange(EVENT_BATCH, null, new BatchInfo(problemName, total));
  }

  // -------------------------------------------------------------------------
  // Producer → shared queue
  // -------------------------------------------------------------------------

  public void loadFromFile(File file) throws IOException, InterruptedException {
    String name = file.getName().replaceFirst("\\.tsp$", "");
    List<City> cities = TspParser.load(file);
    registerBatch(name, cities.size());
    for (int i = 0; i < cities.size(); i++) {
      produce(new TspProblem(name, cities, i));
    }
  }

  /**
   * Enqueues into the shared queue. Both local solvers and the Outsourcer
   * compete to consume from here. Steps 1-3 of the semaphore protocol.
   */
  public void produce(TspProblem problem) throws InterruptedException {
    emptyShared.acquire();
    sharedLock.lock();
    try {
      sharedQueue.add(problem);
      firePropertyChange(EVENT_PROBLEM, null, problem);
    } finally {
      sharedLock.unlock();
    }
    fullShared.release();
  }

  // -------------------------------------------------------------------------
  // Shared consumer (used by BOTH TspSolver and Outsourcer)
  // -------------------------------------------------------------------------

  /**
   * Dequeues the next sub-problem. Whichever thread calls this first — a
   * local solver or the Outsourcer — gets the task. Blocks until one is
   * available. Steps 4-6 of the semaphore protocol.
   */
  public TspProblem getNextProblem() throws InterruptedException {
    fullShared.acquire();
    TspProblem problem;
    sharedLock.lock();
    try {
      problem = sharedQueue.remove();
    } finally {
      sharedLock.unlock();
    }
    emptyShared.release();
    return problem;
  }

  // -------------------------------------------------------------------------
  // Fallback local queue (Outsourcer timeout path)
  // -------------------------------------------------------------------------

  /**
   * Called by the Outsourcer when a remote task times out. Moves the problem
   * into the fallback local queue so a TspSolver thread can handle it.
   * Steps 1-3 of the local semaphore protocol.
   */
  public void requeueLocally(TspProblem problem) throws InterruptedException {
    emptyLocal.acquire();
    localLock.lock();
    try {
      localQueue.add(problem);
    } finally {
      localLock.unlock();
    }
    fullLocal.release();
  }

  /**
   * Dequeues from the fallback local queue. Used by TspSolver threads when
   * they have no shared-queue work. Steps 4-6 of the local semaphore protocol.
   */
  public TspProblem getNextLocalFallback() throws InterruptedException {
    fullLocal.acquire();
    TspProblem problem;
    localLock.lock();
    try {
      problem = localQueue.remove();
    } finally {
      localLock.unlock();
    }
    emptyLocal.release();
    return problem;
  }

  // -------------------------------------------------------------------------
  // Result submission
  // -------------------------------------------------------------------------

  /**
   * Submit a completed result. Always fires EVENT_RESULT for progress
   * counting. Fires EVENT_BEST_RESULT only when this beats the current best.
   * Safe to call from multiple threads for the same sub-problem — only the
   * shorter tour wins.
   */
  public void submitResult(TspResult result) {
    String name = result.getProblem().getName();
    firePropertyChange(EVENT_RESULT, null, result);

    boolean isBest = false;
    TspResult previous;
    synchronized (bestResults) {
      previous = bestResults.get(name);
      if (previous == null || result.getLength() < previous.getLength()) {
        bestResults.put(name, result);
        isBest = true;
      }
    }
    if (isBest) firePropertyChange(EVENT_BEST_RESULT, previous, result);

    AtomicInteger completed = completedCounts.get(name);
    AtomicInteger total     = totalCounts.get(name);
    if (completed != null) {
      int done = completed.incrementAndGet();
      int tot  = total != null ? total.get() : -1;
      System.out.printf("[Progress] %s  %d/%d%s%n",
          name, done, tot,
          isBest ? "  ★ " + String.format("%.3f", result.getLength()) : "");
    }
  }

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  public int sharedQueueSize() {
    sharedLock.lock();
    try { return sharedQueue.size(); } finally { sharedLock.unlock(); }
  }

  public int localQueueSize() {
    localLock.lock();
    try { return localQueue.size(); } finally { localLock.unlock(); }
  }

  public Optional<TspResult> getBestResult(String name) {
    return Optional.ofNullable(bestResults.get(name));
  }

  public int getCompleted(String name) {
    AtomicInteger c = completedCounts.get(name);
    return c == null ? 0 : c.get();
  }

  public int getTotal(String name) {
    AtomicInteger t = totalCounts.get(name);
    return t == null ? 0 : t.get();
  }

  @Override
  public void firePropertyChange(String propertyName, Object oldValue, Object newValue) {
    super.firePropertyChange(propertyName, oldValue, newValue);
  }

  // -------------------------------------------------------------------------
  // Nested types
  // -------------------------------------------------------------------------

  public static class BatchInfo {
    public final String name;
    public final int total;
    BatchInfo(String name, int total) { this.name = name; this.total = total; }
  }
}
