import java.beans.PropertyChangeSupport;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TspProblemRepository extends PropertyChangeSupport {

  public static final String EVENT_RESULT = "result";
  public static final String EVENT_BEST_RESULT = "bestResult";
  public static final String EVENT_BATCH = "batch";
  public static final String EVENT_ERROR = "error";
  public static final String EVENT_PROBLEM = "problems";

  private static final int CAPACITY = 2000;

  private static final TspProblemRepository instance = new TspProblemRepository();

  private final Queue<TspProblem> sharedQueue = new LinkedList<>();
  private final Semaphore emptyShared = new Semaphore(CAPACITY);
  private final Semaphore fullShared = new Semaphore(0);
  private final Lock sharedLock = new ReentrantLock();

  private final Queue<TspProblem> localQueue = new LinkedList<>();
  private final Semaphore emptyLocal = new Semaphore(CAPACITY);
  private final Semaphore fullLocal = new Semaphore(0);
  private final Lock localLock = new ReentrantLock();

  private final Map<String, TspResult> bestResults = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> totalCounts = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> completedCounts = new ConcurrentHashMap<>();

  private TspProblemRepository() {
    super(new Object());
  }

  public static TspProblemRepository getInstance() {
    return instance;
  }

  public void registerBatch(String problemName, int total) {
    totalCounts.put(problemName, new AtomicInteger(total));
    completedCounts.put(problemName, new AtomicInteger(0));
    bestResults.remove(problemName);
    firePropertyChange(EVENT_BATCH, null, new BatchInfo(problemName, total));
  }

  public void loadFromFile(File file) throws IOException, InterruptedException {
    String name = file.getName().replaceFirst("\\.tsp$", "");
    List<City> cities = TspParser.load(file);
    registerBatch(name, cities.size());
    for (int i = 0; i < cities.size(); i++) {
      produce(new TspProblem(name, cities, i));
    }
  }

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
    if (isBest)
      firePropertyChange(EVENT_BEST_RESULT, previous, result);

    AtomicInteger completed = completedCounts.get(name);
    AtomicInteger total = totalCounts.get(name);
    if (completed != null) {
      int done = completed.incrementAndGet();
      int tot = total != null ? total.get() : -1;
      System.out.printf("[Progress] %s  %d/%d%s%n",
          name, done, tot,
          isBest ? "  ★ " + String.format("%.3f", result.getLength()) : "");
    }
  }

  public int sharedQueueSize() {
    sharedLock.lock();
    try {
      return sharedQueue.size();
    } finally {
      sharedLock.unlock();
    }
  }

  public int localQueueSize() {
    localLock.lock();
    try {
      return localQueue.size();
    } finally {
      localLock.unlock();
    }
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

  public static class BatchInfo {
    public final String name;
    public final int total;

    BatchInfo(String name, int total) {
      this.name = name;
      this.total = total;
    }
  }
}
