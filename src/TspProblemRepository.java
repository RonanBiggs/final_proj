package tsp;

import java.beans.PropertyChangeSupport;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Singleton buffer that holds TSP problems waiting to be solved.
 * Producers enqueue problems via {@link #produce}; solvers dequeue them via {@link #getNextProblem}.
 * Capacity is enforced with semaphores and thread safety is guaranteed by a reentrant lock.
 *
 * @author ryanschmitt
 * @version 2.0
 */
public class TspProblemRepository extends PropertyChangeSupport {

  private static final int CAPACITY = 10;
  private static final TspProblemRepository instance = new TspProblemRepository();

  private final Queue<TspProblem> problems = new LinkedList<>();
  private final Semaphore empty = new Semaphore(CAPACITY);
  private final Semaphore full = new Semaphore(0);
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
   *
   * @throws InterruptedException if the thread is interrupted while waiting for space
   */
  public void produce(TspProblem problem) throws InterruptedException {
    empty.acquire();
    lock.lock();
    try {
      problems.add(problem);
      firePropertyChange("problems", null, problem);
    } finally {
      lock.unlock();
    }
    full.release();
  }

  /**
   * Dequeues and returns the next problem to solve. Blocks until one is available.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public TspProblem getNextProblem() throws InterruptedException {
    full.acquire();
    TspProblem problem;
    lock.lock();
    try {
      problem = problems.remove();
    } finally {
      lock.unlock();
    }
    empty.release();
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
}
