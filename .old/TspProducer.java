import java.io.File;
import java.io.IOException;

/**
 * Producer thread: loads a TSPLIB .tsp file and enqueues it in the shared
 * {@link TspProblemRepository}. Running this on its own thread means the
 * GUI stays responsive even when parsing large files — the EDT (Event
 * Dispatch Thread) is never blocked.
 *
 * The producer is gated by Semaphore A (empty slots) inside the repository:
 * if the buffer is full it blocks here, never in the GUI.
 *
 * Design note: one TspProducer is created per file load. This is intentional —
 * it keeps the producer stateless and easy to reason about. A pool of
 * persistent producers would add complexity with no benefit for this workload.
 *
 * @author ryanschmitt
 * @version 1.0
 */
public class TspProducer implements Runnable {

  private final File file;
  private final TspProblemRepository repository;

  public TspProducer(File file, TspProblemRepository repository) {
    this.file       = file;
    this.repository = repository;
  }

  @Override
  public void run() {
    try {
      // loadFromFile parses the .tsp file then calls produce(), which:
      //   1. acquires Semaphore A (blocks if buffer is full)
      //   2. locks the queue, enqueues, unlocks
      //   3. releases Semaphore B (signals consumer)
      repository.loadFromFile(file);
    } catch (IOException e) {
      // Fire an error event so the GUI can display it without us touching Swing.
      repository.firePropertyChange("error", null, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}