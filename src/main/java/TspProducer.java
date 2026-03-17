import java.io.File;
import java.io.IOException;

/**
 * Producer thread: loads a TSPLIB .tsp file and enqueues one sub-problem
 * per starting city into the shared {@link TspProblemRepository}.
 *
 * <h3>Why one sub-problem per city?</h3>
 * The Nearest Neighbor heuristic is sensitive to the starting city — a
 * different start often yields a significantly shorter tour. By splitting
 * the file into N sub-problems (one per city), we let local and remote
 * workers explore all N starting points in parallel. The repository tracks
 * the best tour across all results and notifies the GUI whenever a new
 * champion is found.
 *
 * <h3>Threading model</h3>
 * Running on its own thread keeps the EDT (Event Dispatch Thread)
 * responsive even for large files. The producer blocks inside
 * {@link TspProblemRepository#produce} if the buffer is full — it never
 * blocks the GUI.
 *
 * @author ryanschmitt
 * @version 2.0
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
      // loadFromFile parses the .tsp file, registers a batch, then
      // calls produce() once per city — each produce() follows the
      // six-step semaphore protocol inside the repository.
      repository.loadFromFile(file);
    } catch (IOException e) {
      repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
