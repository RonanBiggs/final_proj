import java.io.File;
import java.io.IOException;

public class TspProducer implements Runnable {

  private final File file;
  private final TspProblemRepository repository;

  public TspProducer(File file, TspProblemRepository repository) {
    this.file = file;
    this.repository = repository;
  }

  @Override
  public void run() {
    try {
      repository.loadFromFile(file);
    } catch (IOException e) {
      repository.firePropertyChange(TspProblemRepository.EVENT_ERROR, null, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
