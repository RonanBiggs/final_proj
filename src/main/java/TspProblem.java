import java.util.List;

public class TspProblem {

  private final String name;
  private final List<City> cities;
  private final int startIndex;

  /**
   * Full constructor.
   *
   * @param name       human-readable label (e.g. the .tsp filename)
   * @param cities     the full set of cities for this problem
   * @param startIndex the city index the Nearest Neighbor heuristic should start
   *                   from
   */
  public TspProblem(String name, List<City> cities, int startIndex) {
    this.name = name;
    this.cities = List.copyOf(cities);
    this.startIndex = startIndex;
  }

  /** Convenience constructor — startIndex defaults to 0. */
  public TspProblem(String name, List<City> cities) {
    this(name, cities, 0);
  }

  public String getName() {
    return name;
  }

  public List<City> getCities() {
    return cities;
  }

  public int getStartIndex() {
    return startIndex;
  }

  @Override
  public String toString() {
    return name + " (" + cities.size() + " cities, start=" + startIndex + ")";
  }
}
