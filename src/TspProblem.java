package tsp;

import java.util.List;

/**
 * Represents a single TSP problem instance: a named set of cities to be solved.
 *
 * @author ryanschmitt
 * @version 1.0
 */
public class TspProblem {

  private final String name;
  private final List<City> cities;

  public TspProblem(String name, List<City> cities) {
    this.name = name;
    this.cities = List.copyOf(cities);
  }

  public String getName() {
    return name;
  }

  public List<City> getCities() {
    return cities;
  }

  @Override
  public String toString() {
    return name + " (" + cities.size() + " cities)";
  }
}
