import java.util.*;

/**
 * A simple implementation of the Nearest Neighbor heuristic for TSP.
 *
 * @author javiergs
 * @version 2.0
 */
public class NearestNeighborSolver {

  public static List<Integer> solve(List<City> cities, int startIndex) {
    int n = cities.size();
    if (n == 0) return List.of();
    if (startIndex < 0 || startIndex >= n) startIndex = 0;
    boolean[] used = new boolean[n];
    List<Integer> tour = new ArrayList<>(n + 1);
    int current = startIndex;
    used[current] = true;
    tour.add(current);
    for (int step = 1; step < n; step++) {
      int next = -1;
      double best = Double.POSITIVE_INFINITY;
      City curCity = cities.get(current);
      for (int j = 0; j < n; j++) {
        if (used[j]) continue;
        double d = curCity.distanceTo(cities.get(j));
        if (d < best) {
          best = d;
          next = j;
        }
      }
      used[next] = true;
      tour.add(next);
      current = next;
    }
    tour.add(tour.get(0));
    return tour;
  }

  public static double length(List<City> cities, List<Integer> tour) {
    if (tour == null || tour.size() < 2) return 0.0;
    double total = 0.0;
    for (int i = 0; i < tour.size() - 1; i++) {
      City a = cities.get(tour.get(i));
      City b = cities.get(tour.get(i + 1));
      total += a.distanceTo(b);
    }
    return total;
  }

}
