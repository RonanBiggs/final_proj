import java.io.*;
import java.util.*;

/**
 * A simple parser for TSPLIB .tsp files that extracts city coordinates.
 *
 * @author javiergs
 * @version 1.0
 */
public class TspParser {

  public static List<City> load(File file) throws IOException {
    List<City> cities = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      boolean inNodes = false;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty()) continue;
        if (!inNodes) {
          if (line.equalsIgnoreCase("NODE_COORD_SECTION")) {
            inNodes = true;
          }
          continue;
        }
        if (line.equalsIgnoreCase("EOF")
            || line.equalsIgnoreCase("DISPLAY_DATA_SECTION")
            || line.equalsIgnoreCase("EDGE_WEIGHT_SECTION")) {
          break;
        }
        String[] parts = line.split("\\s+");
        if (parts.length < 3) continue;

        int id = Integer.parseInt(parts[0]);
        double x = Double.parseDouble(parts[1]);
        double y = Double.parseDouble(parts[2]);
        cities.add(new City(id, x, y));
      }
    }
    if (cities.isEmpty()) {
      throw new IOException("No cities found. Is this a TSPLIB file with NODE_COORD_SECTION?");
    }
    return cities;
  }

}