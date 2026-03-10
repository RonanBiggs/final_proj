package tsp;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;

/**
  * A simple JPanel that draws the cities and the tour.
 *
 * @author javiergs
 * @version 2.0
 */
public class DelegatePanel extends JPanel {

  private final ArrayList<City> cities;
  private List<Integer> tour = List.of(); // indices into cities list

  public DelegatePanel(ArrayList<City> cities) {
    setBackground(new Color(200, 200, 200));
    this.cities = cities;
  }

  public void setTour(List<Integer> tour) {
    this.tour = (tour == null) ? List.of() : tour;
    repaint();
  }

  @Override
  protected void paintComponent(Graphics g) {
    super.paintComponent(g);

    Graphics2D g2 = (Graphics2D) g;

    if (cities.isEmpty()) return;

    int w = getWidth();
    int h = getHeight();

    double minX = cities.stream().mapToDouble(City::getX).min().getAsDouble();
    double maxX = cities.stream().mapToDouble(City::getX).max().getAsDouble();
    double minY = cities.stream().mapToDouble(City::getY).min().getAsDouble();
    double maxY = cities.stream().mapToDouble(City::getY).max().getAsDouble();

    double scaleX = (w - 40) / (maxX - minX);
    double scaleY = (h - 40) / (maxY - minY);

    // Draw cities
    for (City c : cities) {

      int px = (int)((c.getX() - minX) * scaleX) + 20;
      int py = h - ((int)((c.getY() - minY) * scaleY) + 20);

      c.draw(g2, px, py);
    }

    // Draw tour
    if (tour != null && tour.size() > 1) {

      for (int i = 0; i < tour.size() - 1; i++) {

        City a = cities.get(tour.get(i));
        City b = cities.get(tour.get(i + 1));

        int x1 = (int)((a.getX() - minX) * scaleX) + 20;
        int y1 = h - ((int)((a.getY() - minY) * scaleY) + 20);

        int x2 = (int)((b.getX() - minX) * scaleX) + 20;
        int y2 = h - ((int)((b.getY() - minY) * scaleY) + 20);

        a.drawPath(g2, x1, y1, x2, y2);
      }
    }
  }

}