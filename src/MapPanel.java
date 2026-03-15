import javax.swing.*;
import java.awt.*;
import java.util.List;

/**
 * A simple JPanel that draws the cities and the tour, with rotation and better scaling.
 *
 * @author javiergs
 * @version 2.0
 */
public class MapPanel extends JPanel {

  private List<City> cities = List.of();
  private List<Integer> tour = List.of();
  private double rotation = -Math.PI / 2 + Math.PI;  // = +Math.PI/2

  public MapPanel() {
    setBackground(new Color(220, 220, 220));
  }

  public void setCities(List<City> cities) {
    this.cities = (cities == null) ? List.of() : cities;
    this.tour = List.of();
    repaint();
  }

  public void setTour(List<Integer> tour) {
    this.tour = (tour == null) ? List.of() : tour;
    repaint();
  }

  @Override
  protected void paintComponent(Graphics g) {
    super.paintComponent(g);
    if (cities == null || cities.isEmpty()) {
      g.setColor(Color.DARK_GRAY);
      g.drawString("Load a TSPLIB .tsp file (NODE_COORD_SECTION).", 20, 25);
      return;
    }

    Graphics2D g2 = (Graphics2D) g.create();
    g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
    int pad = 25;
    int w = getWidth();
    int h = getHeight();
    // 1) Original bounds
    double minX = Double.POSITIVE_INFINITY, maxX = Double.NEGATIVE_INFINITY;
    double minY = Double.POSITIVE_INFINITY, maxY = Double.NEGATIVE_INFINITY;
    for (City c : cities) {
      minX = Math.min(minX, c.getX());
      maxX = Math.max(maxX, c.getX());
      minY = Math.min(minY, c.getY());
      maxY = Math.max(maxY, c.getY());
    }
    // 2) Rotate around the center of the dataset (important!)
    double cx = (minX + maxX) / 2.0;
    double cy = (minY + maxY) / 2.0;
    double cos = Math.cos(rotation);
    double sin = Math.sin(rotation);
    // 3) Compute bounds AFTER rotation (so we can fit to screen)
    double rMinX = Double.POSITIVE_INFINITY, rMaxX = Double.NEGATIVE_INFINITY;
    double rMinY = Double.POSITIVE_INFINITY, rMaxY = Double.NEGATIVE_INFINITY;
    for (City c : cities) {
      double x = c.getX() - cx;
      double y = c.getY() - cy;
      double xr = x * cos - y * sin;
      double yr = x * sin + y * cos;
      rMinX = Math.min(rMinX, xr);
      rMaxX = Math.max(rMaxX, xr);
      rMinY = Math.min(rMinY, yr);
      rMaxY = Math.max(rMaxY, yr);
    }
    double rDx = Math.max(1e-9, (rMaxX - rMinX));
    double rDy = Math.max(1e-9, (rMaxY - rMinY));
    double sx = (w - 2.0 * pad) / rDx;
    double sy = (h - 2.0 * pad) / rDy;
    double s = Math.min(sx, sy);
    // helper: city -> pixel (rotate, normalize into rotated bounds, then scale)
    double finalRMinX = rMinX;
    double finalRMinY = rMinY;
    java.util.function.Function<City, Point> toPixel = (City c) -> {
      double x = c.getX() - cx;
      double y = c.getY() - cy;
      double xr = x * cos - y * sin;
      double yr = x * sin + y * cos;
      // shift into positive rotated bounds
      double nx = xr - finalRMinX;
      double ny = yr - finalRMinY;
      int px = (int) Math.round(pad + nx * s);
      int py = (int) Math.round(h - pad - ny * s);
      return new Point(px, py);
    };
    // Draw tour (behind points)
    if (tour != null && tour.size() >= 2) {
      g2.setColor(Color.BLACK);
      for (int i = 0; i < tour.size() - 1; i++) {
        City a = cities.get(tour.get(i));
        City b = cities.get(tour.get(i + 1));
        Point pa = toPixel.apply(a);
        Point pb = toPixel.apply(b);
        a.drawPath(g2, pa.x, pa.y, pb.x, pb.y);
      }
    }
    // Draw cities
    g2.setColor(new Color(30, 90, 200));
    for (City c : cities) {
      Point p = toPixel.apply(c);
      c.draw(g2, p.x, p.y);
    }
    g2.dispose();
  }

}
