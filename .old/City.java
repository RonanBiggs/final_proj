import java.awt.*;

/**
 * Represents a city with an ID and (x, y) coordinates.
 *
 * @author javiergs
 * @version 2.0
 */
public class City {

  private final int id;
  private final double x;
  private final double y;

  public City(int id, double x, double y) {
    this.id = id;
    this.x = x;
    this.y = y;
  }

  public int getId() {
    return id;
  }

  public double getX() {
    return x;
  }

  public double getY() {
    return y;
  }

  public double distanceTo(City other) {
    double dx = x - other.x;
    double dy = y - other.y;
    return Math.sqrt(dx * dx + dy * dy);
  }

  public void draw(Graphics2D g, int px, int py) {
    int r = 1;
    g.fillOval(px - r / 2, py - r / 2, r, r);
  }

  public void drawPath(Graphics2D g, int x1, int y1, int x2, int y2) {
    g.drawLine(x1, y1, x2, y2);
  }
}