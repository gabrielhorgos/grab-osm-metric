public class Node {

  private long id;
  private double longitude;
  private double latitude;

  public Node(String s) {
    String[] values = s.split(" ");

    this.id = Long.valueOf(values[0]);
    this.latitude = Double.valueOf(values[1]);
    this.longitude = Double.valueOf(values[2]);
  }

  public long getId() {
    return id;
  }

  public double getLongitude() {
    return longitude;
  }

  public double getLatitude() {
    return latitude;
  }
}
