public class Haversine {

  static double haversine(Node node1, Node node2) {
    double latitudeDistance = Math.toRadians(node2.getLatitude() - node1.getLatitude());
    double longitudeDistance = Math.toRadians(node2.getLongitude() - node2.getLongitude());

    double a =
        Math.pow(Math.sin(latitudeDistance / 2), 2)
            + Math.pow(Math.sin(longitudeDistance / 2), 2)
                * Math.cos(Math.toRadians(node1.getLatitude()))
                * Math.cos(Math.toRadians(node2.getLatitude()));
    double rad = 6371_000; // m ?
    double c = 2 * Math.asin(Math.sqrt(a));
    return rad * c;
  }
}
