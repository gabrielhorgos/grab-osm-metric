import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Iterator;
import scala.collection.immutable.ArraySeq;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.explode;

public class OsmMetric {

  private static final String NODES_FILE_PATH =
      "src/main/resources/andorra_28-09-2022.osm.pbf.node.parquet";
  private static final String WAYS_FILE_PATH =
      "src/main/resources/andorra_28-09-2022.osm.pbf.way.parquet";

  public static void main(String[] args) {
    SparkSession spark = getSparkSeesion();
    SQLContext sqlContext = spark.sqlContext();

    Dataset<Row> nodeDS = sqlContext.read().parquet(NODES_FILE_PATH);
    // Parquet files can also be used to create a temporary view and then used in SQL statements
    nodeDS.createOrReplaceTempView("nodes");

    Dataset<Row> waysDS = sqlContext.read().parquet(WAYS_FILE_PATH);
    waysDS.createOrReplaceTempView("ways");

    // prepp for joining with nodes
    Dataset<Row> ways = filterHighwayTagsAndExplodeNodes(waysDS);

    Dataset<Row> richWays = joinNodesAndComputeDistance(nodeDS, ways);

    richWays.show(100, false);

    // group by highway type and sum distances
    Dataset<Row> lengthByRoadDS = richWays.groupBy("highway_type").sum("distance");

    lengthByRoadDS.show(100, false);
  }

  private static Dataset<Row> joinNodesAndComputeDistance(Dataset<Row> nodeDS, Dataset<Row> ways) {
    // Idea is to match and merge node_id with it's longitude and latitude so we can compute the
    // distances for each
    // way using their nodes array
    Dataset<Row> richWays =
        // join nodes on node id
        ways.join(nodeDS, ways.col("exploded_node_id").equalTo(nodeDS.col("id")), "left")
            .select(
                ways.col("id"),
                ways.col("highway_type"),
                // merge node id , latitude and longitude in one column, whitespace separated
                concat_ws(
                        " ",
                        ways.col("exploded_node_id").as("nodeId"),
                        nodeDS.col("latitude"),
                        nodeDS.col("longitude"))
                    .as("rich_node"))
            // group and collect formatted nodes as array (collection)
            .groupBy("id", "highway_type")
            .agg(collect_list("rich_node").as("nodes"))
            // apply distance UDF to nodes array for each way entry
            .select(col("id"), col("highway_type"), distanceUdf.apply(col("nodes")).as("distance"));
    return richWays;
  }

  private static Dataset<Row> filterHighwayTagsAndExplodeNodes(Dataset<Row> waysDS) {
    // idea is to filter out the ways rows that are not highways (including those with nulls)
    // explode tags, keep entries with any highway key, then explode nodes to prep it for join
    Dataset<Row> ways =
        waysDS
            .select(col("id"), explode(col("tags")).as("exploded_tags"), col("nodes"))
            .filter(col("exploded_tags.key").contains("highway"))
            .select(
                col("id"),
                col("exploded_tags.value").cast(DataTypes.StringType).as("highway_type"),
                explode(col("nodes.nodeId").as("nodes")).as("exploded_node_id"))
            .toDF();
    return ways;
  }

  private static final UserDefinedFunction distanceUdf =
      functions.udf((ArraySeq nodeIds) -> totalDistanceBetweenNodes(nodeIds), DataTypes.DoubleType);

  private static double totalDistanceBetweenNodes(ArraySeq nodeIds) {
    List<String> nodes = new ArrayList<>();

    Iterator it = nodeIds.iterator();
    while (it.hasNext()) {
      nodes.add((String) it.next());
    }

    double totalDistance = 0.0;

    for (int i = 0; i < nodes.size() - 1; i++) {
      Node node1 = new Node(nodes.get(i));
      Node node2 = new Node(nodes.get(i + 1));
      totalDistance += Haversine.haversine(node1, node2);
    }

    return totalDistance;
  }

  private static SparkSession getSparkSeesion() {
    return SparkSession.builder()
        .appName("OsmMetric")
        .config("spark.master", "local")
        .getOrCreate();
  }
}
