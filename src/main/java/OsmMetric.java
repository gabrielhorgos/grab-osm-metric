import model.Node;
import model.Way;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.explode;

public class OsmMetric {

  private static final String NODES_FILE_PATH =
      "src/main/resources/andorra_28-09-2022.osm.pbf.node.parquet";
  private static final String WAYS_FILE_PATH =
      "src/main/resources/andorra_28-09-2022.osm.pbf.way.parquet";

  public static void main(String[] args) {
    SparkSession spark = getSparkSeesion();
    SQLContext sqlContext = spark.sqlContext();

    Encoder<Node> nodeEncoder = Encoders.bean(Node.class);

    Dataset<Node> nodesDataSet = sqlContext.read().parquet(NODES_FILE_PATH).as(nodeEncoder);
    // Parquet files can also be used to create a temporary view and then used in SQL statements
    nodesDataSet.createOrReplaceTempView("nodes");

    // nodesDataSet.show(10, false);

    Encoder<Way> wayEncoder = Encoders.bean(Way.class);
    Dataset<Way> wayDataSet = sqlContext.read().parquet(WAYS_FILE_PATH).as(wayEncoder);
    wayDataSet.createOrReplaceTempView("ways");

    // wayDataSet.show(10, false);

    // Dataset<Row> ways =
    wayDataSet
        .select(wayDataSet.col("id"), explode(wayDataSet.col("tags")).as("exploded_tags"))
        .filter(col("exploded_tags.key").contains("highway"))
        .select(col("id"), col("exploded_tags.value").as("highway_type").cast("string"))
        .groupBy(col("highway_type"))
        .agg(collect_list("id").as("id_list"))
        .show(10, false);

    /*ways.filter(ways.col("exploded_tags.key").contains("highway"))
    .select(
        ways.col("id").as("way_id"),
        ways.col("exploded_tags.value").as("highway_type").cast("string"))*/

  }

  private static SparkSession getSparkSeesion() {
    return SparkSession.builder()
        .appName("OsmMetric")
        .config("spark.master", "local")
        .getOrCreate();
  }
}
