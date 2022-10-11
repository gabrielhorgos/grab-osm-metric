# grab-osm-metric

Compute and display the following metric on OSM data:
 the total length of roads grouped by highway type.

Use Java and Spark to solve the problem.

###
1. Load the two datasets from .parquet files
2. Filter ways dataset and keep only those with highway tags
3. Explode ways.nodes and select : ids, highway_tag, node_id
4. Join ways (point 3.) with nodes dataset on node_id
5. Merge into one column a node representation as a string : "id latitude longitude"
6. Select now looks like : way_id , highway_type, node_as_string
7. group by highway type and collect nodes into one columns ar array -> way_id, highway_type, [node_as_string1, node_as_string_2]
8. select all columns and apply distance on nodes column (on the nodes_string array)
9. group by highway type and sum the distance column
10. display result


####
Distance between two nodes (by coordinates) is Haversine algorithm (found and used as without any changes)
