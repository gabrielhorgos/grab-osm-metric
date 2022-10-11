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


Current result 

+--------------+------------------+
|highway_type  |sum(distance)     |
+--------------+------------------+
|pedestrian    |21826.585584950786|
|trunk_link    |2273.8139354601262|
|cycleway      |5894.64321229683  |
|primary_link  |1131.8420388222607|
|tertiary      |28370.562690892428|
|path          |568448.1131504707 |
|via_ferrata   |192.36722309372973|
|residential   |71251.10703255486 |
|trunk         |4347.36580803968  |
|steps         |1691.2637147681476|
|track         |193869.64446592404|
|footway       |54249.947603743734|
|raceway       |609.6929022847017 |
|unclassified  |10484.336123973177|
|secondary_link|794.3431974707686 |
|secondary     |88459.32253424748 |
|service       |53063.4754974443  |
|construction  |218.0643706429335 |
|rest_area     |118.15572905379474|
|living_street |1966.1264539411998|
|primary       |79690.04583937746 |
|tertiary_link |71.23147000817004 |
+--------------+------------------+
