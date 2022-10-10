package model;

public class Way {

    private long id;
    private Integer version;
    private long timestamp;
    private long changeset;
    private int uid;
    private String user_sid;
    private Tag[] tags;
    private WayNode[] nodes;

    private class WayNode {
        private int index;
        private long nodeId;
    }
}
