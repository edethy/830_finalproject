package simpledb;

public class GraphQuery {

    private String node_table_name;
    private String edge_table_name;
    
    // How are we materializing the view and what information do we need?

    public GraphQuery(String node_table_name, String edge_table_name) {
        this.node_table_name = node_table_name;
        this.edge_table_name = edge_table_name;
    }

    public void executeDFSJoinQuery(Field start_node_value, int node_pk_field, Predicate.Op target_node_op,
                                    Field target_node_field_value, int target_node_field, int target_node_join_field
    ){


    }


}