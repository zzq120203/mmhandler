package iie.databak;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;

public class NodeGroupImage implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String node_group_name; // required
	private String comment; // required
	private int status; // required
	private List<String> nodeKeys; // required


	public NodeGroupImage(String node_group_name, String comment, int status,
			List<String> nodeKeys) {
		super();
		this.node_group_name = node_group_name;
		this.comment = comment;
		this.status = status;
		this.nodeKeys = nodeKeys;
	}


	public static NodeGroupImage generateNodeGroupImage(NodeGroup ng)
	{
		List<String> nodeKeys = new ArrayList<String>();
		for(Node nd : ng.getNodes()){
			//String ndKey = name + ".nd." + nd.getNode_name();
			nodeKeys.add(nd.getNode_name());
		}
		return new NodeGroupImage(ng.getNode_group_name(), ng.getComment(), ng.getStatus(),nodeKeys);
	}
	

	public String getNode_group_name() {
		return node_group_name;
	}

	public void setNode_group_name(String node_group_name) {
		this.node_group_name = node_group_name;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public List<String> getNodeKeys() {
		return nodeKeys;
	}

	public void setNodeKeys(List<String> nodeKeys) {
		this.nodeKeys = nodeKeys;
	}


	@Override
	public String toString() {
		return "NodeGroup [node_group_name=" + node_group_name + ", comment=" + comment + ", status="
				+ status  + "]";
	}

	
}
