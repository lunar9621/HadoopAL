package apriori;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashTreeNode {

	private Map<Integer, HashTreeNode> mapAtNode;
	private boolean isLeafNode;
	private List<ItemSet> itemsets;

	public HashTreeNode() {
		mapAtNode = new HashMap<Integer, HashTreeNode>();
		isLeafNode = false;
		itemsets = new ArrayList<ItemSet>();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("IsLeaf : ").append(Boolean.toString(isLeafNode)).append("\t");
		builder.append("MapKeys :").append(mapAtNode.keySet().toString()).append("\t");
		builder.append("Itemsets : ").append(itemsets.toString());
		return builder.toString();
	}
	public Map<Integer, HashTreeNode> getMapAtNode() {
		return mapAtNode;
	}

	public void setMapAtNode(Map<Integer, HashTreeNode> mapAtNode) {
		this.mapAtNode = mapAtNode;
	}

	public boolean isLeafNode() {
		return isLeafNode;
	}

	public void setLeafNode(boolean isLeafNode) {
		this.isLeafNode = isLeafNode;
	}

	public List<ItemSet> getItemsets() {
		return itemsets;
	}

	public void setItemsets(List<ItemSet> itemsets) {
		this.itemsets = itemsets;
	}
}
