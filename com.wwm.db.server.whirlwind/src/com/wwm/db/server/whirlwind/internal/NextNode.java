package com.wwm.db.server.whirlwind.internal;

import org.fuzzydb.attrs.internal.NodeScore;
import org.fuzzydb.server.internal.search.Priority;



/**
 * Tuple of unexpanded node and it's priority
 * @author ac
 */
public class NextNode extends Priority {
	private NodeScore score;
	private Node<?> node;
	
	/**
	 * @param score
	 * @param sequence
	 * @param node
	 */
	public NextNode(NodeScore score, int sequence, Node<?> node) {
		super(sequence);
		this.score = score;
		this.node = node;
	}
	/**
	 * @return Returns the node.
	 */
	public Node<?> getNode() {
		return node;
	}

	@Override
	public NodeScore getScore() {
		return score;
	}
	
	@Override
	public String toString() {
		return score.toString();
	}
}
