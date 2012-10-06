package com.wwm.db.server.whirlwind.internal;

public interface NodeVisitor {

	public void visit(BranchNode branchNode, NodeStorageManager storage);

	public void visit(LeafNode leafNode, NodeStorageManager storage);

}