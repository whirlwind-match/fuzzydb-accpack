package com.wwm.db.server.whirlwind.internal;

import org.fuzzydb.server.internal.pager.RawPagedTableImpl;
import org.fuzzydb.server.internal.server.Namespace;
import org.fuzzydb.server.internal.table.RawTable;
import org.fuzzydb.server.internal.table.Table;
import org.fuzzydb.server.internal.whirlwind.RefPatchingTableImpl;

/**
 * Abstraction for the creation of Whirlwind tables, so we can have different ones for different purposes (as we did in Db1)
 */
public class WWTableFactory {


    public static <T> Table<BranchNode,BranchNode> createBranchWWTable(Namespace namespace, Class<T> clazz, String instanceName) {

        // Create a paged underlying table with a user table on top of the stack
        // 2 entity stack
        RawTable<BranchNode> rawTable = new RawPagedTableImpl<BranchNode>(namespace, clazz,
                "@" + instanceName + "@Branches");	// class@instanceName@Branches is used as disk name
        Table<BranchNode,BranchNode> table = new RefPatchingTableImpl<BranchNode>(rawTable, -1); // table id not required for index tables

        return table;
    }

    public static <T> Table<LeafNode,LeafNode> createLeafWWTable(Namespace namespace, Class<T> clazz, String instanceName) {

        // Create a paged underlying table with a user table on top of the stack
        // 2 entity stack
        RawTable<LeafNode> rawTable = new RawPagedTableImpl<LeafNode>(namespace, clazz,
                "@" + instanceName + "@Leaves");	// class@instanceName@Leaves is used as disk name
        Table<LeafNode,LeafNode> table = new RefPatchingTableImpl<LeafNode>(rawTable, -2); // table id not required for index tables

        return table;
    }

    public static <T> Table<Node,Node> createWWTable(Namespace namespace, Class<T> clazz, String instanceName) {

        // Create a paged underlying table with a user table on top of the stack
        // 2 entity stack
        RawTable<Node> rawTable = new RawPagedTableImpl<Node>(namespace, clazz,
                "@" + instanceName + "@Nodes");	// class@instanceName@Nodes is used as disk name
        @SuppressWarnings("unchecked") // TODO: This gets around generic issue with doing RefPatchingTableImpl<Node>
		Table<Node,Node> table = new RefPatchingTableImpl(rawTable, -3); // table id not required for index tables

        return table;
    }
}
