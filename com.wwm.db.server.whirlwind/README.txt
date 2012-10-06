


Some design thoughts:

	
	
IndexedTable and IndexManager
=============================
- IndexedTable is persistent, as are all IndexManager implementations. The IndexManager is provided 
to the IndexedTable to give it the ability to update multiple indexes.  It seems to make sense to have 
IndexManagerImpl consult the ImplementationsService to find out what indexes to build. 
	

Visitor Pattern
===============
When we are matching, we are visiting (as per a Visitor pattern GoF '95) IWhirlwindItem.

But we first visit *some* BranchNodes and LeafNodes, for which we need some sort of "Tour Guide",
which works in collaboration with the Visitor.

The guide or navigator walks some parts of the tree, allows a visit, and then takes it's next actions
based on the result.

Perhaps it's like a journey through a maze, or town, searching for areas with certain characteristics desired
by the visitor.  At each junction, the visitor can read a sign and decide which route to take, based on
information about what lies beyond (e.g. number of houses, flats, min/max price, if pets are allowed, etc).
 
The aim of the visitor is to *know* that they have found the best 10 houses in the city according to
their specific criteria.

As the visitor travels, they keep a queue of the best options found so far.  As they find individual
houses, they insert them into their queue in behind queued options that may score higher.

The queue is a PendingVisitsQ.  A visit() results in there being items to add to the queue according to their
potential rank.

As the visitor progresses, they will eventually be able to build an ordered list of results, if that is what
they are doing.

A visitor could also do work, such as making some changes (interesting in the context of transactions, 
but not impossible - it would be commit-phase work, as an insert is - in fact an insert operation is a visit, 
as would be a lazy insert operation, and also a background - "push-down" of lazy inserts).

So where are we?

We have some sort of NodeVisitor, which can return a scored list of Nodes to visit, which must also be able
to be a LeafVisitor .. either doing the final insert, or returning results.

The LeafVisitor is actually a visitor to a collection of IWhirlwindItem, which, in write mode, may be
added to.  So... a visitor knows how to visit different types that it may encounter.  It can also
call on other items to accept it without knowing with they are.....



