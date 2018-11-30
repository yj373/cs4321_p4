

Part 1: Top level entry of the project.
    The main function is located at src/App/Main.java
    The SQLInterpreter will reads the input, build the query plan and thus write the output.

Part 2: Implementation of the following algoorithms and functionalities

    1. The selction pushing 
	
    2. The choice of implementation for each logical selection opreator
        This functionality is implemented in the class src/util/SelectDeterminator.java. After the physical plan visits a scan operator, it will creat a SelectDeterminator 
        to determine which index to use or just use full scan. 
    
    3. The choice of join order
    
    4. The choice of each join operator

	 
Part 3: Physical Plan Builder to Seperate the IndexScanOperator Consitions from the normal Scan Conditions

	We build a class “IndexExpressionVisitor” to classify the expressions. If the expression includes two columns, it cannot be handled by index and this condition will be stored in a List “unindexedConditions”. If the express includes only one column and it is in the index information, then this expression can be handled by index and the upper bound and lower bound will be updated. Otherwise, this expression will also be stored in “unindexedConditions”. After visiting all the expressions, the expressions in “unindexedConditions” will be combined in one expression.
	In the “PhysicalPlanVisitor, once the “LogicalScanOperator” is visited, we need to initialize an “IndexedExpressionVisitor” to visit the expression of the “LogicalScanOperator” first. If both the upper bound and the lower bound are “null”, then only a full-scan operator will be created. Othrwise, an “IndexScanOperator” will be created first with the information of upper bound and lower bound. And a full-scan operator will be built with the only one expression in the “unindexedCondition”. Besides that, the previously built “IndexedExpressionVisitor” will be the child operator of this full-scan operator.
	
Part 4: Serialization of relation data (generating the index file)

	In term of index tree, we build different index trees for every required relation once we initialize an index tree builder.
	For the goal of OOD, we add Node class for this index tree so that we can get keys easily.
	
	Because the data is static it is in our interest to fill the tree completely (to keep it as short as possible). Thus every leaf node gets 2d data entries. However, this may leave us with < d data entries for the last leaf node. If this case happens, we handle the second-to-last leaf node and the last leaf node specially, as follows. Assume we have two nodes left to construct and have k data entries, with 2d < k < 3d. Then the second-to-last node gets k=2 entries, and the last node gets the remainder.
	
	Similarly, we then build the layer of index nodes that sits directly above the leaf layer. Every index node gets 2d keys and 2d + 1 children, except possibly the last two nodes to avoid an underfull situation as before. If we have two index nodes left to construct, and have a total of m children, with 2d + 1 < m < 3d + 2, give the second-to-last node m=2 children and the remainder of the children to the last node.
	
	When choosing an integer to serve as a key inside an index node, consider the subtree corresponding to the pointer after the key. Use the smallest search key found in the leftmost leaf of this subtree.
	
	In detail, the index tree builder will firstly generate all leaf Nodes in the same level, and then does level by level traverse to generate index nodes until we reach the root node. If the tree is so small that it only has one leaf node, the index tree will finally present as a two level tree which root node has no key but only a pointer pointing to index node.
