

Part 1: Top level entry of the project.
    The main function is located at src/App/Main.java
    The SQLInterpreter will reads the input, build the query plan and thus write the output.

Part 2: Implementation of the following algoorithms and functionalities

    1. The selction pushing
    Significantly different from the instructions, we have canceled selection operators in both logical plan and physical plan; Instead, we classify all the expression constraints into "Scan conditions," which apply to the constraints in one specific relation, as well as "Join Conditions", which apply to the constraints involving more than one relations.
    
    Firstly, we apply Union-Find Algorithm on all the expressions gained from SQL Interpreter with UnionFindExpressionVisitor, and attain a series of UnionFind Objects as guided by the instruction. Then by iteratively traversing all the UnionFind Objects, we can generate a new version of expression which is richer and better targeted. That is, for all equal attributes in one Union-Find Object, the upper bound and lower bound will be universally applied, therefore expressions for each attrbute to denote their bounds will be all generated. After that, we use an  ExpressionClassifier visitor, which we implementes in p1, to split the scan conditions and join conditions by the expression, and push all the scan conditions as the constraints to their related scan Operator, and leave the join conditions as the constraints of Join operators.
	
    2. The choice of implementation for each logical selection opreator
        This functionality is implemented in the class src/util/SelectDeterminator.java. After the physical plan visits a scan operator, it will creat a SelectDeterminator 
        to determine which index to use or just use full scan (return null). We will check all the indexes related to current table, and estimate the costs for each plan using different indexes. We will estimate the cost of the full scan as well (all the pages). Then we will return the plan in a form of string. Further explaination is provided in the comments of the class. 
    
    3. The choice of join order
        This functionality is implemented in the class src/util/JoinOrderDeterminator.java. Given a list of tables that need joining, the first step is to get all the subsets of these tables. It can be implemented using DFS (function getAllSubsets()). The second step is to build a cost map using buttom-up dynamic programming. The key of the map is a subset of the tables. And the value of the map contains the following information: the optimal order of the join given these subset of tables, the cost of this order and the output size of this join order (in tuples). After building the cost map, we get the optimal join order. Further explaination is provided in the comments of the class.
    
    4. The choice of each join operator
        This functionality is implemented in the function generatePhysicalJoin() in the class src/visitors/gneratePhysicalVisitor. Based on the results of previous experiments. The SMJ operator is faster that the BNLJ operator. Considering SMJ can only be used to deal with equality join conditions, we use SMJ for all the equality join conditions. As for the other cases, we use BNLJ operator. We use 5 pages for BNLJ, and 6 pages for SMJ. Further explaination is provided in the comments of the class.
	 
Part 3: Known bugs
    1.  Rught now, we cannot print the logical plan and the physical pan without bug. And we are still working on it
