package logicalOperators;

import java.util.List;
import java.util.Map;

import data.TablePair;
import net.sf.jsqlparser.expression.Expression;
import visitors.ExpressionClassifyVisitor;
import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

/**
 * This class is a logical join operator. It has left child and right child as its child 
 * logical operators, and also a field of table list to track all table aliases involved 
 * in this join operator. An join operator will be initialized in LogicalPlanBuiler, and
 * later will be visited by PhysicalPlanVisitor to generate the physical plan. 
 * 
 * @author Ruoxuan Xu
 *
 */
public class LogicalJoinOperator extends LogicalOperator {
	
	/** p4 update: logical joinOperator which contains a list of children
	 * and has no specific join Order*/
	public List<LogicalOperator> childList = null;
	private Map<TablePair, Expression> joinConditions;
	
	/** p4 update: logical joinOperator which contains a list of children
	 * and has no specific join Order*/
	public LogicalJoinOperator(List<LogicalOperator> childList) {
		this.childList = childList;
	}
	
    /**
     * Construct a LogicalJoinOperator instance, with its left child to be op1, and 
     * right child to be op2.
     * @param op1
     * @param op2
     */
	public LogicalJoinOperator(LogicalOperator op1, LogicalOperator op2) {
		if (op1 != null) {
			for (String e : op1.getAllTable()) {
				this.allTable.add(e);
			}
		}
		if (op2 != null) {
			for (String e : op2.getAllTable()) {
				this.allTable.add(e);
			}
		}
    	leftChild = op1;
    	rightChild = op2;
	}
	
	/**
	 * p4 update: get the childList of this abstact logical JoinOperator
	 */
	public List<LogicalOperator> getChildList() {
		return this.childList;
	}
	
	/**
	 * p4 update: set the joinConditions with ExpressionClassifyVisitor classifier
	 */
	public void setJoinConditions(Map<TablePair, Expression> joinConditions) {
		this.joinConditions = joinConditions;
	}
	
	
	/**
	 * p4 update: get the joinConditions of the abstract logical JoinOperator
	 */
	public Map<TablePair, Expression> getjoinConditions() {
		return this.joinConditions;
	}
	
	/**
	 * Accept the visitor of logicalPlanVisitor
	 */
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
	/**
	 * get LeftChild Operator
	 * @return the leftChild
	 */
	@Override
	public LogicalOperator getLeftChild() {
		return leftChild;
	}
	
	/**
	 * get rightChild Operator
	 * @return the rightChild
	 */
	@Override
	public LogicalOperator getRightChild() {
		return rightChild;
	}

	/**
	 * Accept the visitor of PhysicalPlanVisitor
	 */
	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
		
	}

}
