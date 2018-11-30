package logicalOperators;

import java.util.List;
import java.util.logging.Level;

import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.LogicalLogger;
import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

public class LogicalSortOperator extends LogicalOperator{
	private PlainSelect plainSelect;	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param plainSelect  PlainSelect of query
	 * @param op  pass in child operator
	 * 
	 */
	public LogicalSortOperator(PlainSelect plainSelect, LogicalOperator op) {

		this.plainSelect = plainSelect;
		this.leftChild = op;
		this.rightChild = null;

		for (String tableAlias : op.getAllTable()) {
			this.allTable.add(tableAlias);
		}	
	}
	
	public PlainSelect getPlainSelect() {
		return plainSelect;
	}
	
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);	
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
		
	}
	
	@Override
	public void printPlan(int level) {
		StringBuilder path = new StringBuilder();
		PlainSelect plainSelect = this.getPlainSelect();
		List<OrderByElement> list = plainSelect.getOrderByElements();
		
		for (int i=0; i<level; i++) {
			path.append("-");
		}
		path.append("Sort[");
		if (list!= null) {
			for (OrderByElement str : list) {
				path.append(str.toString()).append(",");	
			}
			path.deleteCharAt(path.length()-1);
		}

		
		path.append("]");
		
		LogicalLogger.getLogger().log(Level.SEVERE, path.toString(), new Exception());
	}
}
