package logicalOperators;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import util.LogicalLogger;
import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

public class LogicalProjectOperator extends LogicalOperator{
	/*store information of needed attributes*/
	private List<SelectItem> selectItems;
	/*check that whether return all attributes or return specific attributes*/
	private boolean allColumns = false;
	private List<String> aliasOrder = new ArrayList<>();
	/** 
	 * This method is a constructor which is to
	 * get corresponding columns information and initialize childOp.
	 * 
	 * @param plainSelect  PlainSelect of query
	 * @param op  pass in child operator
	 * 
	 */

	public LogicalProjectOperator(PlainSelect plainSelect,LogicalOperator op) {
		this.leftChild = op;
		this.rightChild = null;
		for (String e : op.getAllTable()) {
			allTable.add(e);
		}
		selectItems = plainSelect.getSelectItems();
		if (selectItems.get(0).toString() == "*") {
			allColumns = true;
		} 		
		
		if (plainSelect.getFromItem() != null) {
			String mainTableInfo = plainSelect.getFromItem().toString();
			String[] aimTable = mainTableInfo.split("\\s+");
			aliasOrder.add(aimTable[aimTable.length - 1]);
		}
		
		if (plainSelect.getJoins() != null) {
			for (Object o : plainSelect.getJoins()) {
			    // join the root with the new coming tables
				String joinItem = o.toString();
				String[] joinTable = joinItem.split("\\s+");
				aliasOrder.add(joinTable[joinTable.length - 1]);
			}
		}
	}
	
	public List<String> getAliasOrder() {
		return aliasOrder;
	}
	
	public boolean isAllCol() {
		return allColumns;
	}
	
	public List<SelectItem> getSelectItems() {
		return selectItems;
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
		List<SelectItem> list = this.getSelectItems();
		
		
		for (int i=0; i<level; i++) {
			path.append("-");
		}
		path.append("Project[");
		for (SelectItem str : list) {
			path.append(str.toString()).append(",");	
		}
		path.deleteCharAt(path.length()-1);
		path.append("]");
		
		LogicalLogger.getLogger().log(Level.SEVERE, path.toString(), new Exception());
	}
	
}
