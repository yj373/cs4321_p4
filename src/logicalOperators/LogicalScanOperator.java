package logicalOperators;

import java.util.LinkedList;
import java.util.logging.Level;

import data.DataBase;
import net.sf.jsqlparser.expression.Expression;
import util.LogicalLogger;
import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

public class LogicalScanOperator extends LogicalOperator {
	private String tableName;
	private String tableAddress;
	private String tableAliase;
	private LinkedList<String> attributes;
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableInfo table information
	 * 
	 */
	public LogicalScanOperator(String tableInfo) {
		String[] aimTable = tableInfo.split("\\s+");
		if (aimTable.length<1) {
			this.tableName = null;
			return;
		}
		this.tableName = aimTable[0];
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableAliase = aimTable[aimTable.length-1];
		this.attributes = DataBase.getInstance().getSchema(tableName);
		this.allTable.add(tableAliase);
	}
	
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
	/** get table alise*/
	public String getTableAliase() {
		return tableAliase;
	}
	
	/** get table attributes*/
	public LinkedList<String> getAttributes(){
		return attributes;
	}
	
	/** get table name*/
	public String getTableName() {
		return tableName;
	}

	/** get table address*/
	public String getTableAddress() {
		return tableAddress;
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);		
	}

	@Override
	public void printPlan(int level) {
		StringBuilder path = new StringBuilder();
		Expression exp = this.expression;
		
		if (exp!=null) {
			for (int i=0; i<level; i++) {
				path.append("-");
			}
			
			String[] array = exp.toString().split("AND");
			path.append("Select[");
			for (String str : array) {
				path.append(str.trim()).append(",");
			}
			
			path.deleteCharAt(path.length()-1);
			path.append("]");
			path.append(System.getProperty("line.separator"));
			
			level ++;
		}
		
		/* print scan operator*/
		for (int i=0; i<level; i++) {
			path.append("-");
		}
		path.append("Leaf[");
		path.append(tableName).append("]");
		
		LogicalLogger.getLogger().log(Level.SEVERE, path.toString(), new Exception());
	}

	
}
