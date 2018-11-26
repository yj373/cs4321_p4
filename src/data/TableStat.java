package data;

import java.util.ArrayList;
import java.util.List;

public class TableStat {
    public String tableName;
    public Integer tupleNumber;
    public List<String> columns = new ArrayList<>();
    public List<Long> lowerBound = new ArrayList<>();
    public List<Long> upperBound = new ArrayList<>();
    
    public TableStat(String tableName, List<String> columns) {
    	this.tableName = tableName;
    	this.columns = columns;
    }
    
    @Override
    public String toString() {
    	StringBuilder sbTableStat = new StringBuilder(tableName);
    	sbTableStat.append(" ").append(tupleNumber);
    	for (int i = 0; i < lowerBound.size(); i++) {
    		sbTableStat.append(" ");
    		sbTableStat.append(columns.get(i));
    		sbTableStat.append(",");
    		sbTableStat.append(lowerBound.get(i));
    		sbTableStat.append(",");
    		sbTableStat.append(upperBound.get(i));
    	}
    	return sbTableStat.toString();
    }
}
