package util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import data.DataBase;
import data.IndexNote;
import data.TableStat;
import logicalOperators.LogicalScanOperator;

public class SelectDeterminator {
	/*Target logical scan oprator*/
	private LogicalScanOperator lScan;
	/*Store the index information related to this table, String: Sailres.A*/
	private Map<String, IndexNote> indMap;
	/*The number of tuples in this table */
	private int t;
	/*The number of pages in this table*/
	private int p;
	/*Reduction factor map*/
	private Map<String, Float> redMap;
	
	public SelectDeterminator(LogicalScanOperator ls) {
		this.lScan = ls;
		this.indMap = new HashMap<String, IndexNote>();
		String tableName = lScan.getTableName();
		Map<String, List<IndexNote>> indexInfoRoster = DataBase.getInstance().getIndexInfos();
		List<IndexNote> indexedColumns = indexInfoRoster.get(tableName);
		for (IndexNote in : indexedColumns) {
			StringBuilder sb = new StringBuilder();
			sb.append(tableName);
			sb.append(".");
			sb.append(in.getColumn());
			indMap.put(sb.toString(), in);
			
		}
		Map<String, TableStat> statistics = DataBase.getInstance().getStatistics();
		t = statistics.get(tableName).tupleNumber;
		int tupleSize = DataBase.getInstance().getSchema(tableName).size()*4;
		p = t*tupleSize/4096;
		
		
	}
	public String selectColumn() {
		String tableName = this.lScan.getTableName();
		Map<String, Integer> indexLeaves = DataBase.getInstance().getIndexLeaves();
		Map<String, TableStat> statistics = DataBase.getInstance().getStatistics();
		String res = null;
		int cost = p;
		for (String candCol : indMap.keySet()) {
			boolean isClustered = checkClustered(candCol);
			if (isClustered) {
				
			}
		}
		
		return null;
	}
	/**
	 * Check this column is clustered or not
	 * @param column (eg. Sailors.A)
	 * @return clustered or not
	 */
	public boolean checkClustered(String column) {
		if (column == null) return false;
		else if (indMap.containsKey(column)) {
			return indMap.get(column).isClustered();
		}
		return false;
	}
	

}
