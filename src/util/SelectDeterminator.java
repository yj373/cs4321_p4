package util;

import java.util.HashMap;
import java.util.Map;

import data.DataBase;
import data.IndexNote;
import logicalOperators.LogicalScanOperator;

public class SelectDeterminator {
	LogicalScanOperator lScan;
	public SelectDeterminator(LogicalScanOperator ls) {
		this.lScan = ls;
	}
	public String selectColumn() {
		String tableName = this.lScan.getTableName();
		Map<String, Integer> indexLeavex = DataBase.getInstance().getIndexLeaves();
		Map<String, IndexNote> indexInfoRoster = DataBase.getInstance().getIndexInfos();
		
		return null;
	}
	

}
