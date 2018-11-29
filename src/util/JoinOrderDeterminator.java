package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import data.DataBase;
import data.PlanCostInfo;
import data.TableStat;
import data.UfCollection;
import data.UfElement;
import operators.Operator;

public class JoinOrderDeterminator {
	private List<String> tableAliases;
	private Map<String, Integer> outputSizeMap;//Store the output size after selection, String: tableAliase
	private Map<String, UfElement> ufcMap;
	private Map<String, Set<String>> ufcDirec;//Store the table aliase and corresponding attrs in the ufcMap (with equality conditions)
	private Map<String, TableStat> statistics;
	private List<String> tableNames;
	public JoinOrderDeterminator(List<String>tNames, List<String> tList, Map<String, Integer> oMap, UfCollection u) {
		tableAliases = tList;
		outputSizeMap = oMap;
		ufcMap = u.getMap();
		ufcDirec = new HashMap<String, Set<String>>();
		statistics = DataBase.getInstance().getStatistics();
		tableNames = tNames;
		for (String key : ufcMap.keySet()) {
			UfElement uEle = ufcMap.get(key);
			if(uEle.getEqualityConstraint()!=null) {
				String[] attr = key.split(".");
				if(!ufcDirec.containsKey(attr[0])) {
					Set<String> attrs = new HashSet<String>();
					attrs.add(key);
					ufcDirec.put(attr[0], attrs);
				}else {
					Set<String> attrs = ufcDirec.get(attr[0]);
					attrs.add(key);
					ufcDirec.put(attr[0], attrs);
				}
			}
			
		}
	}
	public List<Integer> getOrder(){
		StringBuilder resKey = new StringBuilder();
		for (int i = 0; i < tableAliases.size(); i++) {
			resKey.append(String.valueOf(i));
		}
		List<HashSet<String>> subsets = new ArrayList<HashSet<String>>();
		for(int i = 0; i < tableAliases.size(); i++) {
			subsets.add(new HashSet<String>());
		}
		StringBuilder candSet = new StringBuilder();
		getAllSubsets(0, candSet, subsets);
		Map<String, PlanCostInfo> costMap = new HashMap<String, PlanCostInfo>();
		buildCostMap(costMap, subsets);
		String order = costMap.get(resKey.toString()).bestOrder;
		char[] orderChars = order.toCharArray();
		List<Integer>res = new LinkedList<Integer>();
		for (int i = 0; i < orderChars.length; i++) {
			Character ind = orderChars[i];
			res.add(Integer.parseInt(ind.toString()));
		}
		return res;
	}
	
	private void getAllSubsets(int currInd, StringBuilder candSet, List<HashSet<String>> subsets){
		if(currInd == tableAliases.size()) {
			if (candSet.length() >0) {
				int index = candSet.length()-1;
				subsets.get(index).add(candSet.toString());
			}
			return;
		}
		candSet.append(String.valueOf(currInd));
		getAllSubsets(currInd + 1, candSet, subsets);
		candSet.deleteCharAt(candSet.length()-1);
		getAllSubsets(currInd + 1, candSet, subsets);
	}
	
	private void buildCostMap(Map<String, PlanCostInfo> costMap, List<HashSet<String>> subsets) {
		Map<String, Set<String>> tempDirc = this.ufcDirec;
		for (int i = 0; i < subsets.size(); i++) {
			HashSet<String> keys = subsets.get(i);
			for(String key : keys) {
				PlanCostInfo planCost = getCost(key, costMap, tempDirc);
				costMap.put(key, planCost);
			}
		}
	}
	
	private PlanCostInfo getCost(String tables, Map<String, PlanCostInfo> costMap, Map<String,Set<String>> direc) {
		if (tables.length() == 0) return null;
		char[] tableChars = tables.toCharArray();
		if (tableChars.length == 1) {
			Character tableChar = tableChars[0];
			int tableIndex = Integer.parseInt(tableChar.toString());
			String tableAliase = tableAliases.get(tableIndex);
			int cost = 0;
			int outputSize = outputSizeMap.get(tableAliase);
			String order = String.valueOf(tableIndex);
			Set<String> allTables = new HashSet<String>();
			allTables.add(tableAliase);
			PlanCostInfo res = new PlanCostInfo(cost, outputSize, order, allTables);
			return res;
		}else{
			int cost = -1;
			String order = "";
			String rightAliase = "";
			PlanCostInfo leftPlanCost = null;
			for (int i = 0; i < tableChars.length; i++) {
				StringBuilder sb = new StringBuilder(tables);
				String left = sb.deleteCharAt(i).toString();
				PlanCostInfo tempLeftPlanCost= costMap.get(left);
				int leftCost = tempLeftPlanCost.cost;
				int leftOutput = tempLeftPlanCost.outputSize;
				int currCost = leftCost +leftOutput;
				if (cost == -1 || currCost < cost) {
					cost = currCost;
					Character tableChar = tableChars[i];
					int rightIndex = Integer.parseInt(tableChar.toString());
					rightAliase = tableAliases.get(rightIndex); 
					order = sb.append(tableChar).toString();
					leftPlanCost = tempLeftPlanCost;
				}
			}
			Set<String> leftAllTables = leftPlanCost.allTables;
			Set<String> allTables = leftAllTables;
			allTables.add(rightAliase);
			int rightOutput = outputSizeMap.get(rightAliase);
			int leftOutput = leftPlanCost.outputSize;
			long denominator = 1;
			if(direc.containsKey(rightAliase)) {
				Set<String> rightEquaAttrs = direc.get(rightAliase);
				List<String> rightRemoveList = new LinkedList<String>();
				List<String> leftRemoveList = new LinkedList<String>();
				for(String rightEqualAttr : rightEquaAttrs) {
					UfElement uEle = ufcMap.get(rightEqualAttr);
					List<String> leftEqualAttrs = uEle.getAttributes();
					for (String leftEqualAttr : leftEqualAttrs) {
						String[] splittedAttr = leftEqualAttr.split(".");
						if (leftAllTables.contains(splittedAttr[0])) {
							rightRemoveList.add(rightEqualAttr);
							leftRemoveList.add(leftEqualAttr);
						}
					}
				}
				if(!rightRemoveList.isEmpty()) {
					for (String right : rightRemoveList) {
						UfElement uEle = ufcMap.get(right);
						String rightName = tableNames.get(tableAliases.indexOf(rightAliase));
						TableStat rightStatistics = statistics.get(rightName);
						int attrInd  = rightStatistics.columns.indexOf(right);
						List<Long> lBounds = rightStatistics.lowerBound;
						List<Long> uBounds = rightStatistics.upperBound;
						Long rightLower = (uEle.getLowerBound() == null) ? lBounds.get(attrInd) : uEle.getLowerBound();
						Long rightUpper = (uEle.getUpperBound() == null) ? uBounds.get(attrInd) : uEle.getUpperBound();
						long vRight = Math.min(rightOutput, (rightUpper-rightLower+1));
						long vLeft = Math.min(leftOutput, (rightUpper-rightLower+1));
						denominator = denominator*Math.max(vRight, vLeft);
						rightEquaAttrs.remove(right);
						if(rightEquaAttrs.isEmpty()) {
							direc.remove(rightAliase);
						}else {
							direc.put(rightAliase, rightEquaAttrs);
						}
					}
				}
			}
			int outputSize = (int)(leftOutput*rightOutput/denominator);
			PlanCostInfo res = new PlanCostInfo(cost, outputSize, order, allTables);
			return res;	
		}
	}
	

}
