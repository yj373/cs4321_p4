package util;

import java.util.ArrayList;
import java.util.List;

public class JoinOrderDeterminator {
	List<String> tableList;
	public JoinOrderDeterminator(List<String> tList) {
		tableList = tList;
	}
	public List<Integer> getOrder(){
		List<Integer> res = new ArrayList<Integer>();
		for (int i = 0; i < tableList.size(); i++) {
			res.add(i);
		}
		return res;
	}
	

}
