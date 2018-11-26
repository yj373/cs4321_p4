package data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

public class UfCollection {
	
	/*store the relation between attribute and corresponding UfElement*/
	private Map<String, UfElement> map;
	private Expression unusableExpression;
	
	public UfCollection () {
		map = new HashMap<>();
		unusableExpression = null;
	}
	
	public UfElement getUfElement(String attribute) {
			if (map.get(attribute) == null) {
				UfElement cur = new UfElement(attribute);
				map.put(attribute, cur);
			}
			
			return map.get(attribute);
	}
	
	public void setUnusableExpression (Expression express) {
		if (unusableExpression == null) {
			unusableExpression = express;
		} else {
			unusableExpression = new AndExpression(unusableExpression, express);
		}
	}
	
	public Expression getUnusableExpression (){
		return unusableExpression;
	}
	
	public Map<String, UfElement> getMap (){
		return map;
	}
	
	public void mergeUfElement (UfElement a, UfElement b) {

		
		/*check attributes*/
		for (String str : b.getAttributes()) {
			a.getAttributes().add(str);
			map.put(str, a);
		}
		/*check lower bound*/
		Integer aLower = a.getLowerBound();
		Integer bLower = b.getLowerBound();
		a.setLowerBound(aLower == null ? bLower : aLower);
		
		/*check upper bound*/
		Integer aUpper = a.getUpperBound();
		Integer bUpper = b.getUpperBound();
		a.setUpperBound(aUpper == null ? bUpper : aUpper);
		
		/*check equality constraint*/
		Integer aEqual = a.getEqualityConstraint();
		Integer bEqual = b.getEqualityConstraint();
		a.setEqualityConstraint(aEqual == null ? bEqual : aEqual);
	
	}
	
	public void setUfElementLowerBound (String attribute, Integer value) {
		UfElement cur = getUfElement(attribute);
		cur.setLowerBound(value);
	}
	
	public void setUfElementUpperBound (String attribute, Integer value) {
		UfElement cur = getUfElement(attribute);
		cur.setUpperBound(value);
	}
	
	public void setUfElementEqualityConstraint (String attribute, Integer value) {
		UfElement cur = getUfElement(attribute);
		cur.setEqualityConstraint(value);
	}
	

}
