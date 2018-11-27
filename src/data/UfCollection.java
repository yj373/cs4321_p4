package data;


/**
 * Collections to store every UfElement
 * 
 * It provides the following API:
 * 
 *  (1) given a particular attribute, find and return the union-find element containing that attribute; 
 *  if no such element is found, create it and return it.such element is found, create it and rn
 *  (2) given two union-find elements, modify the union-find data structure so that these two elements get unioned
 *  (3) given a union-find element, set its lower bound, upper bound, or equality constraint to a particular value.
 *
 *  @author Xiaoxing Yan
 */


import java.util.HashMap;
import java.util.Map;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

public class UfCollection {
	
	/*store the relation between attribute and corresponding UfElement*/
	private Map<String, UfElement> map;
	/* store the following type of unusable expression: 
	 * S.A OP S.B (OP is not "=="  or OP is "!=" / "<>")*/
	private Expression unusableExpression;
	
	
	public UfCollection () {
		map = new HashMap<>();
		unusableExpression = null;
	}
	
	/**
	 *  Return UfElement according to the attribute 
	 *  if UfElement does not exist, create a new one and return
	 *  
	 * @param attribute
	 * @return
	 */
	public UfElement getUfElement(String attribute) {
			if (map.get(attribute) == null) {
				UfElement cur = new UfElement(attribute);
				map.put(attribute, cur);
			}
			
			return map.get(attribute);
	}
	
	/**
	 * set unusable expression
	 * @param express
	 */
	public void setUnusableExpression (Expression express) {
		if (unusableExpression == null) {
			unusableExpression = express;
		} else {
			unusableExpression = new AndExpression(unusableExpression, express);
		}
	}
	
	/** getter method to get unusable expression which combined with AND Expression*/
	public Expression getUnusableExpression (){
		return unusableExpression;
	}
	
	/** getter method to get map*/
	public Map<String, UfElement> getMap (){
		return map;
	}
	
	/**
	 *  merge two UfElement by merge attribute lists and related fields
	 *  
	 * @param a
	 * @param b
	 */
	public void mergeUfElement (UfElement a, UfElement b) {

		
		/*check attributes*/
		for (String str : b.getAttributes()) {
			a.getAttributes().add(str);
			map.put(str, a);
		}
		/*check lower bound*/
		Long aLower = a.getLowerBound();
		Long bLower = b.getLowerBound();
		a.setLowerBound(aLower == null ? bLower : aLower);
		
		/*check upper bound*/
		Long aUpper = a.getUpperBound();
		Long bUpper = b.getUpperBound();
		a.setUpperBound(aUpper == null ? bUpper : aUpper);
		
		/*check equality constraint*/
		Long aEqual = a.getEqualityConstraint();
		Long bEqual = b.getEqualityConstraint();
		a.setEqualityConstraint(aEqual == null ? bEqual : aEqual);
	
	}
	
	/** setter method to set lower bound*/
	public void setUfElementLowerBound (String attribute, Long value) {
		UfElement cur = getUfElement(attribute);
		cur.setLowerBound(value);
	}
	
	/** setter method to set upper bound*/
	public void setUfElementUpperBound (String attribute, Long value) {
		UfElement cur = getUfElement(attribute);
		cur.setUpperBound(value);
	}
	
	/** setter method to set equality constraints*/
	public void setUfElementEqualityConstraint (String attribute, Long value) {
		UfElement cur = getUfElement(attribute);
		cur.setEqualityConstraint(value);
	}
	

}
