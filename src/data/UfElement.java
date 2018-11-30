package data;

/**
 *  Class for every element generated from Union-Find algorithm
 *  store the following data within every element:
 *  
 *  List<String> attributes -- eg. Sailors.A, Boats.H
 *	Long lowerBound 
 *	Long upperBound
 *	Long equalityConstraint
 *  
 *  @author Xiaoxing Yan
 */

import java.util.ArrayList;
import java.util.List;


public class UfElement {
	private List<String> attributes;
	
	/* Sailors.A >= 10*/
	private Long lowerBound;
	/* Sailors.A <= 10*/
	private Long upperBound;
	/* Sailors.A == 10*/
	private Long equalityConstraint;
	

	/**
	 * Constructor
	 * @param att  the String representation of column
	 */
	public UfElement (String att) {
		attributes = new ArrayList<>();
		attributes.add(att);

		lowerBound = null;
		upperBound = null;
		equalityConstraint = null;
	}
	
	
	/** getter method to get attribute list */
	public List<String> getAttributes() {
		return attributes;
	}

	/** getter method to get lower bound*/
	public Long getLowerBound() {
		return lowerBound;
	}

	/** getter method to get upper bound*/
	public Long getUpperBound() {
		return upperBound;
	}

	/** getter method to get equal constraint*/
	public Long getEqualityConstraint() {
		return equalityConstraint;
	}

	/** setter method to set lower bound*/
	public void setLowerBound(Long lowerBound) {
		this.lowerBound = lowerBound;
	}

	/** setter method to set upper bound*/
	public void setUpperBound(Long upperBound) {
		this.upperBound = upperBound;
	}

	/** setter method to set equal constraint*/
	public void setEqualityConstraint(Long equalityConstraint) {
		this.equalityConstraint = equalityConstraint;
	}


	
	

	

}
