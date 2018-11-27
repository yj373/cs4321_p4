package data;

/**
 *  Class for every element generated from Union-Find algorihtm
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
import net.sf.jsqlparser.schema.Column;

public class UfElement {
	private List<String> attributes;
	private Long lowerBound;//inclusive
	private Long upperBound;
	private Long equalityConstraint;
	

	public UfElement (String att) {
		attributes = new ArrayList<>();
		attributes.add(att);

		lowerBound = null;
		upperBound = null;
		equalityConstraint = null;
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public Long getLowerBound() {
		return lowerBound;
	}

	public Long getUpperBound() {
		return upperBound;
	}

	public Long getEqualityConstraint() {
		return equalityConstraint;
	}

	public void setLowerBound(Long lowerBound) {
		this.lowerBound = lowerBound;
	}

	public void setUpperBound(Long upperBound) {
		this.upperBound = upperBound;
	}

	public void setEqualityConstraint(Long equalityConstraint) {
		this.equalityConstraint = equalityConstraint;
	}


	
	

	

}
