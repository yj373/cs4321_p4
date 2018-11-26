package data;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;

public class UfElement {
	private List<String> attributes;
	private Integer lowerBound;//inclusive
	private Integer upperBound;
	private Integer equalityConstraint;
	
//	public UfElement () {
//		attributes = new ArrayList<>();
//		lowerBound = null;
//		upperBound = null;
//		equalityConstraint = null;
//	}
//	
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

	public Integer getLowerBound() {
		return lowerBound;
	}

	public Integer getUpperBound() {
		return upperBound;
	}

	public Integer getEqualityConstraint() {
		return equalityConstraint;
	}

	public void setLowerBound(Integer lowerBound) {
		this.lowerBound = lowerBound;
	}

	public void setUpperBound(Integer upperBound) {
		this.upperBound = upperBound;
	}

	public void setEqualityConstraint(Integer equalityConstraint) {
		this.equalityConstraint = equalityConstraint;
	}


	
	

	

}
