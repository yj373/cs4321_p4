package visitors;

/**
 * Given a query, using this visitor to walk every Expression after Where clause
 * and build up a union-find collection.
 * Classify different Expression and return optimized Expression.
 * 
 * It should process each comparison as follows:
 * 
 * (1) if the comparison has the form att1 = att2, find the two elements containing att1 and att2 and union them.
 * (2) if the comparison has the form att OP val, where val is an integer and OP is =; <;<=;>=;>,  
 *  find the element containing att and update the appropriate numeric bound in the element.
 * (3) if the comparison has any other form, it is an unusable comparison; put it aside for separate processing later as it does not go into the union-find.
 *
 * @author Xiaoxing Yan
 */

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import data.Dynamic_properties;
import data.UfCollection;
import data.UfElement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import util.GlobalLogger;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class UnionFindExpressionVisitor implements ExpressionVisitor {

	/* collection to store all UfElements*/
	private UfCollection ufCollections;


	/**
	 * Constructor
	 */
	public UnionFindExpressionVisitor () {
		ufCollections = new UfCollection();
	}

	/**
	 * iterate over unusable expression and every UfElement 
	 * generate new optimized expression
	 * 
	 * @return
	 */
	public Expression generateExpression () {
		/*de-duplicate*/
		Map<String, UfElement> map = ufCollections.getMap();
		Set<UfElement> set = new HashSet<>();
		for (UfElement cur : map.values()) {
			set.add(cur);
		}


		Expression finalExpress = ufCollections.getUnusableExpression();

		/*iterate every attriubte in this map*/
		for (UfElement cur : set) {

			/* append numerical constraints*/
			for (String att : cur.getAttributes()) {


				if (cur.getEqualityConstraint() == null) {
					/* append lower bound constraint*/
					if (cur.getLowerBound() != null) {
						Column column = generateColumn (att);
						Expression val = new LongValue(String.valueOf(cur.getLowerBound()));
						Expression loExpre = new GreaterThanEquals(column, val);

						if (finalExpress == null) {
							finalExpress = loExpre;
						} else {
							finalExpress = new AndExpression(finalExpress, loExpre);
						}

					}
					/* append upper bound constraint*/
					if (cur.getUpperBound() != null) {

						Column column = generateColumn (att);
						Expression val = new LongValue(String.valueOf(cur.getUpperBound()));
						Expression upExpre = new MinorThanEquals(column, val);

						if (finalExpress == null) {
							finalExpress = upExpre;
						} else {
							finalExpress = new AndExpression(finalExpress, upExpre);
						}

					}

				} else {
					/* append equality constraint to every attribute*/
					Column column = generateColumn (att);
					
					Expression val = new LongValue(String.valueOf(cur.getEqualityConstraint()));
					Expression eqExpre = new EqualsTo(column, val);

					if (finalExpress == null) {
						finalExpress = eqExpre;
					} else {
						finalExpress = new AndExpression(finalExpress, eqExpre);
					}

				}

			}


			/* append equality constraints between attributes*/
			List<String> attList = cur.getAttributes();
			if (attList.size() > 1) {
				for (int i=0; i< attList.size(); i++) {
					for (int j=i+1; j <attList.size(); j++) {
						
						Column column1 = generateColumn (attList.get(i));
						Column column2 = generateColumn (attList.get(j));
						Expression eqExpre = new EqualsTo(column1, column2);

						if (finalExpress == null) {
							finalExpress = eqExpre;
						} else {
							finalExpress = new AndExpression(finalExpress, eqExpre);
						}

					}
				}
			}


		}

		return finalExpress;
	}

	/**
	 *  helper function to generate new Column Expression
	 */
	public Column generateColumn (String str) {

		String[] names = str.split("\\.");
		Column column  = new Column();
		Table t = new Table();
		t.setName(names[0]);
		column.setTable(t);
		column.setColumnName(names[1]);
		
		return column;
	}
	
	/** getter method to get unusable expression*/
	public Expression getUnusableExpression (){
		return ufCollections.getUnusableExpression();
	}

	/** getter method to get map*/
	public Map<String, UfElement> getMap (){
		return ufCollections.getMap();
	}


	@Override
	/**
	 * visit AndExpression
	 */
	public void visit(AndExpression arg0) {

		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);

	}

	public static void main(String[] args) throws FileNotFoundException, ParseException {

		CCJSqlParser parser = new CCJSqlParser(new FileReader(Dynamic_properties.queryPath));
		Statement statement;
		while ((statement = parser.Statement()) != null) {
			GlobalLogger.getLogger().info("Read statement: " + statement);
			//System.out.println("Read statement: " + statement);
			Select select = (Select) statement;
			PlainSelect ps = (PlainSelect)select.getSelectBody();

			UnionFindExpressionVisitor visitor = new UnionFindExpressionVisitor();
			if (ps != null) {
				Expression origin = ps.getWhere();

				if (origin != null) {
					origin.accept(visitor);
					System.out.println("finish");
				}
			}

			Expression te = visitor.generateExpression();
			System.out.println("finish2");
		}
	}


	@Override
	/**
	 * visit EqualsTo Expression
	 */
	public void visit(EqualsTo arg0) {
		/* if left node and right node are both columns
		 * merger two UfElements
		 */
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {

			String att1 = arg0.getLeftExpression().toString();
			String att2 = arg0.getRightExpression().toString();
			UfElement a = ufCollections.getUfElement(att1);
			UfElement b = ufCollections.getUfElement(att2);
			ufCollections.mergeUfElement(a, b);

		} else {
			/* if one node is column and another is column 
			 * reset the equality constraint in UfElement
			 */

			Long value;
			String[] att = new String[1];
			if (arg0.getLeftExpression() instanceof Column) {
				att[0] = arg0.getLeftExpression().toString();
				value =  Long.valueOf(arg0.getRightExpression().toString());
			} else {
				att[0] = arg0.getRightExpression().toString();
				value =  Long.valueOf(arg0.getLeftExpression().toString());
			}

			/* equality is the most strong constraint which change the value of both lower bound and upper bound*/
			UfElement cur = ufCollections.getUfElement(att[0]);
			cur.setEqualityConstraint(value);
			cur.setLowerBound(value);
			cur.setUpperBound(value);

		}

	}

	@Override
	/**
	 * visit NotEqualsTo Expression
	 */
	public void visit(NotEqualsTo arg0) {

		/* add expression to unusable expression*/
		ufCollections.setUnusableExpression(arg0);

	}

	@Override
	/**
	 * visit GreaterThan Expression
	 */
	public void visit(GreaterThan arg0) {

		/* both side are columns*/
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			ufCollections.setUnusableExpression(arg0);
		} else {

			boolean leftIsColumn = true;
			Long value;
			String[] att = new String[1];
			/* eg. sailors.A > 5 -- reset lower bound*/
			if (arg0.getLeftExpression() instanceof Column) {
				att[0] = arg0.getLeftExpression().toString();
				value =  Long.valueOf(arg0.getRightExpression().toString()) + 1;

			} else {
				/* eg.  5 > sailors.A-- reset upper bound*/
				leftIsColumn = false;
				att[0] = arg0.getRightExpression().toString();
				value =  Long.valueOf(arg0.getLeftExpression().toString()) - 1;
			}
			UfElement cur = ufCollections.getUfElement(att[0]);
			if (cur.getEqualityConstraint() == null) {
				if (leftIsColumn) {
					if (cur.getLowerBound() == null) {
						cur.setLowerBound(value);
					} else {
						cur.setLowerBound(Math.max(cur.getLowerBound(), value));
					}

				} else {
					if (cur.getUpperBound() == null) {
						cur.setUpperBound(value);
					} else {
						cur.setUpperBound(Math.min(cur.getUpperBound(), value));
					}
				}
			}
		}


	}

	@Override
	/**
	 * visit GreaterThanEquals Expression
	 */
	public void visit(GreaterThanEquals arg0) {
		/* both side are columns*/
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			ufCollections.setUnusableExpression(arg0);
		} else {

			boolean leftIsColumn = true;
			Long value;
			String[] att = new String[1];
			/* eg. sailors.A >= 5 -- reset lower bound*/
			if (arg0.getLeftExpression() instanceof Column) {
				att[0] = arg0.getLeftExpression().toString();
				value =  Long.valueOf(arg0.getRightExpression().toString());

			} else {
				/* eg.  5 >= sailors.A-- reset upper bound*/
				leftIsColumn = false;
				att[0] = arg0.getRightExpression().toString();
				value =  Long.valueOf(arg0.getLeftExpression().toString());
			}
			UfElement cur = ufCollections.getUfElement(att[0]);
			if (cur.getEqualityConstraint() == null) {
				if (leftIsColumn) {
					if (cur.getLowerBound() == null) {
						cur.setLowerBound(value);
					} else {
						cur.setLowerBound(Math.max(cur.getLowerBound(), value));
					}

				} else {
					if (cur.getUpperBound() == null) {
						cur.setUpperBound(value);
					} else {
						cur.setUpperBound(Math.min(cur.getUpperBound(), value));
					}
				}
			}
		}

	}


	@Override
	/**
	 * visit MinorThan Expression
	 */
	public void visit(MinorThan arg0) {

		/* both side are columns*/
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			ufCollections.setUnusableExpression(arg0);
		} else {

			boolean leftIsColumn = true;
			Long value;
			String[] att = new String[1];
			/* eg. sailors.A < 5 -- reset upper bound*/
			if (arg0.getLeftExpression() instanceof Column) {
				att[0] = arg0.getLeftExpression().toString();
				value =  Long.valueOf(arg0.getRightExpression().toString()) - 1;

			} else {
				/* eg.  5 < sailors.A-- reset lower bound*/
				leftIsColumn = false;
				att[0] = arg0.getRightExpression().toString();
				value =  Long.valueOf(arg0.getLeftExpression().toString()) + 1;
			}
			UfElement cur = ufCollections.getUfElement(att[0]);
			if (cur.getEqualityConstraint() == null) {
				if (leftIsColumn) {

					if (cur.getUpperBound() == null) {
						cur.setUpperBound(value);
					} else {
						cur.setUpperBound(Math.min(cur.getUpperBound(), value));
					}

				} else {

					if (cur.getLowerBound() == null) {
						cur.setLowerBound(value);
					} else {
						cur.setLowerBound(Math.max(cur.getLowerBound(), value));
					}



				}
			}
		}


	}

	@Override
	/**
	 * visit MinorThanEquals Expression
	 */
	public void visit(MinorThanEquals arg0) {


		/* both side are columns*/
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			ufCollections.setUnusableExpression(arg0);
		} else {

			boolean leftIsColumn = true;
			Long value;
			String[] att = new String[1];
			/* eg. sailors.A <= 5 -- reset upper bound*/
			if (arg0.getLeftExpression() instanceof Column) {
				att[0] = arg0.getLeftExpression().toString();
				value =  Long.valueOf(arg0.getRightExpression().toString());

			} else {
				/* eg.  5 <= sailors.A-- reset lower bound*/
				leftIsColumn = false;
				att[0] = arg0.getRightExpression().toString();
				value =  Long.valueOf(arg0.getLeftExpression().toString());
			}
			UfElement cur = ufCollections.getUfElement(att[0]);
			if (cur.getEqualityConstraint() == null) {
				if (leftIsColumn) {

					if (cur.getUpperBound() == null) {
						cur.setUpperBound(value);
					} else {
						cur.setUpperBound(Math.min(cur.getUpperBound(), value));
					}

				} else {

					if (cur.getLowerBound() == null) {
						cur.setLowerBound(value);
					} else {
						cur.setLowerBound(Math.max(cur.getLowerBound(), value));
					}



				}
			}
		}



	}

	/********************************************************************************************************************/

	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub

	}


	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub

	}


	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub

	}


	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub

	}
}
