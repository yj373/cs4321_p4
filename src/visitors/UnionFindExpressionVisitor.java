package visitors;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

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

public class UnionFindExpressionVisitor implements ExpressionVisitor {

	private UfCollection ufCollections;


	public UnionFindExpressionVisitor () {
		ufCollections = new UfCollection();
	}

	public Expression generateExpression () {
		//de-duplicate
		return null;
	}

	public Expression getUnusableExpression (){
		return ufCollections.getUnusableExpression();
	}

	public Map<String, UfElement> getMap (){
		return ufCollections.getMap();
	}


	@Override
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
				}
			}
		}
	}

		@Override
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
				
				Integer value;
				String[] att = new String[1];
				if (arg0.getLeftExpression() instanceof Column) {
				    att[0] = arg0.getLeftExpression().toString();
				    value =  Integer.valueOf(arg0.getRightExpression().toString());
				} else {
					att[0] = arg0.getRightExpression().toString();
					value =  Integer.valueOf(arg0.getLeftExpression().toString());
				}

				UfElement cur = ufCollections.getUfElement(att[0]);
				cur.setEqualityConstraint(value);
				cur.setLowerBound(value);
				cur.setUpperBound(value);

			}

		}

		@Override
		public void visit(NotEqualsTo arg0) {
			
			ufCollections.setUnusableExpression(arg0);

		}
		
		@Override
		public void visit(GreaterThan arg0) {
			
			

		}

		@Override
		public void visit(GreaterThanEquals arg0) {
			// TODO Auto-generated method stub

		}


		@Override
		public void visit(MinorThan arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(MinorThanEquals arg0) {
			// TODO Auto-generated method stub

		}



		@Override
		public void visit(Column arg0) {
			// TODO Auto-generated method stub

		}



		/********************************************************************************************************************/
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
