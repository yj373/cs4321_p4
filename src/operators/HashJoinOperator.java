package operators;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import data.Dynamic_properties;
import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import util.TupleReader;
import util.TupleWriter;
import visitors.LocateExpressionVisitor;

/**
 * There are three phases for Hash join 
 * 
 * (1) Partitioning Phase: given two tables R and S, partitioning two tables separately and save temporary 
 * partition file on disk
 * (2) Building Phase: building hash table according to the hash key of small table, say R table, for each partition
 * (3) Probing Phase: probing S table using hash function and adding concatenated tuple to message queue,
 * get the next tuple from message queue
 */



public class HashJoinOperator extends JoinOperator{



	//	protected Operator leftChild;
	//	protected Operator rightChild;
	//	protected Expression exp;
	//	protected String name;
	//	
	//	protected Map<String, Integer> schema;


	/* define the bucket size used in two hash functions*/
	private static final int PARTITION_BUCKET_SIZE = 10;
	private static final int BUILDING_BUCKET_SIZE = 50;

	/* fetch tuples from messageQueue*/
	private Queue<Tuple> messageQueue;

	/* input buffer*/
	private Buffer inputBuffer = null;
	private boolean partitionEnd = false;

	/* store temporary file of partition*/
	private Map<Integer, TupleWriter> leftBucketWriters;
	private Map<Integer, TupleWriter> rightBucketWriters;

	/* store attributes in equality condition*/
	private List<String> leftRelationColumns;
	private List<String> rightRelationColumns;

	/* schema for tuples from left relation and right relation respectively */
	private Map<String, Integer> leftSchema;
	private Map<String, Integer> rightSchema;
	
	
	/* address for temporary file*/
	private String leftFileAddress = Dynamic_properties.tempPath +"/"+ "hash_left_"+ this.hashCode()+"_";
	private String rightFileAddress = Dynamic_properties.tempPath +"/"+ "hash_right_"+ this.hashCode()+"_";

	/* extract the columns related to op1 from expression
	 * extract the columns related to op2 from expression, corresponding to sequence of op1
	 */
	public HashJoinOperator(Operator op1, Operator op2, Expression expression) {
		super(op1, op2, expression);

		StringBuilder sb = new StringBuilder();
		sb.append("hashjoin-");
		sb.append(op1.name);
		sb.append("-");
		sb.append(op2.name);
		name = sb.toString();
		if (exp != null) {
			LocateExpressionVisitor locator = new LocateExpressionVisitor(op1.schema, op2.schema);
			exp.accept(locator);
			leftRelationColumns = locator.leftSortColumns();
			rightRelationColumns = locator.rightSortColumns();
		}

		//这个之后再看 可能有问题
		messageQueue = new LinkedList<>();



		try {
			partitionPhase();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	public void partitionPhase () throws IOException {

		leftBucketWriters = new HashMap<>();
		rightBucketWriters = new HashMap<>();
		/* initilize partitioning buckets*/
		for (int i = 0; i < PARTITION_BUCKET_SIZE; i++) {

			//tempPath = "src/samples/temp"
			//leftFileAddress = Dynamic_properties.tempPath + this.hashCode()+ "hashjoin_left_bucket_";
			String tempAddress = leftFileAddress + i;

			//如果删除不成功 就换成这个
			//String tempAddress = Dynamic_properties.tempPath + "/hashjoin——left/bucket__" +this.hashCode() + i;
			//不能确定这部分是否正确
//			File tempDir = new File(tempAddress);
//			if(!tempDir.exists()) {
//				tempDir.mkdirs();
//			}

			TupleWriter tupleWriter1 = new TupleWriter(tempAddress);
			leftBucketWriters.put(i, tupleWriter1);

			String tempAddress2 = rightFileAddress + i;
//			File tempDir2 = new File(tempAddress);
//			if(!tempDir2.exists()) {
//				tempDir2.mkdirs();
//			}

			TupleWriter tupleWriter2 = new TupleWriter(tempAddress2);
			rightBucketWriters.put(i, tupleWriter2);
		}



		/* partition left relation*/
		while (!partitionEnd) {
			loadTuples(this.leftChild);
			/* partition */
			for (int i=0; i< inputBuffer.getPosition(); i++) {
				Tuple cur = inputBuffer.get(i);
				if (leftSchema == null) {
					leftSchema = cur.getSchema();
				}
				int hashcode = leftHashCode1(cur);
				leftBucketWriters.get(hashcode).writeTuple(cur);	
			}
			inputBuffer.reset();
		}

		partitionEnd = false;
		/* partition right relation*/
		while (!partitionEnd) {
			loadTuples(this.rightChild);
			for (int i=0; i< inputBuffer.getPosition(); i++) {
				Tuple cur = inputBuffer.get(i);
				if (rightSchema == null) {
					rightSchema = cur.getSchema();
				}
				int hashcode = rightHashCode1(cur);
				rightBucketWriters.get(hashcode).writeTuple(cur);
			}
			inputBuffer.reset();
		}


		/* close files */
		for (int i = 0; i < PARTITION_BUCKET_SIZE; i++) {
			leftBucketWriters.get(i).writeTuple(null);
			rightBucketWriters.get(i).writeTuple(null);
		}


		try {
			BuildingPhase();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 * Building hash table and find matched tuple for each partition
	 * @throws Exception 
	 */
	public void BuildingPhase() throws Exception {


		//ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		for (int i = 0; i < PARTITION_BUCKET_SIZE; i++) {
			Task task = new Task(i);
			task.run(messageQueue);
		}


	}


	class Task {

		private TupleReader leftReader;
		private TupleReader rightReader;

		private Map<Integer, List<Tuple>> hashTable;



		//初始的时候把每个分区读进来 然后建立hashtable 
		//run的时候开始遍历右边 把符合条件的放到message queue
		public Task(int i) {
			String leftAddress = leftFileAddress + i;
			leftReader = new TupleReader(leftAddress, leftSchema);
			String rightAddress = rightFileAddress + i;
			rightReader = new TupleReader(rightAddress, rightSchema);


			hashTable = new HashMap<>();

			/* assume that the memory is enough to load all data in this partition*/
			for (int j = 0; j < BUILDING_BUCKET_SIZE; j++) {
				hashTable.put(j, new ArrayList<>());
			}

			/* building hash table*/
			try {
				Tuple tuple = leftReader.readNextTuple();
				while (tuple != null) {
					hashTable.get(leftHashCode2(tuple)).add(tuple);
					tuple = leftReader.readNextTuple();
				}
				leftReader.close();
			}

			catch (Exception e) {
				e.printStackTrace();
			}

		}


		public void run (Queue<Tuple> messageQueue) throws Exception {

			// 这一句的位置*******
			Tuple rightTuple = rightReader.readNextTuple();
			while (rightTuple != null) {
				int hash = rightHashCode2(rightTuple);

				/* hash value does not exist*/
				if (hashTable.get(hash) == null) {
					rightTuple = rightReader.readNextTuple();
					continue;
				}

				/* evaluate join condition for every pair of tuples*/
				for (Tuple leftTuple : hashTable.get(hash)) {
					boolean match = true;
					for (int k = 0; k < leftRelationColumns.size(); k++) {
						int index1 = leftTuple.getSchema().get(leftRelationColumns.get(k));
						long data1 = leftTuple.getData()[index1];

						int index2 = rightTuple.getSchema().get(rightRelationColumns.get(k));
						long data2 = rightTuple.getData()[index2];

						if (data1 != data2) {
							match = false;
							break;
						}


					}
					/* if two tuples cannot match, moving to the next pair*/
					if (!match) {
						continue;
					}
					Tuple newTuple = concatenate(leftTuple, rightTuple);
					messageQueue.offer(newTuple);
				}

				//这一句可能后面要删除
				rightTuple = rightReader.readNextTuple();
			}


		}

	}

	public void loadTuples (Operator op) {
		Tuple cur = op.getNextTuple();

		if (inputBuffer == null) {
			if (cur != null) {
				inputBuffer = new Buffer(cur);
				inputBuffer.addTuple(cur);
				cur = op.getNextTuple();

			}
		} 

	
		while (!inputBuffer.isFull()) {
			if (cur == null) {
				partitionEnd = true;
				break;
			}
			inputBuffer.addTuple(cur);
			cur = op.getNextTuple();
		}

	}




	private int leftHashCode1(Tuple tuple) {
		int sum = 0;
		for (String col : leftRelationColumns) {
			int index = tuple.getSchema().get(col);
			long data = tuple.getData()[index];
			sum += data;
		}
		return sum % PARTITION_BUCKET_SIZE;
	}

	private int rightHashCode1(Tuple tuple) {
		int sum = 0;
		for (String col : rightRelationColumns) {
			int index = tuple.getSchema().get(col);
			long data = tuple.getData()[index];
			sum += data;
		}
		return sum % PARTITION_BUCKET_SIZE;
	}

	private int leftHashCode2(Tuple tuple) {
		int sum = 0;
		for (String col : leftRelationColumns) {
			int index = tuple.getSchema().get(col);
			long data = tuple.getData()[index];
			sum += data;
		}
		return sum % BUILDING_BUCKET_SIZE;
	}

	private int rightHashCode2(Tuple tuple) {
		int sum = 0;
		for (String col : rightRelationColumns) {
			int index = tuple.getSchema().get(col);
			long data = tuple.getData()[index];
			sum += data;
		}
		return sum % BUILDING_BUCKET_SIZE;
	}

	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub

		if (!messageQueue.isEmpty()) {
			Tuple test = messageQueue.poll();
			return test;
			//return messageQueue.poll();
		}

		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}


	class Buffer {

		/*buffer for storing Tuples*/
		private Tuple[] bufferTuples;
		/*the size of one page in bytes*/
		private final int size = 4096;
		/*the next tuple's index*/
		private int position;
		/*maximum tuples in the current block*/
		private int maxTupleNumber;
		private int bufferPage = 3;


		public Buffer (Tuple tuple) {

			/*calculate the number of tuples in this buffer and prepare buffer*/

			maxTupleNumber = (int)(bufferPage * size)/(tuple.getSize() *4);
			bufferTuples = new Tuple[maxTupleNumber];
			position = 0;

		}

		public Tuple get (int index) {
			return bufferTuples[index];
		}

		public void addTuple (Tuple tuple) {
			bufferTuples[position++] = tuple;
		}

		public int getPosition () {
			return position;

		}

		public int getLimit () {
			return maxTupleNumber;	
		}

		public boolean isFull () {
			return position > maxTupleNumber;
		}

		public void reset() {
			bufferTuples = new Tuple[maxTupleNumber];
			position = 0;
		}

	}


}
