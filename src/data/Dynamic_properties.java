package data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * This class provides function:
 * 
 * store dynamic properties for future use
 * 
 * @author Yixuan Jiang
 *
 */
public class Dynamic_properties {

	public static String inputPath = "src/samples/input";
	public static String dataPath = inputPath + "/db/data/";
	public static String queryPath = inputPath + "/queries.sql";
	public static String configuePath = inputPath + "/plan_builder_config.txt";
	public static String schemaPath = inputPath + "/db/schema.txt";
	public static String indexInfoPath = inputPath + "/db/index_info.txt";
	public static String indexedPath = inputPath + "/db/indexes";
	public static String outputPath = "src/samples/output";
	public static String tempPath = "src/samples/temp";
	
	/**
	 * set input and output path according to pass in parameters
	 * @param p0 input absolute path
	 * @param p1 output absolute path
	 * @param p2 temporary scratch file directory
	 */
	public static void setPath(String p0, String p1, String p2) {		
		inputPath = p0;
		outputPath = p1;
		tempPath = p2;
		dataPath = inputPath + "/db/data/";
		queryPath = inputPath + "/queries.sql";
		schemaPath = inputPath + "/db/schema.txt";
		configuePath = inputPath + "/plan_builder_config.txt";
		indexInfoPath = inputPath + "/db/index_info.txt";
		indexedPath = inputPath + "/db/indexes";
	}
	/**
	 * set input and output path according to pass in parameters
	 * @param p0 input absolute path
	 * @param p1 output absolute path
	 */
	public static void setPath(String p0, String p1) {
		inputPath = p0;
		outputPath = p1;
		dataPath = inputPath + "/db/data/";
		queryPath = inputPath + "/queries.sql";
		schemaPath = inputPath + "/db/schema.txt";
		
	}
}
