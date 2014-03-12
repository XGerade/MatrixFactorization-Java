/*
 * Project: MatrixFactorization
 * @author Fangzhou Yang
 * @author Xugang Zhou
 * @version 1.0
 */

package de.tu_berlin.dima.bigdata.matrixfactorization;

import de.tu_berlin.dima.bigdata.matrixfactorization.itemrating.ItemRatingVectorMapper;
import de.tu_berlin.dima.bigdata.matrixfactorization.itemrating.ItemRatingVectorReducer;
import de.tu_berlin.dima.bigdata.matrixfactorization.prediction.PredictionCrosser;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.FeatureMatrixUpdatePlan;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.InitItemFeatureMatrixMapper;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.ItemFeatureMatrixCrosser;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.ItemFeatureMatrixReducer;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.UserFeatureMatrixCrosser;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.UserFeatureMatrixReducer;
import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.userrating.UserRatingVectorMapper;
import de.tu_berlin.dima.bigdata.matrixfactorization.userrating.UserRatingVectorReducer;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactFloat;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.generic.contract.IterationContract;

/*
 * This Class is the the "plan" class of this project.
 */
public class MatrixFactorizationPlan implements PlanAssembler, PlanAssemblerDescription{
	  
	private final int numIterations = 20;
	
	private final CrossContract userFeatureMatrixCrossers[] = new CrossContract[numIterations];
	private final CrossContract itemFeatureMatrixCrossers[] = new CrossContract[numIterations];
	private final ReduceContract userFeatureMatrixReducers[] = new ReduceContract[numIterations];
	private final ReduceContract itemFeatureMatrixReducers[] = new ReduceContract[numIterations];
	
	@Override
	public String getDescription() {
		return "Usage: [inputPath] [outputPath] ([numSubtasks])";
	}
	  /*
	   * This method defines how the data would be operated.
	   * @return The whole scala-plan
	   * @param args(0) Number of subtasks to specify parallelism
	   * @param args(1) Path to input file
	   * @param args(2) Path to output file
	   * @param args(3) lambda which is used during ALS learning
	   * @param args(4) Number of features pre-set for learning
	   * @param args(5) Number of Users of input
	   * @param args(6) Number of Items of input
	   * @param args(7) Number of Iterations to run in ALS 
	   */
	@Override
	public Plan getPlan(String... args) {
		String inputPath = args.length >= 1 ? args[0] : "";
		String outputPath = args.length >= 2 ? args[1] : "";
		int numSubtasks = args.length >= 3 ? Integer.parseInt(args[2]) : 1;

		FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");
		
		System.out.println("Processing.. start iteration..");
		
	    /*
	     * Get item-rating-vector 
	     */
		MapContract itemRatingVectorMapper = MapContract
				.builder(ItemRatingVectorMapper.class).input(source)
				.name("Item Rating Vector Mapper").build();

		ReduceContract itemRatingVectorReducer = ReduceContract
				.builder(ItemRatingVectorReducer.class, PactInteger.class, 0)
				.input(itemRatingVectorMapper).name("Item Rating Vector Reducer").build();
		
	    /*
	     * Get user-rating-vector
	     */
		MapContract userRatingVectorMapper = MapContract
				.builder(UserRatingVectorMapper.class).input(source)
				.name("User Rating Vector Mapper").build();

		ReduceContract userRatingVectorReducer = ReduceContract
				.builder(UserRatingVectorReducer.class, PactInteger.class, 0)
				.input(userRatingVectorMapper).name("User Rating Vector Reducer").build();
		
	    /*
	     * Initialize item-feature-matrix with random value
	     */
		MapContract initItemFeatureMatrixMapper = MapContract
				.builder(InitItemFeatureMatrixMapper.class).input(itemRatingVectorReducer)
				.name("init Item Feature Matrix Mapper").build();
		
		ReduceContract initItemFeatureMatrixReducer = ReduceContract
				.builder(ItemFeatureMatrixReducer.class, PactInteger.class, 0)
				.input(initItemFeatureMatrixMapper).name("init Item Feature Matrix Reducer").build();
		
	    /*
	     * Learn the user-feature-matrix with initialized item-feature-matrix
	     */
		userFeatureMatrixCrossers[0] = CrossContract
				.builder(UserFeatureMatrixCrosser.class).input1(userRatingVectorReducer).input2(initItemFeatureMatrixReducer)
				.name("User Feature Matrix Update Crosser 1").build();
		
		userFeatureMatrixReducers[0] = ReduceContract
				.builder(UserFeatureMatrixReducer.class, PactInteger.class, 0).input(userFeatureMatrixCrossers[0])
				.name("User Feature Matrix Update Reducer 1").build();
		
	    /*
	     * Continue to Alternative-Least-Sqaure (ALS) learning with numIter iterations
	     */
		for(int i = 1; i < numIterations; i ++){
//			System.out.println("iteration :" + i);
			itemFeatureMatrixCrossers[i-1] = CrossContract
					.builder(ItemFeatureMatrixCrosser.class).input1(itemRatingVectorReducer).input2(userFeatureMatrixReducers[i-1])
					.name("Item Feature Matrix Update Crosser " + (i)).build();
			
			itemFeatureMatrixReducers[i-1] = ReduceContract
					.builder(ItemFeatureMatrixReducer.class, PactInteger.class, 0).input(itemFeatureMatrixCrossers[i-1])
					.name("Item Feature Matrix Update Reducer " + (i)).build();
			
			userFeatureMatrixCrossers[i] = CrossContract
					.builder(UserFeatureMatrixCrosser.class).input1(userRatingVectorReducer).input2(itemFeatureMatrixReducers[i-1])
					.name("User Feature Matrix Update Crosser " + (i+1)).build();
			
			userFeatureMatrixReducers[i] = ReduceContract
					.builder(UserFeatureMatrixReducer.class, PactInteger.class, 0).input(userFeatureMatrixCrossers[i])
					.name("User Feature Matrix Update Reducer " + (i+1)).build();
		}
		
		itemFeatureMatrixCrossers[numIterations-1] = CrossContract
				.builder(ItemFeatureMatrixCrosser.class).input1(itemRatingVectorReducer).input2(userFeatureMatrixReducers[numIterations-1])
				.name("Item Feature Matrix Update Crosser " + (numIterations)).build();
		
		itemFeatureMatrixReducers[numIterations-1] = ReduceContract
				.builder(ItemFeatureMatrixReducer.class, PactInteger.class, 0).input(itemFeatureMatrixCrossers[numIterations-1])
				.name("Item Feature Matrix Update Reducer " + (numIterations)).build();	
		
	    /*
	     * Use learned user- and item-feature-vectors to do the prediction of rating
	     */
		CrossContract predictCrosser = CrossContract.builder(PredictionCrosser.class)
				.input1(itemFeatureMatrixCrossers[numIterations-1])
				.input2(userFeatureMatrixCrossers[numIterations-1])
				.name("Predict Crosser")
				.build();
		
	    /*
	     * Put the predicted-rating result to output stream
	     */
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, predictCrosser, "Rating Prediction");
		RecordOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 0)
			.field(PactInteger.class, 1)
			.field(PactFloat.class, 2);
		

	    /*
	     * Return the plan
	     */
		Plan plan = new Plan(sink, "Rating Prediction Computation");
		plan.setDefaultParallelism(numSubtasks);

		return plan;
	}
	/*
	 * This method enables you to run this project locally.
	 * Run this object with the parameters specified below will result in run this project locally.
	 */
	public static void main(String[] args) throws Exception {

		String inputPath = "file://"+System.getProperty("user.dir") +"/datasets/100k/ua.base";

		String outputPath = "file://"+System.getProperty("user.dir") +"/results/100k/Prediction_ua_i=20.result";


		System.out.println("Reading input from " + inputPath);
		System.out.println("Writing output to " + outputPath);

		Plan toExecute = new MatrixFactorizationPlan().getPlan(inputPath, outputPath);

		Util.executePlan(toExecute);
	}
	
}