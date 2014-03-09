package de.tu_berlin.dima.bigdata.matrixfactorization.solve;

import de.tu_berlin.dima.bigdata.matrixfactorization.itemrating.ItemRatingVectorMapper;
import de.tu_berlin.dima.bigdata.matrixfactorization.itemrating.ItemRatingVectorPlan;
import de.tu_berlin.dima.bigdata.matrixfactorization.itemrating.ItemRatingVectorReducer;
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
import eu.stratosphere.pact.common.type.base.PactInteger;


public class FeatureMatrixUpdatePlan implements PlanAssembler, PlanAssemblerDescription{
	@Override
	public String getDescription() {
		return "Usage: [inputPath] [outputPath] ([numSubtasks])";
	}
	@Override
	public Plan getPlan(String... args) {
		String inputPath = args.length >= 1 ? args[0] : "";
		String outputPath = args.length >= 2 ? args[1] : "";
		int numSubtasks = args.length >= 3 ? Integer.parseInt(args[2]) : 1;

		FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");

		
		MapContract itemRatingVectorMapper = MapContract
				.builder(ItemRatingVectorMapper.class).input(source)
				.name("Item Rating Vector Mapper").build();

		ReduceContract itemRatingVectorReducer = ReduceContract
				.builder(ItemRatingVectorReducer.class, PactInteger.class, 0)
				.input(itemRatingVectorMapper).name("Item Rating Vector Reducer").build();
		
		MapContract userRatingVectorMapper = MapContract
				.builder(UserRatingVectorMapper.class).input(source)
				.name("User Rating Vector Mapper").build();

		ReduceContract userRatingVectorReducer = ReduceContract
				.builder(UserRatingVectorReducer.class, PactInteger.class, 0)
				.input(userRatingVectorMapper).name("User Rating Vector Reducer").build();
		
		MapContract initItemFeatureMatrixMapper = MapContract
				.builder(InitItemFeatureMatrixMapper.class).input(itemRatingVectorReducer)
				.name("init Item Feature Matrix Mapper").build();
		
		ReduceContract initItemFeatureMatrixReducer = ReduceContract
				.builder(ItemFeatureMatrixReducer.class, PactInteger.class, 0)
				.input(initItemFeatureMatrixMapper).name("init Item Feature Matrix Reducer").build();
		
		CrossContract userFeatureMatrixCrosser = CrossContract
				.builder(UserFeatureMatrixCrosser.class).input1(userRatingVectorReducer).input2(initItemFeatureMatrixReducer)
				.name("User Feature Matrix Update Crosser").build();
		
		ReduceContract userFeatureMatrixReducer = ReduceContract
				.builder(UserFeatureMatrixReducer.class, PactInteger.class, 0).input(userFeatureMatrixCrosser)
				.name("User Feature Matrix Update Reducer").build();
		
		CrossContract itemFeatureMatrixCrosser = CrossContract
				.builder(ItemFeatureMatrixCrosser.class).input1(itemRatingVectorReducer).input2(userFeatureMatrixReducer)
				.name("Item Feature Matrix Update Crosser").build();
		
		
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, itemFeatureMatrixCrosser, "Item Feature Vectors");
		RecordOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 2)
			.field(PactVector.class, 1);
		


		Plan plan = new Plan(sink, "Item Feature Matrix Update Computation");
		plan.setDefaultParallelism(numSubtasks);

		return plan;
	}
	public static void main(String[] args) throws Exception {

		String inputPath = "file://"+System.getProperty("user.dir") +"/datasets/10m/ratings.dat";

		String outputPath = "file://"+System.getProperty("user.dir") +"/results/10m/ItemFeatureMatixUpdate";


		System.out.println("Reading input from " + inputPath);
		System.out.println("Writing output to " + outputPath);

		Plan toExecute = new FeatureMatrixUpdatePlan().getPlan(inputPath, outputPath);
//		toExecute.setDefaultParallelism(1);
		Util.executePlan(toExecute);
		
		// Util.deleteAllTempFiles();
	}
	
}