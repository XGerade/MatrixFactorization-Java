package de.tu_berlin.dima.bigdata.matrixfactorization.userrating;

import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
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



public class UserRatingVectorPlan implements PlanAssembler, PlanAssemblerDescription{

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

		
		MapContract irMapper = MapContract
				.builder(UserRatingVectorMapper.class).input(source)
				.name("User Rating Vector Mapper").build();

		ReduceContract irReducer = ReduceContract
				.builder(UserRatingVectorReducer.class, PactInteger.class, 0)
				.input(irMapper).name("User Rating Vector Reducer").build();
		
		
		
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, irReducer, "User Rating Vectors");
		RecordOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 0) // user id 
			.field(PactVector.class, 1); // rating vector

		Plan plan = new Plan(sink, "User Rating Vectors Construction");
		plan.setDefaultParallelism(numSubtasks);

		return plan;
	}
	
	public static void main(String[] args) throws Exception {

		String inputPath = "file://"+System.getProperty("user.dir") +"/datasets/10m/ratings.dat";

		String outputPath = "file://"+System.getProperty("user.dir") +"/results/10m/userRating";


		System.out.println("Reading input from " + inputPath);
		System.out.println("Writing output to " + outputPath);

		Plan toExecute = new UserRatingVectorPlan().getPlan(inputPath, outputPath);
		
		Util.executePlan(toExecute);
		
		// Util.deleteAllTempFiles();
	}
	
}