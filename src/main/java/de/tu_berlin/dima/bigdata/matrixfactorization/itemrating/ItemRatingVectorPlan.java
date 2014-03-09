package de.tu_berlin.dima.bigdata.matrixfactorization.itemrating;

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



public class ItemRatingVectorPlan implements PlanAssembler, PlanAssemblerDescription{

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
				.builder(ItemRatingVectorMapper.class).input(source)
				.name("Item Rating Vector Mapper").build();

		ReduceContract irReducer = ReduceContract
				.builder(ItemRatingVectorReducer.class, PactInteger.class, 0)
				.input(irMapper).name("Item Rating Vector Reducer").build();
		
		
		
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, irReducer, "Item Rating Vectors");
		RecordOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 0) // item id 
			.field(PactVector.class, 1); // rating vector

		Plan plan = new Plan(sink, "Item Rating Vectors Construction");
		plan.setDefaultParallelism(numSubtasks);

		return plan;
	}
	
	public static void main(String[] args) throws Exception {

		String inputPath = "file://"+System.getProperty("user.dir") +"/datasets/ua.base.txt";

		String outputPath = "file://"+System.getProperty("user.dir") +"/results/itemRating";


		System.out.println("Reading input from " + inputPath);
		System.out.println("Writing output to " + outputPath);

		Plan toExecute = new ItemRatingVectorPlan().getPlan(inputPath, outputPath);
		
		Util.executePlan(toExecute);
		
		// Util.deleteAllTempFiles();
	}
	
}