package de.tu_berlin.dima.bigdata.matrixfactorization.evaluation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

public class Evaluation{

	static final int user_pos = 0;
	static final int item_pos = 1;
	static final int rating_pos =2;
	
	public static float evaluate(String testFile, Pattern pattern_test, String predictFile, Pattern pattern_predict) throws IOException{
		BufferedReader br_test = new BufferedReader(new FileReader(testFile));
		BufferedReader br_predict = new BufferedReader(new FileReader(predictFile));
		
		int count = 0;
		float sd_sum = 0;
		
		while(br_test.ready()){
			String line_test = br_test.readLine();
			String []items = pattern_test.split(line_test);
			int userID_test = Integer.parseInt(items[user_pos]);
			int itemID_test = Integer.parseInt(items[item_pos]);
			float rating_test = Float.parseFloat(items[rating_pos]);
			
			while(br_predict.ready()){
				String line_predict = br_predict.readLine();
				String []items_p = pattern_predict.split(line_predict);
				int userID_predict = Integer.parseInt(items_p[user_pos]);
				int itemID_predict = Integer.parseInt(items_p[item_pos]);
				float rating_predict = Float.parseFloat(items_p[rating_pos]);
				if(userID_predict == userID_test && itemID_predict == itemID_test){
					count ++;
					sd_sum += square_deviation(rating_test, rating_predict);
					break;
				}
			}
			
		}
		
		br_test.close();
		br_predict.close();
		
		return (float)Math.sqrt( sd_sum/count);
		
	}
	
	private static float square_deviation(float r1, float r2){
		return (r1-r2)*(r1-r2);
	}
	
	public static void main(String [] args) throws IOException{
		String predict = "results/100k/Prediction_ua_i=20.result";
		String test = "datasets/100k/ua.test.txt";
		Pattern DELIMITER_test = Pattern.compile("[\t]");
		Pattern DELIMITER_predict = Pattern.compile(" ");
		float rmsd = evaluate(test,DELIMITER_test, predict, DELIMITER_predict);
		
		System.out.println("RMSD: " + rmsd);
	}
	
}