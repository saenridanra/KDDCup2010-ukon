package org.ukon.kddcup.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class PreprocessRunner {

	private static String row = "Row";
	private static String anonStud = "Anon Student Id";
	private static String problemHierarchy = "Problem Hierarchy";
	private static String problemName = "Problem Name";
	private static String problemView = "Problem View";
	private static String stepName = "Step Name";
	private static String correctFirstAttempt = "Correct First Attempt";
	private static String stepStartTime = "Step Start Time";
	private static String firstTransactionTime = "First Transaction Time";
	private static String correctTransactionTime = "Correct Transaction Time";
	private static String stepEndTime = "Step End Time";
	private static String stepDuration = "Step Duration (sec)";
	private static String correctStepDuration = "Correct Step Duration (sec)";
	private static String errorStepDuration = "Error Step Duration (sec)";
	private static String incorrects = "Incorrects";
	private static String hints = "Hints";
	private static String corrects = "Corrects";
	private static String kcSub = "KC(SubSkills)";
	private static String opportunitySub = "Opportunity(SubSkills)";
	private static String kcTraced = "KC(KTracedSkills)";
	private static String opportunityTraced = "Opportunity(KTracedSkills)";
	private static String kcRules = "KC(Rules)";
	private static String opportunityRules = "Opportunity(Rules)";

	private static final String rootPath = "C:" + File.separator + "Users"
			+ File.separator + "andre_000" + File.separator + "Documents"
			+ File.separator + "Studium" + File.separator + "KDDCup"
			+ File.separator + "Data" + File.separator;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// Step one, remove columns
		// Preparing lists
		Map<String, Integer> columns = new HashMap<String, Integer>();
		columns.put(row, -1);
		columns.put(stepStartTime, -1);
		columns.put(firstTransactionTime, -1);
		columns.put(correctTransactionTime, -1);
		columns.put(problemName, -1);
		columns.put(stepEndTime, -1);
		columns.put(stepDuration, -1);
		columns.put(correctStepDuration, -1);
		columns.put(errorStepDuration, -1);
		columns.put(incorrects, -1);
		columns.put(hints, -1);
		columns.put(corrects, -1);
		columns.put(kcSub, -1);
		columns.put(opportunitySub, -1);
		columns.put(kcTraced, -1);
		columns.put(kcRules, -1);
		columns.put(opportunityTraced, -1);
		columns.put(opportunityRules, -1);
//
		removeUnnecessaryColumns(new File(rootPath
				+ "algebra_2008_2009_train.txt"), new File(rootPath
				+ "Step1_remove" + File.separator
				+ "algebra_2008_2009_train_removed_columns.txt"), columns);

		// Step two, parse step name
		parseStepNameRedundancies(new File(rootPath + "Step1_remove"
				+ File.separator
				+ "algebra_2008_2009_train_removed_columns.txt"), new File(
				rootPath + "Step2_parse" + File.separator
						+ "algebra_2008_2009_train_parsedStepName.txt"));

		// Step Three, nominal statistics
		List<String> columns2 = new ArrayList<String>();
		columns2.add(anonStud);
		columns2.add(problemHierarchy);
//		columns2.add(problemName);
		columns2.add(stepName);
//
		Map<String, Set<String>> nominalStats = createNominalStatistics(
				new File(rootPath + "Step2_parse" + File.separator
						+ "algebra_2008_2009_train_parsedStepName.txt"),
				columns2);

		createBinaryAttributes(
				new File(rootPath + "Step2_parse" + File.separator
						+ "algebra_2008_2009_train_parsedStepName.txt"),
				new File("G:" + File.separator + "Data" + File.separator
						+ "KDDCup" + File.separator + "Step3_binary3" + File.separator
						+ "algebra_2008_2009_train_binaryAttributes.txt"),
				nominalStats, correctFirstAttempt);
		
//		createSubmissionsFile(new File("G:" + File.separator + "Data" + File.separator
//						+ "KDDCup" + File.separator + "Step3_binary3" + File.separator
//						+ "output_20_iter_regression.txt"), new File("G:" + File.separator + "Data" + File.separator
//								+ "KDDCup" + File.separator + "Step3_binary3" + File.separator
//								+ "algebra_2008_2009_submission.txt"));

	}

	/**
	 * Removing unnecessary columns in this method. Saving file to another
	 * location.
	 * 
	 * @param src
	 * @param dest
	 * @param columns
	 * @throws IOException
	 */
	public static void removeUnnecessaryColumns(File src, File dest,
			Map<String, Integer> columns) throws IOException {

		if (dest.exists()) {
			dest.delete();
		}

		BufferedReader reader = Files.newBufferedReader(src.toPath(),
				Charset.forName("UTF-8"));
		BufferedWriter stream = Files.newBufferedWriter(dest.toPath(), Charset.forName("UTF-8"));
		String line = null;
		List<Integer> indexes = new ArrayList<Integer>();
		boolean firstLine = true;
		int counter = 0;
		while ((line = reader.readLine()) != null) {
			String[] splitString = line.split("\t", -1);

			if (counter % 100000 == 0) {
				System.out.println("Reading line " + counter);
			}

			if (firstLine) {
				for (int i = 0; i < splitString.length; i++) {
					if (columns.get(splitString[i]) != null) {
						columns.put(splitString[i], i);
						indexes.add(i);
					}
				}
				firstLine = false;
			}

			String out = "";
			for (int i = 0; i < splitString.length; i++) {
				if (!indexes.contains(i)) {
					out += splitString[i] + "\t";
				}
			}
			out.substring(0, out.length() - 2);
			out += "\n";

			stream.write(out);
			counter++;
		}

		stream.close();
		reader.close();
	}

	/**
	 * 
	 * @param src
	 * @param dest
	 * @throws IOException
	 */
	public static void parseStepNameRedundancies(File src, File dest)
			throws IOException {
		if (dest.exists()) {
			dest.delete();
		}

		BufferedReader reader = Files.newBufferedReader(src.toPath(),
				Charset.forName("UTF-8"));
		OutputStream stream = Files.newOutputStream(dest.toPath());
		String line = null;
		int stepNameIndex = -1;
		boolean firstLine = true;
		int counter = 0;
		while ((line = reader.readLine()) != null) {
			String[] splitString = line.split("\t", -1);

			if (counter % 100000 == 0) {
				System.out.println("Parsing line " + counter);
			}

			if (firstLine) {
				for (int i = 0; i < splitString.length; i++) {
					if (splitString[i].equals(stepName)) {
						stepNameIndex = i;
					}
				}
				firstLine = false;
			} else {
				if (splitString[stepNameIndex].contains("=")
						|| splitString[stepNameIndex].contains("+")
						|| splitString[stepNameIndex].contains("-")
						|| splitString[stepNameIndex].contains("*")
						|| splitString[stepNameIndex].contains("/")
						|| splitString[stepNameIndex].contains("^")
						|| splitString[stepNameIndex].contains(">")
						|| splitString[stepNameIndex].contains("<")
						|| splitString[stepNameIndex].contains("sqrt")) {
					splitString[stepNameIndex] = "EquationParse";
				} else if (splitString[stepNameIndex]
						.matches("[0-9]+[.]?[0-9]*[.]?")) {
					splitString[stepNameIndex] = "ConstantNumber";
				}
			}

			String out = "";
			for (int i = 0; i < splitString.length - 2; i++) {
				out += splitString[i] + "\t";
			}

			out += splitString[splitString.length - 2];
			out += "\n";

			stream.write(out.getBytes());
			counter++;
		}

		stream.close();
		reader.close();
	}

	/**
	 * Create statistics regarding the nominal attributes. This step precedes
	 * the creation of binary attributes.
	 * 
	 * @param src
	 * @param columns
	 * @throws IOException
	 */
	public static Map<String, Set<String>> createNominalStatistics(File src,
			List<String> columns) throws IOException {
		Map<String, Set<String>> stats = new HashMap<String, Set<String>>();
		Map<Integer, String> indexToKey = new HashMap<Integer, String>();

		for (String c : columns) {
			stats.put(c, new HashSet<String>());
		}

		BufferedReader reader = Files.newBufferedReader(src.toPath(),
				Charset.forName("UTF-8"));
		String line = null;
		boolean firstLine = true;
		int counter = 0;
		while ((line = reader.readLine()) != null) {
			String[] splitString = line.split("\t", -1);

			if (counter % 1000000 == 0) {
				System.out.println("Parsing line " + counter);
			}
			if (firstLine) {
				for (int i = 0; i < splitString.length; i++) {
					if (columns.contains(splitString[i])) {
						indexToKey.put(i, splitString[i]);
					}
				}
				firstLine = false;
			} else {
				for (int i = 0; i < splitString.length; i++) {
					if (indexToKey.containsKey(i)) {
						stats.get(indexToKey.get(i)).add(splitString[i]);
					}
				}
			}
			counter++;
		}

		return stats;
	}

	/**
	 * Parsing nominal attributes to binary ones and saving to a new file.
	 * 
	 * @param src
	 * @param dest
	 * @param attributes
	 * @throws IOException
	 */
	public static void createBinaryAttributes(File src, File dest,
			Map<String, Set<String>> attributes, String targetColumn) throws IOException {
		dest.mkdirs();

		if (dest.exists()) {
			dest.delete();
		}

		BufferedReader reader = Files.newBufferedReader(src.toPath(),
				Charset.forName("UTF-8"));
		BufferedWriter writer = Files.newBufferedWriter(dest.toPath(), Charset.forName("UTF-8"));
		String line = null;
		boolean firstLine = true;

		Map<String, Integer> oldColumns = new HashMap<String, Integer>();

		int counter = 0;
		int pre_binary_columns = 0;
		int targetColumnIndex = 0;
		int unknowAttributeCounter = 0;
		long timeTaken = System.currentTimeMillis();

		Map<String, Integer> colToIndex = new HashMap<String, Integer>();
		for (Entry<String, Set<String>> entry : attributes.entrySet()) {
			int i = 0;
			for (String s : entry.getValue()) {
				colToIndex.put(entry.getKey() + "_" + s, i);
				i++;
			}
		}
		
		unknowAttributeCounter = colToIndex.values().size();

		String out = "";

		while ((line = reader.readLine()) != null) {
			String[] splitString = line.split("\t", -1);

			if (counter % 10000 == 0) {
				System.out
						.println("Creating binary line "
								+ counter
								+ ", time: "
								+ (((double) System.currentTimeMillis() - timeTaken) / 1000.0));
				timeTaken = System.currentTimeMillis();
			}

			if (firstLine) {
				for (int i = 0; i < splitString.length; i++) {
					if (attributes.containsKey(splitString[i])) {
						oldColumns.put(splitString[i], i);
					}
					
					if(splitString[i].equals(targetColumn)){
						targetColumnIndex = i;
					}
				}
			}

			if (firstLine) {
				for (int i = 0; i < splitString.length; i++) {
					if (!oldColumns.containsValue(i)  && i != targetColumnIndex) {
						out += pre_binary_columns + ":" + splitString[i] + "\t";
						pre_binary_columns++;
					}
				}
			} else {
				int counterF = 0;
				if(splitString[targetColumnIndex].equals("")){
					out += "0\t";
				}
				else{
					out += splitString[targetColumnIndex]+"\t";
				}
				for (int i = 0; i < splitString.length; i++) {
					if (!oldColumns.containsValue(i) && i != targetColumnIndex) {
						out += counterF + ":" + splitString[i] + "\t";
						counterF++;
					}
				}
			}

			if (firstLine) {
				for (Entry<String, Set<String>> entry : attributes.entrySet()) {
					for (String s : entry.getValue()) {
						out += entry.getKey() + "_" + s + "\t";
					}
				}
			} else {
				for (Entry<String, Set<String>> entry : attributes.entrySet()) {
					String s = entry.getKey() + "_"
							+ splitString[oldColumns.get(entry.getKey())];
					
					int colIndex = -1;
					
					if(colToIndex.get(s) != null){
						colIndex = colToIndex.get(s);
					}
					else{
						colIndex = unknowAttributeCounter;
						unknowAttributeCounter++;
					}
							
					out += colIndex + ":1\t";
				}
			}

			if (!firstLine) {
				out = out.trim();
				out += "\n";
				writer.append(out);
				out = "";
			} else {
				out = "";
				firstLine = false;
			}
			counter++;
		}

		writer.close();
		reader.close();
	}
	
	public static void createSubmissionsFile(File src, File dest) throws IOException{
		dest.mkdirs();

		if (dest.exists()) {
			dest.delete();
		}

		BufferedReader reader = Files.newBufferedReader(src.toPath(),
				Charset.forName("UTF-8"));
		BufferedWriter writer = Files.newBufferedWriter(dest.toPath(), Charset.forName("UTF-8"));
		double positive = 0, negative = 0;
		
		String out = "Row\tCorrect First Attempt\n";
		writer.write(out);
		out = "";
		double lineNumber = 1;
		String line = "";
		while ((line = reader.readLine()) != null) {
			double number = Double.valueOf(line.trim());
			if(number < 0.5){
				out += ((int) lineNumber)+"\t0\n";
				negative++;
			}
			else{
				out += ((int) lineNumber)+"\t1\n";
				positive++;
			}
			lineNumber++;
			writer.write(out);
			out = "";
		}
		
		System.err.println("Positive: " + positive + "\t Negative " + negative);
		System.err.println("Positive(p): " + (positive/lineNumber) + "\t Negative(p) " + (negative/lineNumber));
		
		writer.close();
		reader.close();
	}


}
