package org.acumos.modelrunner.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecCmdUtil {
	private static final Logger logger = LoggerFactory.getLogger(ExecCmdUtil.class);

	/**
	 * 
	 * @param cmd
	 *            command to be run with no output
	 * @return error code
	 * @throws Exception
	 */
	public static int runCommand(String cmd) throws Exception {
		logger.info("Exec: " + cmd);
		Process p = Runtime.getRuntime().exec(cmd);

		// get the error stream of the process and print it
		InputStream error = p.getErrorStream();
		for (int i = 0; i < error.available(); i++) {
			logger.error("" + error.read());
		}

		int exitVal = p.waitFor();
		logger.info("Exit Value: " + exitVal);
		return exitVal;
	}

	/**
	 * https://stackoverflow.com/questions/5711084/java-runtime-getruntime-getting-output-from-executing-a-command-line-program
	 * 
	 * @param cmd
	 *            command to be executed
	 * @return result of executing command
	 * @throws IOException
	 *             On failure
	 */
	public static String execCommand(String cmd) throws IOException {
		Process proc = Runtime.getRuntime().exec(cmd);
		InputStream is = proc.getInputStream();
		Scanner s = new java.util.Scanner(is);
		// Scanner s = scanner.useDelimiter("\\A");

		String val = "";
		if (s.hasNext()) {
			val = s.next();
		} else {
			val = "";
		}
		s.close();
		return val;
	}
	
	/**
	 * print out output of an command
	 * 
	 * @param bufferedReader
	 * @return output
	 * @throws IOException
	 */
	public static ArrayList<String> printCmdOutput(BufferedReader bufferedReader) throws IOException {
		String currentLine;
		ArrayList<String> output = new ArrayList<>();

		while ((currentLine = bufferedReader.readLine()) != null) {
			logger.info("printCmdOuput: " + currentLine);

			int i;
			for (i = 0; i < currentLine.length(); i++)
				if (!Character.isWhitespace(currentLine.charAt(i))) {
					output.add(currentLine);
					break;
				}
		}
		bufferedReader.close();
		return output;
	}

	/* print out command error message */
	public static void printCmdError(InputStream error) throws IOException {
		for (int i = 0; i < error.available(); i++) {
			logger.error("printCmdError: " + error.read());
		}
		error.close();
	}


	/**
	 * Deleting a directory recursively :
	 * http://www.baeldung.com/java-delete-directory
	 * 
	 * @param directoryToBeDeleted
	 * @return true or false
	 */
	public static boolean deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				deleteDirectory(file);
			}
		}
		return directoryToBeDeleted.delete();
	}
}
