/*-
 * ===============LICENSE_START=======================================================
 * Acumos
 * ===================================================================================
 * Copyright (C) 2017 AT&T Intellectual Property & Tech Mahindra. All rights reserved.
 * ===================================================================================
 * This Acumos software file is distributed by AT&T and Tech Mahindra
 * under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ===============LICENSE_END=========================================================
 */

package org.acumos.modelrunner.controller;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;

import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.ws.rs.core.MediaType;

import hex.genmodel.easy.RowData;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.prediction.*;
import hex.genmodel.MojoModel;
import hex.genmodel.easy.exception.PredictException;
import java.util.*;

import net.sf.javaml.core.Dataset;
import net.sf.javaml.tools.data.FileHandler;

import org.springframework.beans.factory.annotation.Value;
import org.apache.commons.io.IOUtils;
import org.metawidget.util.simple.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
public class RunnerController {
	@Value("${plugin_root}")
	private String pluginRoot;

	@Value("${rel_model_zip}")
	private String relModelZip;

	@Value("${rel_default_protofile}")
	private String relDefaultProtofile;

	@Value("${model_type}")
	private String modelType;

	@Value("${model_config}")
	private String modelConfig;

	private String modelZip = null;

	private String defaultProtofile;

	private String projectRoot;

	private String protoFilePath = null;

	private String protoOutputPath = null;

	private String pluginClassPath = null;

	private ArrayList<String> attributes = new ArrayList<String>();
	private ArrayList<String> attributeTypes = new ArrayList<String>();
	Class<?> dataframerow;
	Class<?> dataframe;
	Class<?> prediction;
	Class<?> dataframeBuilder;

	private static final Logger logger = LoggerFactory.getLogger(RunnerController.class);
	private static final Class<?>[] parameters = new Class[] { URL.class };
	private static final String sep = File.separator;
	private static final String newLine = System.lineSeparator();
	private static final String tmpPath = new String("/tmp/modelrunner/");
	private ClassLoader cl = null;

	@RequestMapping(value = "/hello", method = RequestMethod.GET)
	public String hello() {
		return "HelloWorld";

	}

	@RequestMapping(value = "/getBinaryDefault", method = RequestMethod.POST, produces = "application/octet-stream")
	public byte[] getBinaryDefault(@RequestPart("csvFile") MultipartFile csvFile) {
		return getBinary_(csvFile, null);
		// return new ResponseEntity<File>(binaryFile, HttpStatus.OK);
	}

	@RequestMapping(value = "/getBinary", method = RequestMethod.POST, produces = MediaType.APPLICATION_OCTET_STREAM)
	public byte[] getBinary(@RequestPart("csvFile") MultipartFile csvFile, @RequestPart("proto") MultipartFile proto) {

		return getBinary_(csvFile, proto);

	}

	public byte[] getBinary_(MultipartFile file, MultipartFile proto) {
		if (file.isEmpty()) {
			logger.error("You failed to upload " + file.getOriginalFilename() + " because the file was empty.");
			return null;
		}

		try {

			String contentType = file.getContentType();
			if (!contentType.equalsIgnoreCase("application/vnd.ms-excel")
					&& !contentType.equalsIgnoreCase("text/csv")) {
				logger.error("Wrong file type");
				return null;
			}

			String protoString = null;
			if (proto != null && !proto.isEmpty()) {
				long protosize = proto.getSize();

				InputStream protoInput = new BufferedInputStream(proto.getInputStream());
				byte[] protodata = new byte[(int) protosize];
				char[] protoChar = new char[(int) protosize];
				int bytesRead = protoInput.read(protodata);
				for (int i = 0; i < bytesRead; i++) {
					protoChar[i] = (char) protodata[i];
				}

				protoString = new String(protoChar);
			}

			init(protoString);
			Method newBuilder = dataframerow.getMethod("newBuilder");
			Object dfr = newBuilder.invoke(null); // dfr is of
													// DataFrameRow.Builder type
			Method dfNewBuilder = dataframe.getMethod("newBuilder");
			Object df = dfNewBuilder.invoke(null); // df is of DataFrame.Builder
													// type
			Method dfAddRows = dataframeBuilder.getMethod("addRows", dfr.getClass());

			long size = file.getSize();

			InputStream csvInput = new BufferedInputStream(file.getInputStream());
			byte[] data = new byte[(int) size];
			char[] dataChar = new char[(int) size];
			int bytesRead = csvInput.read(data);
			for (int i = 0; i < bytesRead; i++) {
				dataChar[i] = (char) data[i];
			}

			String dataString = new String(dataChar);
			String[] lines = dataString.split(newLine);

			for (String line : lines) {
				String[] array = line.split(",");
				System.out.println("Current line is: " + line);
				for (int i = 0; i < array.length; i++) {
					if (array[i].length() == 0) // skip missing field
						continue;

					String attr = attributes.get(i);
					String attrMethodName = StringUtils.camelCase("set_" + attr, '_');
					Method attrMethod = null;
					switch (attributeTypes.get(i)) {
					case "double":
						attrMethod = dfr.getClass().getMethod(attrMethodName, double.class);
						attrMethod.invoke(dfr, Double.parseDouble(array[i]));
						break;

					case "float":
						attrMethod = dfr.getClass().getMethod(attrMethodName, float.class);
						attrMethod.invoke(dfr, Float.parseFloat(array[i]));
						break;

					case "int32":
					case "uint32":
					case "sint32":
					case "fixed32":
					case "sfixed32":
						attrMethod = dfr.getClass().getMethod(attrMethodName, int.class);
						attrMethod.invoke(dfr, Integer.parseInt(array[i]));
						break;

					case "int64":
					case "uint64":
					case "sint64":
					case "fixed64":
					case "sfixed64":
						attrMethod = dfr.getClass().getMethod(attrMethodName, long.class);
						attrMethod.invoke(dfr, Long.parseLong(array[i]));
						break;

					case "bool":
						attrMethod = dfr.getClass().getMethod(attrMethodName, boolean.class);
						attrMethod.invoke(dfr, Boolean.parseBoolean(array[i]));
						break;

					case "string":
						attrMethod = dfr.getClass().getMethod(attrMethodName, String.class);
						attrMethod.invoke(dfr, array[i]);
						break;

					case "bytes":
						attrMethod = dfr.getClass().getMethod(attrMethodName, byte.class);
						attrMethod.invoke(dfr, Byte.parseByte(array[i]));
						break;
					default:
						break;
					}

				}
				dfAddRows.invoke(df, dfr);
			}

			Method dfBuilder = df.getClass().getMethod("build");
			Object obj = dfBuilder.invoke(df);

			Method tobytearray = obj.getClass().getSuperclass().getSuperclass().getSuperclass()
					.getDeclaredMethod("toByteArray");
			return (byte[]) tobytearray.invoke(obj);

		} catch (Exception e) {
			logger.error(e.getMessage());
			return null;
		}
	}

	@RequestMapping(value = "/transformDefault", method = RequestMethod.POST)
	public byte[] transform(@RequestPart("csvFile") MultipartFile csvFile) {
		return transform_(csvFile, null, null);

	}

	@RequestMapping(value = "/transform", method = RequestMethod.POST)
	public byte[] transform(@RequestPart("csvFile") MultipartFile csvFile, @RequestPart("model") MultipartFile model,
			@RequestPart("proto") MultipartFile proto) {
		return transform_(csvFile, model, proto);
	}

	public byte[] transform_(MultipartFile file, MultipartFile model, MultipartFile proto) {
		if (file.isEmpty()) {
			logger.error("You failed to upload " + file.getOriginalFilename() + " because the file was empty.");
			return null;
		}

		try {

			String contentType = file.getContentType();
			if (!contentType.equalsIgnoreCase("application/vnd.ms-excel")
					&& !contentType.equalsIgnoreCase("text/csv")) {
				logger.error("Wrong file type");
				return null;
			}

			String protoString = null;
			if (proto != null && !proto.isEmpty()) {
				long protosize = proto.getSize();

				InputStream protoInput = new BufferedInputStream(proto.getInputStream());
				byte[] protodata = new byte[(int) protosize];
				char[] protoChar = new char[(int) protosize];
				int bytesRead = protoInput.read(protodata);
				for (int i = 0; i < bytesRead; i++) {
					protoChar[i] = (char) protodata[i];
				}

				protoString = new String(protoChar);
			}

			init(protoString);
			Method newBuilder = dataframerow.getMethod("newBuilder");
			Object dfr = newBuilder.invoke(null); // dfr is of
													// DataFrameRow.Builder type
			Method dfNewBuilder = dataframe.getMethod("newBuilder");
			Object df = dfNewBuilder.invoke(null); // df is of DataFrame.Builder
													// type
			Method dfAddRows = dataframeBuilder.getMethod("addRows", dfr.getClass());

			long size = file.getSize();

			InputStream csvInput = new BufferedInputStream(file.getInputStream());
			byte[] data = new byte[(int) size];
			char[] dataChar = new char[(int) size];
			int bytesRead = csvInput.read(data);
			for (int i = 0; i < bytesRead; i++) {
				dataChar[i] = (char) data[i];
			}

			String dataString = new String(dataChar);
			String[] lines = dataString.split(newLine);

			for (String line : lines) {
				String[] array = line.split(",");
				System.out.println("Current line is: " + line);
				for (int i = 0; i < array.length; i++) {
					if (array[i].length() == 0) // skip missing field
						continue;

					String attr = attributes.get(i);
					String attrMethodName = StringUtils.camelCase("set_" + attr, '_');
					Method attrMethod = null;
					switch (attributeTypes.get(i)) {
					case "double":
						attrMethod = dfr.getClass().getMethod(attrMethodName, double.class);
						attrMethod.invoke(dfr, Double.parseDouble(array[i]));
						break;

					case "float":
						attrMethod = dfr.getClass().getMethod(attrMethodName, float.class);
						attrMethod.invoke(dfr, Float.parseFloat(array[i]));
						break;

					case "int32":
					case "uint32":
					case "sint32":
					case "fixed32":
					case "sfixed32":
						attrMethod = dfr.getClass().getMethod(attrMethodName, int.class);
						attrMethod.invoke(dfr, Integer.parseInt(array[i]));
						break;

					case "int64":
					case "uint64":
					case "sint64":
					case "fixed64":
					case "sfixed64":
						attrMethod = dfr.getClass().getMethod(attrMethodName, long.class);
						attrMethod.invoke(dfr, Long.parseLong(array[i]));
						break;

					case "bool":
						attrMethod = dfr.getClass().getMethod(attrMethodName, boolean.class);
						attrMethod.invoke(dfr, Boolean.parseBoolean(array[i]));
						break;

					case "string":
						attrMethod = dfr.getClass().getMethod(attrMethodName, String.class);
						attrMethod.invoke(dfr, array[i]);
						break;

					case "bytes":
						attrMethod = dfr.getClass().getMethod(attrMethodName, byte.class);
						attrMethod.invoke(dfr, Byte.parseByte(array[i]));
						break;
					default:
						break;
					}

				}
				dfAddRows.invoke(df, dfr);
			}

			Method dfBuilder = df.getClass().getMethod("build");
			Object obj = dfBuilder.invoke(df);
			/*
			 * try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
			 * try(ObjectOutputStream o = new ObjectOutputStream(b)){ o.writeObject(obj); }
			 * return b.toByteArray(); }
			 */
			String modelLoc = null;
			if (model != null && !model.isEmpty()) {
				byte[] bytes = model.getBytes();

				// Creating the directory to store file

				File dir = new File(tmpPath + sep + "tmpFiles");
				if (!dir.exists())
					dir.mkdirs();

				// Create the file on server
				File modelFile = new File(dir.getAbsolutePath() + sep + "mode_" + UUID.randomUUID() + ".zip");
				BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(modelFile));
				stream.write(bytes);
				stream.close();
				modelLoc = modelFile.getAbsolutePath();
				logger.info("model File Location=" + modelFile.getAbsolutePath());
			}

			if (!modelType.equalsIgnoreCase("G"))
				return doPredict(obj, modelLoc);
			else
				return doPredictJavaGeneric(df, modelLoc);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return null;

	}

	private byte[] doPredictJavaGeneric(Object df, String modelLoc) {
		try {
			/*
			 * if (modelLoc != null) mojo = MojoModel.load(modelLoc); else mojo =
			 * MojoModel.load(modelZip);
			 */

			Method getRowsCount = dataframe.getMethod("getRowsCount");
			int row_count = (int) getRowsCount.invoke(df);
			logger.info("We have: " + row_count + " rows.");

			Method getRowsList = dataframe.getMethod("getRowsList");
			List<?> list = (List<?>) getRowsList.invoke(df);
			int j = 0;
			StringBuffer rowString = new StringBuffer();
			for (Object obj : list) {
				j = 0;
				for (String attr : attributes) {
					String attrMethodName = StringUtils.camelCase("get_" + attr, '_');
					Method attrMethod = dataframerow.getMethod(attrMethodName);
					Object attrValObj = attrMethod.invoke(obj);

					if (j != 0) {
						rowString.append(",");
					}
					switch (attributeTypes.get(j)) {
					case "double":
						double attrValDouble = ((Double) attrValObj).doubleValue();
						rowString.append(attrValDouble);
						break;

					case "float":
						float attrValFloat = ((Float) attrValObj).floatValue();
						rowString.append(attrValFloat);
						break;

					case "int32":
					case "uint32":
					case "sint32":
					case "fixed32":
					case "sfixed32":
						int attrValInt = ((Integer) attrValObj).intValue();
						rowString.append(attrValInt);
						break;

					case "int64":
					case "uint64":
					case "sint64":
					case "fixed64":
					case "sfixed64":
						long attrValLong = ((Long) attrValObj).longValue();
						rowString.append(attrValLong);
						break;

					case "bool":
						boolean attrValBool = ((Boolean) attrValObj).booleanValue();
						rowString.append(attrValBool);
						break;

					case "string":
						String attrValStr = (String) attrValObj;
						rowString.append(attrValObj);
						break;
					case "bytes":
						byte attrValByte = ((Byte) attrValObj).byteValue();
						rowString.append(attrValByte);
						break;
					default:
						break;
					}
					j++;

				}
				rowString.append("\n");
			}
			logger.info(rowString.toString());
			String tempPath = tmpPath + sep + "tmpFiles";
			File dir = new File(tempPath);
			if (!dir.exists())
				dir.mkdirs();
			String genfile = tempPath + sep + UUID.randomUUID() + "_genfile.csv";
			FileWriter ff = new FileWriter(genfile);
			ff.write(rowString.toString());
			ff.close();

			String propFile = new String(projectRoot + modelConfig);
			// Load property
			Properties prop = new Properties();
			InputStream input = new FileInputStream(propFile);
			prop.load(input);

			String modelMethodName = prop.getProperty("modelMethod");
			String modelClassName = prop.getProperty("modelClassName");

			logger.info("model class name and method=" + modelClassName + "  " + modelMethodName);

			// model invoke and preparation

			File modelSource = new File(projectRoot + relModelZip);
			File modelJarPath = new File(pluginClassPath + sep + modelSource.getName());
			Files.copy(modelSource, modelJarPath);

			cl = RunnerController.class.getClassLoader();
			addFile(modelJarPath);
			logger.info("Jar file path=" + modelJarPath);
			List<?> predictlist = null;

			Class<?> modelClass = cl.loadClass(modelClassName);

			logger.info("getDeclaredMethods: " + Arrays.toString(modelClass.getDeclaredMethods()));
			logger.info("getMethods: " + Arrays.toString(modelClass.getMethods()));

			String paramType = getMethodParamType(modelClass, modelMethodName);

			Method methodPredict = null;

			logger.info(modelMethodName + " method parameter type=" + paramType);

			switch (paramType) {

			case "java.io.File":

				File file = new File(genfile);
				methodPredict = modelClass.getDeclaredMethod(modelMethodName, File.class);
				predictlist = (List<?>) methodPredict.invoke(null, file);
				break;

			case "net.sf.javaml.core.Dataset":

				logger.info("getDeclaredMethods: " + Arrays.toString(modelClass.getDeclaredMethods()));
				logger.info("getMethods: " + Arrays.toString(modelClass.getMethods()));
				methodPredict = modelClass.getDeclaredMethod(modelMethodName, Dataset.class);
				logger.info("number of attributes: " + j);
				Dataset dataset = null;
				dataset = (Dataset) FileHandler.loadDataset(new File(genfile), (j - 1), ",");
				predictlist = (List<?>) methodPredict.invoke(null, dataset);
				logger.info("dataset attributes: " + dataset.noAttributes());
				break;

			case "java.lang.String":
				methodPredict = modelClass.getDeclaredMethod(modelMethodName, String.class);
				predictlist = (List<?>) methodPredict.invoke(null, rowString.toString());
				break;

			default:
				break;
			}

			Object[] predictor = new Object[row_count + 2];
			for (int i = 0; i <= row_count + 1; i++)
				predictor[i] = null;

			Method newBuilder = prediction.getMethod("newBuilder");
			Object object = newBuilder.invoke(null);
			Method addPrediction = object.getClass().getMethod("addPrediction", String.class);
			for (int i = 1; i <= row_count; i++) {
				addPrediction.invoke(object, predictlist.get(i - 1));

			}
			Method build = object.getClass().getMethod("build");

			Object pobj = build.invoke(object);

			Method toByteArray = pobj.getClass().getMethod("toByteArray");

			logger.info("In predict method: Done Prediction, returning binary serialization of prediction. ");
			return (byte[]) (toByteArray.invoke(pobj));

		} catch (Exception e) {
			logger.error("ERROR " + e.getMessage());
			e.printStackTrace();
			return null;
		}

	}

	private String getMethodParamType(Class<?> modelClass, String modelMethodName) {
		String fmt = "%24s: %s%n";
		String paramType = null;
		Method[] allMethods = modelClass.getDeclaredMethods();
		for (Method m : allMethods) {
			if (!m.getName().equals(modelMethodName)) {
				continue;
			}
			System.out.format("%s%n", m.toGenericString());

			System.out.format(fmt, "ReturnType", m.getReturnType());
			System.out.format(fmt, "GenericReturnType", m.getGenericReturnType());

			Class<?>[] pType = m.getParameterTypes();
			Type[] gpType = m.getGenericParameterTypes();
			for (int i = 0; i < pType.length; i++) {
				System.out.format(fmt, "ParameterType", pType[i]);

				paramType = gpType[i].getTypeName();

				// Below code needs to be removed later added as there overloaded predict method
				// with diff parameter
				if ("net.sf.javaml.core.Dataset".equals(paramType) || paramType.equals("java.io.File"))
					return paramType;
			}

		}
		return paramType;
	}

	private byte[] serialize(Object obj) throws IOException {
		try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
			try (ObjectOutputStream o = new ObjectOutputStream(b)) {
				o.writeObject(obj);
			}
			return b.toByteArray();
		}
	}

	private byte[] doPredict(Object df, String modelLoc) {
		try {
			/* start prediction */
			MojoModel mojo = null;
			EasyPredictModelWrapper model = null;
			try {
				if (modelLoc != null)
					mojo = MojoModel.load(modelLoc);
				else
					mojo = MojoModel.load(modelZip);
				model = new EasyPredictModelWrapper(mojo);
			} // try ends

			catch (IOException ie) {
				ie.getMessage();
			} // catch ends

			Method getRowsCount = dataframe.getMethod("getRowsCount");
			int row_count = (int) getRowsCount.invoke(df);
			logger.info("We have: " + row_count + " rows.");

			ArrayList<String> lst = new ArrayList<String>();
			Method getRowsList = dataframe.getMethod("getRowsList");
			List<?> list = (List<?>) getRowsList.invoke(df);
			RowData row = new RowData();
			for (Object obj : list) {
				int j = 0;
				for (String attr : attributes) {
					String attrMethodName = StringUtils.camelCase("get_" + attr, '_');
					Method attrMethod = dataframerow.getMethod(attrMethodName);
					Object attrValObj = attrMethod.invoke(obj);

					switch (attributeTypes.get(j)) {
					case "double":
						double attrValDouble = ((Double) attrValObj).doubleValue();
						row.put(attr, attrValDouble);
						break;

					case "float":
						float attrValFloat = ((Float) attrValObj).floatValue();
						row.put(attr, attrValFloat);
						break;

					case "int32":
					case "uint32":
					case "sint32":
					case "fixed32":
					case "sfixed32":
						int attrValInt = ((Integer) attrValObj).intValue();
						row.put(attr, attrValInt);
						break;

					case "int64":
					case "uint64":
					case "sint64":
					case "fixed64":
					case "sfixed64":
						long attrValLong = ((Long) attrValObj).longValue();
						row.put(attr, attrValLong);
						break;

					case "bool":
						boolean attrValBool = ((Boolean) attrValObj).booleanValue();
						row.put(attr, attrValBool);
						break;

					case "string":
						String attrValStr = (String) attrValObj;
						row.put(attr, attrValStr);
						break;
					case "bytes":
						byte attrValByte = ((Byte) attrValObj).byteValue();
						row.put(attr, attrValByte);
						break;
					default:
						break;
					}
					j++;

				}

				/*
				 * We handle the following model categories: Binomial Multinomial Regression
				 * Clustering AutoEncoder DimReduction WordEmbedding Unknown
				 */

				String current_model_category = mojo.getModelCategory().toString();
				logger.info("model category again: " + current_model_category);

				String pr = null;
				Object p = null;

				try {

					// Assume all predictions will be string for the time being.
					// When protobuf is autogenerated, we can infer this from
					// the model
					// category and handle it here.
					// But right now, if we return different datatypes/ i.e what
					// h2o
					// predictions return, the user who is handcrafting the
					// protobuf
					// will have to know the exact datatype.
					// Hence the decision to use string for the prediction.

					switch (current_model_category) {

					case "Binomial":
						p = model.predictBinomial(row);
						String bnmpred = ((BinomialModelPrediction) p).label;
						pr = bnmpred;
						break;

					case "Multinomial":
						p = model.predictMultinomial(row);
						String mnmpred = ((MultinomialModelPrediction) p).label;
						pr = mnmpred;
						break;

					case "Regression":
						p = model.predictRegression(row);
						double regpred = ((RegressionModelPrediction) p).value;
						pr = Double.toString(regpred);
						break;

					case "Clustering":
						p = model.predictClustering(row);
						int clspred = ((ClusteringModelPrediction) p).cluster;
						pr = Integer.toString(clspred);
						break;

					case "AutoEncoder":
						p = model.predictAutoEncoder(row);
						double[] autopred = ((AutoEncoderModelPrediction) p).reconstructed;
						pr = autopred.toString();
						break;

					case "DimReduction":
						p = model.predictDimReduction(row);
						double[] dimredpred = ((DimReductionModelPrediction) p).dimensions;
						pr = Arrays.toString(dimredpred);
						break;

					// TODO: See if this works
					case "WordEmbedding":
						p = model.predictWord2Vec(row);
						HashMap<String, float[]> word2vecpred = ((Word2VecPrediction) p).wordEmbeddings;
						pr = word2vecpred.toString();
						break;

					case "Unknown":
						logger.error(
								"Unknown model category. Results not available. Refer to http://docs.h2o.ai/h2o/latest-stable/h2o-genmodel/javadoc/hex ModelCategory.html");
						pr = "Unknown h2o model category. Results not available. Refer to http://docs.h2o.ai/h2o/latest-stable/h2o-genmodel/javadoc/hex/ModelCategory.html";
						break;

					default:
						logger.error(
								"Model category not recognized. Results not guaranteed.Refer to http://docs.h2o.ai/h2o/latest-stable/h2o-genmodel/javadoc/hex/ModelCategory.html");
						pr = "Your model did not match any supported category. Results not available.Refer to http://docs.h2o.ai/h2o/latest-stable/h2o-genmodel/javadoc/hex/ModelCategory.html";

					}

				} // try ends

				catch (PredictException pe) {
					pe.getMessage();
				} // catch ends

				logger.info("The prediction is  " + pr);

				lst.add(pr);
			}

			// Create a Prediction and set its value depending on the output of
			// the H2o predict method

			Object[] predictor = new Object[row_count + 2];
			for (int i = 0; i <= row_count + 1; i++)
				predictor[i] = null;

			Method newBuilder = prediction.getMethod("newBuilder");
			Object obj = newBuilder.invoke(null);
			Method addPrediction = obj.getClass().getMethod("addPrediction", String.class);

			for (int i = 1; i <= row_count; i++)
				addPrediction.invoke(obj, lst.get(i - 1));
			Method build = obj.getClass().getMethod("build");

			Object pobj = build.invoke(obj);

			Method toByteArray = pobj.getClass().getMethod("toByteArray");

			logger.info("In predict method: Done Prediction, returning binary serialization of prediction. ");
			return (byte[]) (toByteArray.invoke(pobj));

		} catch (Exception e) {
			logger.error("ERROR " + e.getMessage());
			e.printStackTrace();
			return null;

		}

	}

	@RequestMapping(value = "/predict", method = RequestMethod.POST)
	public byte[] predict(@RequestBody byte[] dataset) {
		logger.info("/predict GETTING POST REQUEST:");

		try {
			init(null);

			// dframe = DataFrame.parseFrom(datain);
			Method method = dataframe.getMethod("parseFrom", new Class[] { byte[].class });

			Object df = method.invoke(null, dataset);

			if (!modelType.equalsIgnoreCase("G"))
				return doPredict(df, null);
			else
				return doPredictJavaGeneric(df, null);

		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return null;
	}

	private void init(String protoString) throws Exception {

		setProjectRoot();
		logger.info("Project Root is " + projectRoot);
		protoFilePath = new String(projectRoot);

		String protoJarPath = null;
		pluginClassPath = new String(pluginRoot + sep + "classes");
		protoJarPath = pluginClassPath + sep + "pbuff.jar";
		protoOutputPath = new String(pluginRoot + sep + "src");

		// Make sure plugin Root directories exist. If not, create them.
		File dir = new File(pluginRoot);
		if (!dir.exists()) {
			dir.mkdirs();
			logger.info("Creating pluginRoot directory: " + pluginRoot);
		}

		File classdir = new File(pluginClassPath);
		if (!classdir.exists()) {
			classdir.mkdirs();
			logger.info("Creating plugin class path directory: " + pluginClassPath);
		}

		File srcdir = new File(protoOutputPath);
		if (!srcdir.exists()) {
			srcdir.mkdirs();
			logger.info("Creating plugin src directory: " + protoOutputPath);
		}

		modelZip = new String(projectRoot + relModelZip);
		defaultProtofile = new String(projectRoot + relDefaultProtofile);

		generateProto(protoString); // Use null for now.

		cl = RunnerController.class.getClassLoader();

		addFile(protoJarPath);

		// loading DatasetProto$ classes
		dataframerow = cl.loadClass("com.google.protobuf.DatasetProto$DataFrameRow");
		dataframe = cl.loadClass("com.google.protobuf.DatasetProto$DataFrame");
		prediction = cl.loadClass("com.google.protobuf.DatasetProto$Prediction");
		dataframeBuilder = cl.loadClass("com.google.protobuf.DatasetProto$DataFrame$Builder");
	}

	private boolean setProtoAttributes(String protoString) throws Exception {
		if (protoString == null)
			return false;

		attributes = new ArrayList<String>();
		attributeTypes = new ArrayList<String>();

		String[] types = { "double", "float", "int32", "int64", "uint32", "uint64", "sint32", "sint64", "fixed32",
				"fixed64", "sfixed32", "sfixed64", "bool", "string", "bytes" };

		int idx_msg1 = protoString.indexOf("message DataFrameRow");
		int idx_begincurly1 = protoString.indexOf("{", idx_msg1);
		int idx_endcurly1 = protoString.indexOf("}", idx_begincurly1);
		if (idx_msg1 == -1 || idx_begincurly1 == -1 || idx_endcurly1 == -1) {
			logger.error("Wrong proto String format!");
			return false;
		}
		int idx = idx_begincurly1 + 1;

		do {
			int prev_idx = -2;
			int cur_idx = -2;
			int target_idx = -2;
			String prev_type = "";
			String target_type = "";
			for (String cur_type : types) {

				cur_idx = protoString.indexOf(cur_type, idx);
				if (cur_idx != -1) { // Do something
					target_type = cur_type;

					if (prev_idx == -2) {
						prev_idx = cur_idx;
						prev_type = cur_type;
						target_idx = cur_idx;
					} else if (prev_idx > cur_idx) { // should use current
														// type then
						target_idx = cur_idx;
						prev_idx = cur_idx;
						prev_type = cur_type;

					} else { // should still use the prev one
						target_type = prev_type;

					}
				}
			}
			if (target_idx == -2)
				break;
			int begin_attr = target_idx + target_type.length() + 1;
			int end_attr = protoString.indexOf(" ", begin_attr);
			if (begin_attr > idx_endcurly1 || end_attr > idx_endcurly1)
				break;
			attributes.add(protoString.substring(begin_attr, end_attr));
			attributeTypes.add(target_type);

			idx = end_attr + 1;

		} while (idx > idx_begincurly1 && idx < idx_endcurly1);

		return true;

	}

	private void generateProto(String protoString) throws Exception {
		if (protoString == null)
			protoString = getDefaultProtoString();

		if (protoString == null)
			protoString = generateMockMessage();

		setProtoAttributes(protoString);
		String protoFilename = protoFilePath + sep + "dataset.proto";
		writeProtoFile(protoString, protoFilename);
		generateJavaProtoCode();
		buildProtoClasses();
		updateProtoJar();
	}

	private String getDefaultProtoString() throws IOException {
		String result = null;
		defaultProtofile = new String(projectRoot + relDefaultProtofile);
		InputStream is = new FileInputStream(defaultProtofile);
		logger.info("finding default proto:" + defaultProtofile);
		result = IOUtils.toString(is, StandardCharsets.UTF_8);

		return result;
	}

	private String generateMockMessage() {
		String result;

		String PROTO_SYNTAX = "syntax = \"proto3\";\n";
		String option1 = "option java_package = \"com.google.protobuf\";\n";
		String option2 = "option java_outer_classname = \"DatasetProto\";\n";

		result = PROTO_SYNTAX + option1 + option2 + "message DataFrameRow {\n" + "string sepal_len = 1;\n"
				+ "string sepal_wid = 2;\n" + "string petal_len = 3;\n" + "string petal_wid = 4;\n" + "}\n"
				+ "message DataFrame {\n" + " 	repeated DataFrameRow rows = 1;\n" + "}\n" + "message Prediction {\n"
				+ "	repeated string prediction= 1;\n" + "}\n";

		return result;
	}

	private void writeProtoFile(String protoString, String filename) {
		BufferedWriter bw = null;
		FileWriter fw = null;

		try {
			fw = new FileWriter(filename);
			bw = new BufferedWriter(fw);
			bw.write(protoString);

			logger.info("Done writing dataset.proto");
		} catch (IOException e) {
			logger.error(e.getMessage());
			e.printStackTrace();

		} finally {
			try {
				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();
			} catch (IOException ex) {
				logger.error(ex.getMessage());
				ex.printStackTrace();
			}
		}

	}

	private void generateJavaProtoCode() {
		String cmd;
		int exitVal = -1;

		cmd = "protoc -I=" + protoFilePath + " --java_out=" + protoOutputPath + " " + protoFilePath + sep
				+ "dataset.proto";

		try {
			exitVal = runCommand(cmd);

		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}

		if (exitVal != 0)
			logger.error("Failed generating DataframeProto.java");
		else
			logger.info("Complete generating DataframeProto.java!");
	}

	private void buildProtoClasses() {
		String buildPath;
		String protoJavaRuntimeJar;
		String cmd;
		int exitVal = -1;

		buildPath = protoOutputPath + sep + "com" + sep + "google" + sep + "protobuf" + sep;
		protoJavaRuntimeJar = projectRoot;
		cmd = "javac -cp " + protoJavaRuntimeJar + sep + "protobuf-java-3.4.0.jar " + buildPath + sep
				+ "DatasetProto.java -d " + pluginClassPath;

		try {
			exitVal = runCommand(cmd);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		if (exitVal != 0)
			logger.error("Failed creating proto class files");
		else
			logger.info("Completed creating class files!");
	}

	private void updateProtoJar() {
		int exitVal = -1;

		try {
			String protort = projectRoot + sep + "protobuf-java-3.4.0.jar";
			ProcessBuilder pb = new ProcessBuilder("jar", "-xvf", protort);
			pb.directory(new File(pluginClassPath));

			logger.info("Setting directory to : " + pluginClassPath + " before producing/updating pbuff.jar");
			logger.info("executing command: \"jar -xvf " + protort + "\" from directory " + pluginClassPath);
			Process p = pb.start();
			// get the error stream of the process and print it
			InputStream error = p.getErrorStream();
			for (int i = 0; i < error.available(); i++) {
				logger.error("" + error.read());
			}

			PrintWriter printWriter = new PrintWriter(p.getOutputStream());
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			bufferedReader.close();
			printWriter.flush();
			exitVal = p.waitFor();

			logger.info("Exit Value: " + exitVal);
			File plugin = new File(pluginClassPath);
			JarOutputStream target = new JarOutputStream(new FileOutputStream(pluginClassPath + sep + "pbuff.jar"));
			add(plugin, plugin, target);
			target.close();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Failed producing/updating pbuff.jar " + e.getMessage());
			return;
		}
		if (exitVal != 0)
			logger.error("Failed extracting protobuf JAVA runtime");
		else
			logger.info("Completed producing/updating pbuff.jar!");

	}

	private void add(File root, File source, JarOutputStream target) throws IOException {
		BufferedInputStream in = null;
		try {
			if (source.isDirectory()) {
				String name = source.getPath().replace("\\", "/");
				if (!name.isEmpty()) {
					if (!name.endsWith("/")) {
						name += "/";
					}
					JarEntry entry = new JarEntry(name);
					entry.setTime(source.lastModified());
					target.putNextEntry(entry);
					target.closeEntry();
				}
				for (File nestedFile : source.listFiles()) {
					add(root, nestedFile, target);
				}
				return;
			}

			String relPath = source.getCanonicalPath().substring(root.getCanonicalPath().length() + 1,
					source.getCanonicalPath().length());
			if (!relPath.endsWith("class")) {
				return;
			}

			JarEntry entry = new JarEntry(relPath.replace("\\", "/"));
			entry.setTime(source.lastModified());
			target.putNextEntry(entry);
			in = new BufferedInputStream(new FileInputStream(source));

			byte[] buffer = new byte[1024];
			while (true) {
				int count = in.read(buffer);
				if (count == -1) {
					break;
				}
				target.write(buffer, 0, count);
			}
			target.closeEntry();
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	private int runCommand(String cmd) throws Exception {
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

	private void setProjectRoot() {
		projectRoot = System.getProperty("user.dir");
	}

	/**
	 * Adds the content pointed by the URL to the classpath.
	 * 
	 * @param u:
	 *            the URL pointing to the content to be added
	 * @throws IOException
	 */
	public static void addURL(URL u) throws IOException {
		URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		Class<?> sysclass = URLClassLoader.class;
		try {
			Method method = sysclass.getDeclaredMethod("addURL", parameters);
			method.setAccessible(true);
			method.invoke(sysloader, new Object[] { u });
		} catch (Throwable t) {
			t.printStackTrace();
			throw new IOException("Error, could not add URL to system classloader");
		}
	}

	/**
	 * Adds a file to the classpath.
	 * 
	 * @param s:
	 *            a String pointing to the file
	 * @throws IOException
	 */
	public static void addFile(String s) throws IOException {
		File f = new File(s);
		addFile(f);
	}

	/**
	 * Adds a file to the classpath
	 * 
	 * @param f:
	 *            the file to be added
	 * @throws IOException
	 */
	public static void addFile(File f) throws IOException {
		addURL(f.toURI().toURL());
	}

}
