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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.MediaType;

import org.acumos.modelrunner.domain.*;
import org.acumos.modelrunner.domain.MessageObject.AttributeEntity;
import org.apache.commons.io.IOUtils;
import org.metawidget.util.simple.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.io.Files;

import hex.genmodel.MojoModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.AutoEncoderModelPrediction;
import hex.genmodel.easy.prediction.BinomialModelPrediction;
import hex.genmodel.easy.prediction.ClusteringModelPrediction;
import hex.genmodel.easy.prediction.DimReductionModelPrediction;
import hex.genmodel.easy.prediction.MultinomialModelPrediction;
import hex.genmodel.easy.prediction.RegressionModelPrediction;
import hex.genmodel.easy.prediction.Word2VecPrediction;
import io.swagger.annotations.ApiOperation;

@RestController
public class RunnerController {
	@Value("${plugin_root}")
	private String pluginRoot;

	@Value("${default_model}")
	private String defaultModel;

	@Value("${default_protofile}")
	private String defaultProto;

	@Value("${model_type}")
	private String modelType;

	@Value("${model_config}")
	private String modelConfig;

	private static final Logger logger = LoggerFactory.getLogger(RunnerController.class);
	private static final Class<?>[] parameters = new Class[] { URL.class };
	private static final String SEP = File.separator;
	private static final String NEWLINE = System.lineSeparator();
	private static final String TMPPATH = System.getProperty("java.io.tmpdir");

	private static final String PROJECTROOT = System.getProperty("user.dir");
	private static final String FLOAT = "float";
	private static final String DOUBLE = "double";
	private static final String INT32 = "int32";
	private static final String UINT32 = "uint32";
	private static final String INT64 = "int64";
	private static final String UINT64 = "uint64";
	private static final String SINT64 = "sint64";
	private static final String FIXED64 = "fixed64";
	private static final String SFIXED64 = "sfixed64";
	private static final String SINT32 = "sint32";
	private static final String FIXED32 = "fixed32";
	private static final String SFIXED32 = "sfixed32";
	private static final String BOOL = "bool";
	private static final String STRING = "string";
	private static final String BYTES = "bytes";

	private String modelZip = null;
	private String defaultProtofile;
	private String protoFilePath = null;
	private String protoOutputPath = null;
	private String pluginClassPath = null;
	private String pluginPkgName = null;
	private String protoJarPath = null;
	private String inputClassName = null;
	private String outputClassName = null;
	private String serviceName = null;
	private ArrayList<String> attributes = new ArrayList<>();
	private ArrayList<String> attributeTypes = new ArrayList<>();
	private Class<?> dataframerow;
	private Class<?> dataframe;
	private Class<?> prediction;
	private Class<?> dataframeBuilder;
	private HashMap<String, MessageObject> classList = new HashMap<>();
	private HashMap<String, ServiceObject> serviceList = new HashMap<>();
	private ArrayList<String> classNames = new ArrayList<>();
	private Properties prop = new Properties();
	private String protoRTVersion = null;
	private ClassLoader cl = null;

	private String[] types = { DOUBLE, FLOAT, INT32, INT64, UINT32, UINT64, SINT32, SINT64, FIXED32, FIXED64, SFIXED32,
			SFIXED64, BOOL, STRING, BYTES };

	@RequestMapping(value = "/hello", method = RequestMethod.GET)
	public String hello() {
		return "HelloWorld";
	}

	/**
	 * End point for a modeler to upload a model
	 * 
	 * @param model
	 *            Uploaded model
	 * @return ResponseEntity
	 */
	@ApiOperation(value = "Upload a machine learning model", response = Map.class)
	@RequestMapping(value = "/putModel", method = RequestMethod.PUT)
	public ResponseEntity<Map<String, String>> putModel(@RequestPart("model") MultipartFile model) {
		logger.info("Receiving /putModel PUT request...");
		Map<String, String> results = new LinkedHashMap<>();

		try {
			if (model != null && !model.isEmpty()) {
				byte[] bytes = model.getBytes();

				// Create the file on server
				File modelFile = new File(PROJECTROOT + SEP + "models" + SEP + model.getOriginalFilename());
				BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(modelFile));
				stream.write(bytes);
				stream.close();

				logger.info("Model File Location=" + modelFile.getAbsolutePath());
			}
		} catch (Exception ex) {
			logger.error("Failed in uploading model: ", ex);
			results.put("status", "bad request");
			results.put("message", ex.getMessage());
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}

		results.put("status", "ok");
		return new ResponseEntity<>(results, HttpStatus.OK);
	}

	/**
	 * End point to upload a proto file
	 * 
	 * @param proto
	 *            Protobuf file
	 * @return ResponseEntity
	 */
	@ApiOperation(value = "Upload a protofile", response = ResponseEntity.class)
	@RequestMapping(value = "/putProto", method = RequestMethod.PUT)
	public ResponseEntity<Map<String, String>> putProto(@RequestPart("proto") MultipartFile proto) {
		logger.info("Receiving /putProto PUT request...");
		Map<String, String> results = new LinkedHashMap<>();

		try {
			if (proto != null && !proto.isEmpty()) {
				byte[] bytes = proto.getBytes();

				// Create the file on server
				File protoFile = new File(PROJECTROOT + SEP + "models" + SEP + proto.getOriginalFilename());
				BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(protoFile));
				stream.write(bytes);
				stream.close();

				logger.info("Proto File Location=" + protoFile.getAbsolutePath());
			}
		} catch (Exception ex) {
			logger.error("Failed in uploading protofile: ", ex);
			results.put("status", "bad request");
			results.put("message", ex.getMessage());
			return new ResponseEntity<>(results, HttpStatus.BAD_REQUEST);
		}

		results.put("status", "ok");
		return new ResponseEntity<>(results, HttpStatus.OK);
	}

	/**
	 * getBinaryDefault converts the uploaded csv file based on the default .proto
	 * file
	 * 
	 * @param csvFile
	 *            CSV file
	 * @return binary stream in protobuf format as inputs for the predictor
	 */
	@ApiOperation(value = "Converts the csv file based on the default .proto to a binary stream in protobuf format.")
	@RequestMapping(value = "/getBinaryDefault", method = RequestMethod.POST, produces = "application/octet-stream")
	public byte[] getBinaryDefault(@RequestPart("csvFile") MultipartFile csvFile) {
		logger.info("Receiving /getBinaryDefault POST request...");
		return getBinary_(csvFile, null);

	}

	/**
	 * getBinary converts the uploaded csv file based on the uploaded proto file
	 * 
	 * @param csvFile
	 *            CSV file
	 * @param proto
	 *            Protobuf file
	 * @return binary stream in protobuf format as inputs to the predictor
	 */
	@ApiOperation(value = "Gets a binary stream in protobuf format based on the provided csv file and .proto file as inputs for the predictor")
	@RequestMapping(value = "/getBinary", method = RequestMethod.POST, produces = MediaType.APPLICATION_OCTET_STREAM)
	public byte[] getBinary(@RequestPart("csvFile") MultipartFile csvFile, @RequestPart("proto") MultipartFile proto) {
		logger.info("Receiving /getBinary POST request...");

		return getBinary_(csvFile, proto);
	}

	public byte[] getBinary_(MultipartFile file, MultipartFile proto) {
		try {
			Object df = getDataFrameBuilder(file, proto); // df is of
															// DataFrame.Builder
															// type
			Method dfBuilder = df.getClass().getMethod("build");
			Object obj = dfBuilder.invoke(df);

			Method tobytearray = obj.getClass().getSuperclass().getSuperclass().getSuperclass()
					.getDeclaredMethod("toByteArray");
			byte[] barray = (byte[]) tobytearray.invoke(obj);
			logger.info("Returning the following byte[] :");
			logger.info(Arrays.toString(barray));
			return barray;

		} catch (Exception ex) {
			logger.error("Failed getting binary stream inputs:", ex);
			return new byte[0];
		}
	}

	/**
	 * 
	 * @param csvFile
	 *            CSV file
	 * @return prediction binary stream in protobuf format
	 */
	@ApiOperation(value = "Gets a prediction binary stream in protobuf format based on the provided csv file and .proto file")
	@RequestMapping(value = "/transformDefault", method = RequestMethod.POST)
	public byte[] transform(@RequestPart("csvFile") MultipartFile csvFile) {
		logger.info("Receiving /transformDefault POST Request...");
		return transform_(csvFile, null, null);

	}

	/**
	 * @param csvFile
	 *            CSV File
	 * @param model
	 *            H2O model
	 * @param proto
	 *            Protobuf file
	 * @return prediction binary stream in protobuf format
	 */
	@ApiOperation(value = "Gets a prediction binary stream in protobuf format based on the provided csv file, H2O model, and .proto file")
	@RequestMapping(value = "/transform", method = RequestMethod.POST)
	public byte[] transform(@RequestPart("csvFile") MultipartFile csvFile, @RequestPart("model") MultipartFile model,
			@RequestPart("proto") MultipartFile proto) {
		logger.info("Receiving /transform POST Request...");
		return transform_(csvFile, model, proto);
	}

	/**
	 * 
	 * @param file
	 *            Data
	 * @param model
	 *            Model
	 * @param proto
	 *            Protobuf
	 * @return prediction binary stream in protobuf format
	 */
	public byte[] transform_(MultipartFile file, MultipartFile model, MultipartFile proto) {
		try {
			Object df = getDataFrameBuilder(file, proto); // df is of
															// DataFrame.Builder
															// type

			Method dfBuilder = df.getClass().getMethod("build");
			Object obj = dfBuilder.invoke(df);

			String modelLoc = null;
			if (model != null && !model.isEmpty()) {
				byte[] bytes = model.getBytes();

				// Creating the directory to store file

				File dir = new File(TMPPATH + SEP + "tmpFiles");
				if (!dir.exists())
					dir.mkdirs();

				// Create the file on server
				File modelFile;
				if (!modelType.equalsIgnoreCase("G"))
					modelFile = new File(dir.getAbsolutePath() + SEP + "model_" + UUID.randomUUID() + ".zip");
				else
					modelFile = new File(dir.getAbsolutePath() + SEP + "model_" + UUID.randomUUID() + ".jar");

				BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(modelFile));
				stream.write(bytes);
				stream.close();
				modelLoc = modelFile.getAbsolutePath();
				logger.info("model File Location=" + modelFile.getAbsolutePath());
			}

			if (!modelType.equalsIgnoreCase("G"))
				return doPredict(obj, modelLoc);
			else
				return doPredictJavaGeneric(obj, modelLoc);
		} catch (Exception ex) {
			logger.error("Failed transforming csv file and getting prediction results: ", ex);

		}

		return new byte[0];
	}

	/**
	 * 
	 * @param file
	 * @param proto
	 * @return DataFrame.Builder
	 * @throws Exception
	 */
	private Object getDataFrameBuilder(MultipartFile file, MultipartFile proto) throws Exception {
		if (file.isEmpty()) {
			logger.error("You failed to upload " + file.getOriginalFilename() + " because the file was empty.");
			return null;
		}

		Object df = null;

		String contentType = file.getContentType();
		if (!"application/vnd.ms-excel".equalsIgnoreCase(contentType) && !"text/csv".equalsIgnoreCase(contentType)) {
			logger.error("Wrong file type. Current content type is " + contentType);
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
		df = dfNewBuilder.invoke(null); // df is of DataFrame.Builder
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
		String[] lines = dataString.split(NEWLINE);

		for (String line : lines) {
			String[] array = line.split(",");

			logger.info("Current line is: " + line);

			for (int i = 0; i < array.length; i++) {
				if (array[i].length() == 0) // skip missing field
					continue;

				String attr = attributes.get(i);
				String attrMethodName = StringUtils.camelCase("set_" + attr, '_');
				Method attrMethod = null;
				switch (attributeTypes.get(i)) {
				case DOUBLE:
					attrMethod = dfr.getClass().getMethod(attrMethodName, double.class);
					attrMethod.invoke(dfr, Double.parseDouble(array[i]));
					break;

				case FLOAT:
					attrMethod = dfr.getClass().getMethod(attrMethodName, float.class);
					attrMethod.invoke(dfr, Float.parseFloat(array[i]));
					break;

				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					attrMethod = dfr.getClass().getMethod(attrMethodName, int.class);
					attrMethod.invoke(dfr, Integer.parseInt(array[i]));
					break;

				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					attrMethod = dfr.getClass().getMethod(attrMethodName, long.class);
					attrMethod.invoke(dfr, Long.parseLong(array[i]));
					break;

				case BOOL:
					attrMethod = dfr.getClass().getMethod(attrMethodName, boolean.class);
					attrMethod.invoke(dfr, Boolean.parseBoolean(array[i]));
					break;

				case STRING:
					attrMethod = dfr.getClass().getMethod(attrMethodName, String.class);
					attrMethod.invoke(dfr, array[i]);
					break;

				case BYTES:
					attrMethod = dfr.getClass().getMethod(attrMethodName, byte.class);
					attrMethod.invoke(dfr, Byte.parseByte(array[i]));
					break;
				default:
					break;
				}

			}
			dfAddRows.invoke(df, dfr);
		}

		return df;
	}

	//
	/**
	 * Get {InputClass}.Builder class based on uploaded data file and proto file
	 * 
	 * @param file
	 * @param proto
	 * @return {InputClass}.Builder
	 * @throws Exception
	 */
	private Object getInputClassBuilder(MultipartFile file, MultipartFile proto) throws Exception {
		if (file.isEmpty()) {
			logger.error("You failed to upload " + file.getOriginalFilename() + " because the file was empty.");
			return null;
		}

		Object df = null;

		String contentType = file.getContentType();
		if (!"application/vnd.ms-excel".equalsIgnoreCase(contentType) && !"text/csv".equalsIgnoreCase(contentType)) {
			logger.error("Wrong file type. Current content type is " + contentType);
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
		df = dfNewBuilder.invoke(null); // df is of DataFrame.Builder
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
		String[] lines = dataString.split(NEWLINE);

		for (String line : lines) {
			String[] array = line.split(",");

			logger.info("Current line is: " + line);

			for (int i = 0; i < array.length; i++) {
				if (array[i].length() == 0) // skip missing field
					continue;

				String attr = attributes.get(i);
				String attrMethodName = StringUtils.camelCase("set_" + attr, '_');
				Method attrMethod = null;
				switch (attributeTypes.get(i)) {
				case DOUBLE:
					attrMethod = dfr.getClass().getMethod(attrMethodName, double.class);
					attrMethod.invoke(dfr, Double.parseDouble(array[i]));
					break;

				case FLOAT:
					attrMethod = dfr.getClass().getMethod(attrMethodName, float.class);
					attrMethod.invoke(dfr, Float.parseFloat(array[i]));
					break;

				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					attrMethod = dfr.getClass().getMethod(attrMethodName, int.class);
					attrMethod.invoke(dfr, Integer.parseInt(array[i]));
					break;

				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					attrMethod = dfr.getClass().getMethod(attrMethodName, long.class);
					attrMethod.invoke(dfr, Long.parseLong(array[i]));
					break;

				case BOOL:
					attrMethod = dfr.getClass().getMethod(attrMethodName, boolean.class);
					attrMethod.invoke(dfr, Boolean.parseBoolean(array[i]));
					break;

				case STRING:
					attrMethod = dfr.getClass().getMethod(attrMethodName, String.class);
					attrMethod.invoke(dfr, array[i]);
					break;

				case BYTES:
					attrMethod = dfr.getClass().getMethod(attrMethodName, byte.class);
					attrMethod.invoke(dfr, Byte.parseByte(array[i]));
					break;
				default:
					break;
				}

			}
			dfAddRows.invoke(df, dfr);
		}

		return df;
	}

	/*
	 * in the case of nestedmsg.proto First iteration : parent - DataFrame, child -
	 * DataFrameRow Second iteration: parent - DataFrameRow, child - SubFrameRow
	 */
	private void getRowString(Object df, String parentName, String attributeName, String childName,
			StringBuffer rowStr) {
		try {
			logger.info("getRowString: " + parentName + " | " + childName);
			MessageObject parentMsg = classList.get(parentName);
			Class<?> parentCls = parentMsg.getCls();
			ArrayList<AttributeEntity> parentAttributes = parentMsg.getAttributes();

			MessageObject childMsg = null;
			Class<?> childCls = null;
			ArrayList<AttributeEntity> childAttributes = null;
			if (childName != null) {
				childMsg = classList.get(childName);
				childCls = childMsg.getCls();
				childAttributes = childMsg.getAttributes();
			}

			for (AttributeEntity ae : parentAttributes) {
				if (attributeName != null && !attributeName.equals(ae.getName()))
					continue;

				if (ae.isRepeated()) {
					String pAttrMethodName = StringUtils.camelCase("get_" + ae.getName(), '_');
					Method getCount = parentCls.getMethod(pAttrMethodName + "Count");
					int rowCount = (int) getCount.invoke(df);
					logger.info("We have: " + rowCount + " rows of " + ae.getName());

					Method getList = parentCls.getMethod(pAttrMethodName + "List");
					List<?> list = (List<?>) getList.invoke(df); // list of child objects or list of primitive types

					Object obj;
					for (int j = 0; j < rowCount; j++) {
						obj = list.get(j);
						
						switch (ae.getType()) {
						case DOUBLE:
							double attrValDouble = ((Double) obj).doubleValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(attrValDouble);
							break;

						case FLOAT:
							float attrValFloat = ((Float) obj).floatValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(attrValFloat);
							break;

						case INT32:
						case UINT32:
						case SINT32:
						case FIXED32:
						case SFIXED32:
							int attrValInt = ((Integer) obj).intValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(attrValInt);
							break;

						case INT64:
						case UINT64:
						case SINT64:
						case FIXED64:
						case SFIXED64:
							long attrValLong = ((Long) obj).longValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(attrValLong);
							break;

						case BOOL:
							boolean attrValBool = ((Boolean) obj).booleanValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(attrValBool);
							break;

						case STRING:
							String attrValStr = (String) obj;
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(attrValStr);
							break;

						case BYTES:
							byte attrValByte = ((Byte) obj).byteValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(attrValByte);
							break;

						default:
							if (childName == null) {
								childName = ae.getType();
								childMsg = classList.get(ae.getType());
								childCls = childMsg.getCls();
								childAttributes = childMsg.getAttributes();
							}

							for (AttributeEntity cae : childAttributes) {
								String cAttrMethodName = StringUtils.camelCase("get_" + cae.getName(), '_');
								Method cAttrMethod = childCls.getMethod(cAttrMethodName);
								Object gcObj = cAttrMethod.invoke(obj);
								
								switch (cae.getType()) {
								case DOUBLE:
									double gcValDouble = ((Double) gcObj).doubleValue();
									if(rowStr.length() != 0)
										rowStr.append(","); 
									rowStr.append(gcValDouble);
									break;

								case FLOAT:
									float gcValFloat = ((Float) gcObj).floatValue();
									if(rowStr.length() != 0)
										rowStr.append(","); 
									rowStr.append(gcValFloat);
									break;

								case INT32:
								case UINT32:
								case SINT32:
								case FIXED32:
								case SFIXED32:
									int gcValInt = ((Integer) gcObj).intValue();
									if(rowStr.length() != 0)
										rowStr.append(","); 
									rowStr.append(gcValInt);
									break;

								case INT64:
								case UINT64:
								case SINT64:
								case FIXED64:
								case SFIXED64:
									long gcValLong = ((Long) gcObj).longValue();
									if(rowStr.length() != 0)
										rowStr.append(","); 
									rowStr.append(gcValLong);
									break;

								case BOOL:
									boolean gcValBool = ((Boolean) gcObj).booleanValue();
									if(rowStr.length() != 0)
										rowStr.append(","); 
									rowStr.append(gcValBool);
									break;

								case STRING:
									String gcValStr = (String) gcObj;
									if(rowStr.length() != 0)
										rowStr.append(","); 
									rowStr.append(gcValStr);
									break;

								case BYTES:
									byte gcValByte = ((Byte) gcObj).byteValue();
									if(rowStr.length() != 0)
										rowStr.append(","); 
									rowStr.append(gcValByte);
									break;

								default:
									getRowString(gcObj, cae.getType(), null, null, rowStr); // use df instead of gcobj first
//									getRowString(df, childName, cae.getName(), cae.getType(), rowStr); // use df instead of gcobj first
									break;
								}

							}
							break;
						}
					}
				} else {
					String pAttrMethodName = StringUtils.camelCase("get_" + ae.getName(), '_');
					Method pAttrMethod = parentCls.getMethod(pAttrMethodName);
					Object cObj = pAttrMethod.invoke(df);
					
					for (AttributeEntity cae : childAttributes) {
						String cAttrMethodName = StringUtils.camelCase("get_" + cae.getName(), '_');
						Method cAttrMethod = childCls.getMethod(cAttrMethodName);
						Object gcObj = cAttrMethod.invoke(cObj);
						
						switch (cae.getType()) {
						case DOUBLE:
							double gcValDouble = ((Double) gcObj).doubleValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(gcValDouble);
							break;

						case FLOAT:
							float gcValFloat = ((Float) gcObj).floatValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(gcValFloat);
							break;

						case INT32:
						case UINT32:
						case SINT32:
						case FIXED32:
						case SFIXED32:
							int gcValInt = ((Integer) gcObj).intValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(gcValInt);
							break;

						case INT64:
						case UINT64:
						case SINT64:
						case FIXED64:
						case SFIXED64:
							long gcValLong = ((Long) gcObj).longValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(gcValLong);
							break;

						case BOOL:
							boolean gcValBool = ((Boolean) gcObj).booleanValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(gcValBool);
							break;

						case STRING:
							String gcValStr = (String) gcObj;
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(gcValStr);
							break;
						case BYTES:
							byte gcValByte = ((Byte) gcObj).byteValue();
							if(rowStr.length() != 0)
								rowStr.append(","); 
							rowStr.append(gcValByte);
							break;
						default: // TODO
							getRowString(gcObj, childName, cae.getName(), cae.getType(), rowStr);
							break;
						}
					}
					rowStr.append("\n");// TODO: is it done yet?
				}
				logger.info(rowStr.toString());
			}
		} catch (Exception ex) {
			logger.error("Failed in getRowString(): ", ex);
		}
	}

	/**
	 * This procedure uses Java ML model to do prediction
	 * 
	 * @param df
	 * @param modelLoc
	 * @return prediction binary stream in protobuf format
	 */
	private byte[] doPredictGeneric(Object df, String modelLoc) {
		try {
			StringBuffer rowString = new StringBuffer();
			
			getRowString(df, inputClassName, null, null, rowString);
			logger.info(rowString.toString());
			String tempPath = TMPPATH + SEP + "tmpFiles";
			File dir = new File(tempPath);
			if (!dir.exists())
				dir.mkdirs();
			String genfile = tempPath + SEP + UUID.randomUUID() + "_genfile.csv";
			FileWriter ff = new FileWriter(genfile);
			ff.write(rowString.toString());
			ff.close();

			String propFile = new String(PROJECTROOT + modelConfig);
			// Load property
			Properties prop = new Properties();
			InputStream input = new FileInputStream(propFile);
			prop.load(input);

			String modelMethodName = prop.getProperty("modelMethod");
			String modelClassName = prop.getProperty("modelClassName");

			logger.info("model class name and method=" + modelClassName + "  " + modelMethodName);

			// model invoke and preparation

			File modelSource = new File(PROJECTROOT + defaultModel);

			File modelJarPath = new File(pluginClassPath + SEP + UUID.randomUUID() + modelSource.getName());
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

			case "java.lang.String":
				methodPredict = modelClass.getDeclaredMethod(modelMethodName, String.class);
				predictlist = (List<?>) methodPredict.invoke(null, rowString.toString());
				break;

			default:
				break;
			}

			if (predictlist == null) {
				logger.debug("predictlist is null");
				return null;
			}
			int row_count = 10;
			Object[] predictor = new Object[row_count + 2];
			for (int i = 0; i <= row_count + 1; i++)
				predictor[i] = null;

			Class<?> prediction = classList.get(outputClassName).getCls();
			Method newBuilder = prediction.getMethod("newBuilder");
			Object object = newBuilder.invoke(null);
			Method addPrediction;
			// Method addPrediction = object.getClass().getMethod("addPrediction",
			// String.class);
			ArrayList<AttributeEntity> outputAttributes = classList.get(outputClassName).getAttributes();
			for (AttributeEntity ae : outputAttributes) {
				String predictMethodName;
				switch (predictlist.get(0).getClass().getSimpleName()) {
				case "Integer":
				case "Float":
				case "Double":
				case "Boolean":
				case "Long":
				case "Byte":
					predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
					addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);
					// addPrediction = object.getClass().getMethod("addAllPrediction",
					// java.lang.Iterable.class);
					addPrediction.invoke(object, predictlist);
					break;

				default:
					predictMethodName = StringUtils.camelCase("add_" + ae.getName(), '_');
					addPrediction = object.getClass().getMethod(predictMethodName, predictlist.get(0).getClass());
					// addPrediction = object.getClass().getMethod("addPrediction",
					// predictlist.get(0).getClass());
					for (int i = 1; i <= row_count; i++) {
						addPrediction.invoke(object, predictlist.get(i - 1));
						// addPrediction.invoke(object, String.valueOf(predictlist.get(i - 1)));
					}
					break;
				}
			}

			Method build = object.getClass().getMethod("build");

			Object pobj = build.invoke(object);

			Method toByteArray = pobj.getClass().getMethod("toByteArray");

			logger.info("In predict method: Done Prediction, returning binary serialization of prediction. ");
			return (byte[]) (toByteArray.invoke(pobj));

		} catch (Exception ex) {
			logger.error("Failed in doPredictGeneric: ", ex);
			return null;
		}

	}

	/**
	 * This procedure uses Java ML model to do prediction
	 * 
	 * @param df
	 * @param modelLoc
	 * @return prediction binary stream in protobuf format
	 */
	private byte[] doPredictJavaGeneric(Object df, String modelLoc) {
		try {

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
					case DOUBLE:
						double attrValDouble = ((Double) attrValObj).doubleValue();
						rowString.append(attrValDouble);
						break;

					case FLOAT:
						float attrValFloat = ((Float) attrValObj).floatValue();
						rowString.append(attrValFloat);
						break;

					case INT32:
					case UINT32:
					case SINT32:
					case FIXED32:
					case SFIXED32:
						int attrValInt = ((Integer) attrValObj).intValue();
						rowString.append(attrValInt);
						break;

					case INT64:
					case UINT64:
					case SINT64:
					case FIXED64:
					case SFIXED64:
						long attrValLong = ((Long) attrValObj).longValue();
						rowString.append(attrValLong);
						break;

					case BOOL:
						boolean attrValBool = ((Boolean) attrValObj).booleanValue();
						rowString.append(attrValBool);
						break;

					case STRING:
						String attrValStr = (String) attrValObj;
						rowString.append(attrValStr);
						break;
					case BYTES:
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
			String tempPath = TMPPATH + SEP + "tmpFiles";
			File dir = new File(tempPath);
			if (!dir.exists())
				dir.mkdirs();
			String genfile = tempPath + SEP + UUID.randomUUID() + "_genfile.csv";
			FileWriter ff = new FileWriter(genfile);
			ff.write(rowString.toString());
			ff.close();

			String propFile = new String(PROJECTROOT + modelConfig);
			// Load property
			Properties prop = new Properties();
			InputStream input = new FileInputStream(propFile);
			prop.load(input);

			String modelMethodName = prop.getProperty("modelMethod");
			String modelClassName = prop.getProperty("modelClassName");

			logger.info("model class name and method=" + modelClassName + "  " + modelMethodName);

			// model invoke and preparation

			File modelSource = new File(PROJECTROOT + defaultModel);

			File modelJarPath = new File(pluginClassPath + SEP + UUID.randomUUID() + modelSource.getName());
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

			case "java.lang.String":
				methodPredict = modelClass.getDeclaredMethod(modelMethodName, String.class);
				predictlist = (List<?>) methodPredict.invoke(null, rowString.toString());
				break;

			default:
				break;
			}

			if (predictlist == null) {
				logger.debug("predictlist is null");
				return null;
			}

			Object[] predictor = new Object[row_count + 2];
			for (int i = 0; i <= row_count + 1; i++)
				predictor[i] = null;

			Method newBuilder = prediction.getMethod("newBuilder");
			Object object = newBuilder.invoke(null);
			Method addPrediction;
			// Method addPrediction = object.getClass().getMethod("addPrediction",
			// String.class);

			switch (predictlist.get(0).getClass().getSimpleName()) {
			case "Integer":
			case "Float":
			case "Double":
			case "Boolean":
			case "Long":
			case "Byte":
				addPrediction = object.getClass().getMethod("addAllPrediction", java.lang.Iterable.class);
				addPrediction.invoke(object, predictlist);
				break;

			default:
				addPrediction = object.getClass().getMethod("addPrediction", predictlist.get(0).getClass());
				for (int i = 1; i <= row_count; i++) {
					addPrediction.invoke(object, predictlist.get(i - 1));
					// addPrediction.invoke(object, String.valueOf(predictlist.get(i - 1)));
				}
				break;
			}

			Method build = object.getClass().getMethod("build");

			Object pobj = build.invoke(object);

			Method toByteArray = pobj.getClass().getMethod("toByteArray");

			logger.info("In predict method: Done Prediction, returning binary serialization of prediction. ");
			return (byte[]) (toByteArray.invoke(pobj));

		} catch (Exception ex) {
			logger.error("Failed in doPredictGenericJava: ", ex);
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

				// Below code needs to be removed later added as there
				// overloaded predict method
				// with diff parameter
				if ("net.sf.javaml.core.Dataset".equals(paramType) || paramType.equals("java.io.File"))
					return paramType;
			}

		}
		return paramType;
	}

	/**
	 * This procedure uses H2O model to do prediction
	 * 
	 * @param df
	 * @param modelLoc
	 * @return prediction results in protobuf format
	 */
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
				logger.error("Failed in loading H2O Model: ", ie);
				return null;

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
					case DOUBLE:
						double attrValDouble = ((Double) attrValObj).doubleValue();
						row.put(attr, attrValDouble);
						break;

					case FLOAT:
						float attrValFloat = ((Float) attrValObj).floatValue();
						row.put(attr, attrValFloat);
						break;

					case INT32:
					case UINT32:
					case SINT32:
					case FIXED32:
					case SFIXED32:
						int attrValInt = ((Integer) attrValObj).intValue();
						row.put(attr, attrValInt);
						break;

					case INT64:
					case UINT64:
					case SINT64:
					case FIXED64:
					case SFIXED64:
						long attrValLong = ((Long) attrValObj).longValue();
						row.put(attr, attrValLong);
						break;

					case BOOL:
						boolean attrValBool = ((Boolean) attrValObj).booleanValue();
						row.put(attr, attrValBool);
						break;

					case STRING:
						String attrValStr = (String) attrValObj;
						row.put(attr, attrValStr);
						break;
					case BYTES:
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
						pr = Arrays.toString(autopred);
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
					logger.error("Failed getting prediction results from H2O model:", pe);
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

		} catch (Exception ex) {
			logger.error("Failed in doPredict() ", ex);
			return null;

		}

	}

	/**
	 * This is API end point for enhanced generic model runner
	 * @param dataset : A serialized version of input data in binary data stream
	 * @param operation : this specifies the operation from the service structure in protobuf file
	 * @return : A serialized version of prediction in binary stream
	 */
	@RequestMapping(value = "/operation/{operation}", method = RequestMethod.POST)
	public byte[] operation(@RequestBody byte[] dataset, @PathVariable("operation") String operation) {
		logger.info("/operation/" + operation + " GETTING POST REQUEST:");

		try {
			if (modelType.equalsIgnoreCase("G"))
				loadModelProp();

			init(null);

			if (serviceList.isEmpty() || classList.isEmpty()) {
				logger.error("Wrong protofile format - must specify message and service!");
				return null;
			}

			ServiceObject so = serviceList.get(operation);
			inputClassName = so.getInputClass();
			outputClassName = so.getOutputClass();
			serviceName = operation;

			// dframe = DataFrame.parseFrom(datain);
			Class<?> inputClass = classList.get(inputClassName).getCls();
			Method method = inputClass.getMethod("parseFrom", new Class[] { byte[].class });

			Object df = method.invoke(null, dataset);

			if (!modelType.equalsIgnoreCase("G"))
				return doPredict(df, null);
			else
				return doPredictGeneric(df, null);

		} catch (Exception ex) {
			logger.error("Failed getting prediction results: ", ex);
		}

		return null;
	}

	/**
	 * 
	 * @param dataset
	 *            Data set
	 * @return prediction results in protobuf format
	 */
	@ApiOperation(value = "Gets a prediction binary stream in protobuf format based on the binary stream input also in protobuf format.", consumes = "application/x-protobuf", response = Object.class)
	@RequestMapping(value = "/predict", method = RequestMethod.POST)
	public byte[] predict(@RequestBody byte[] dataset) {
		logger.info("/predict GETTING POST REQUEST:");
		logger.info(Arrays.toString(dataset));

		try {
			init(null);

			// dframe = DataFrame.parseFrom(datain);
			Method method = dataframe.getMethod("parseFrom", new Class[] { byte[].class });

			Object df = method.invoke(null, dataset);

			if (!modelType.equalsIgnoreCase("G"))
				return doPredict(df, null);
			else
				return doPredictJavaGeneric(df, null);

		} catch (Exception ex) {
			logger.error("Failed getting prediction results: ", ex);
		}

		return null;
	}

	@RequestMapping(value = "/model/{methodname}", method = RequestMethod.POST)
	public byte[] operate(@RequestBody byte[] dataset, @PathVariable("methodname") String methodname) {
		logger.info("/model/" + methodname + " GETTING POST REQUEST:");

		try {

			// methodname should match from modelConfig.properties. if provided method does
			// not match it will
			// return with invalid method message
			if (modelType.equalsIgnoreCase("G")) {

				loadModelProp();
				String modelMethodName = prop.getProperty("modelMethod");

				if (!modelMethodName.equalsIgnoreCase(methodname)) {
					logger.info("Expected model method name is =" + modelMethodName);
					return "Model method name is invalid".getBytes();
				}
			}

			init(null);

			// dframe = DataFrame.parseFrom(datain);
			Method method = dataframe.getMethod("parseFrom", new Class[] { byte[].class });

			Object df = method.invoke(null, dataset);

			if (!modelType.equalsIgnoreCase("G"))
				return doPredict(df, null);
			else
				return doPredictJavaGeneric(df, null);

		} catch (Exception ex) {
			logger.error("Failed getting prediction results: ", ex);

		}

		return null;
	}

	/**
	 * This procedure will prepare the plugin directory, generate JAVA protobuf
	 * source and classes.
	 * 
	 * @param protoString
	 * @throws Exception
	 */
	private void init(String protoString) throws Exception {

		logger.info("Project Root is " + PROJECTROOT);
		protoFilePath = new String(PROJECTROOT);
		Random rand = new Random();

		int n = rand.nextInt(100) + 1;

		pluginClassPath = new String(pluginRoot + SEP + "classes");
		protoJarPath = pluginClassPath + SEP + "pbuff" + n + ".jar";
		protoOutputPath = new String(pluginRoot + SEP + "src");

		// purge plugin root directories if already existed
		File dir = new File(pluginRoot);
		if (dir.exists()) {
			boolean deleted = deleteDirectory(dir);
			logger.info("plugin root directory " + pluginRoot + " is deleted? " + deleted);
		}
		// Now (re)create plugin root directories
		dir.mkdirs();
		logger.info("Creating pluginRoot directory: " + pluginRoot);

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

		modelZip = new String(PROJECTROOT + defaultModel);
		defaultProtofile = new String(PROJECTROOT + defaultProto);

		downloadProtoJavaRuntimeJar();
		generateProto(protoString); // Use null for now.

		cl = RunnerController.class.getClassLoader();

		addFile(protoJarPath);

		dataframerow = cl.loadClass(pluginPkgName + ".DatasetProto$DataFrameRow");
		dataframe = cl.loadClass(pluginPkgName + ".DatasetProto$DataFrame");
		prediction = cl.loadClass(pluginPkgName + ".DatasetProto$Prediction");
		dataframeBuilder = cl.loadClass(pluginPkgName + ".DatasetProto$DataFrame$Builder");

		for (String cname : classNames) {
			MessageObject mobj = classList.get(cname);
			if (mobj == null) {
				classList.put(cname, mobj);
			}

			String className = pluginPkgName + ".DatasetProto$" + cname;
			Class<?> thisClass = cl.loadClass(className);
			mobj.setCls(thisClass);
		}
	}

	/**
	 * load modelConfig properties
	 */
	private void loadModelProp() {
		InputStream input;
		try {

			String propFile = new String(PROJECTROOT + modelConfig);
			input = new FileInputStream(propFile);
			prop.load(input);

		} catch (FileNotFoundException e) {
			logger.error("loadModelProp FileNotFoundException: ", e);

		} catch (IOException e) {
			logger.error("loadModelProp IOException: ", e);
		}
	}

	/**
	 * 
	 * @param protoString
	 * @return true of false
	 * @throws Exception
	 */
	private boolean setProtoClasses(String protoString) throws Exception {
		logger.info("Inside setProtoClasses(): \n" + protoString);

		if (protoString == null)
			return false;

		String[] arr = protoString.split("\\s+");

		int total_len = arr.length;
		int idx = 0;
		while (idx < total_len) {
			if (arr[idx].equals("message")) {
				idx = processMessage(arr, idx);
			} else if (arr[idx].equals("rpc")) {
				StringBuilder builder = new StringBuilder(arr[idx++]);
				while (idx < total_len) {
					builder.append(" ").append(arr[idx]);
					if (arr[idx].contains(";")) {
						break;
					}
					idx++;
				}
				processRPCLine(builder.toString());
			} else if (arr[idx].startsWith("java_package")) {
				idx = processPackage(arr, idx);
			}
			idx++;
		}

		logger.info("Plugin package: " + pluginPkgName);
		for (String cname : classNames) {
			logger.info("plugin class = " + cname);
		}

		return true;
	}

	/**
	 * Process line that contains "rpc" to extract service info
	 * 
	 * @param line
	 *            : such as "rpc GetAcl(GetAclRequest) returns (Acl)"
	 */
	private void processRPCLine(String line) {
		logger.info("processRPCLine(): " + line);
		// Create a Pattern object
		String pattern = "rpc (\\w+\\s*)\\((\\s*\\w+\\s*)(\\)\\s*)returns(\\s*\\()(\\s*\\w+\\s*)\\);";
		Pattern r = Pattern.compile(pattern);

		// Now create matcher object.
		Matcher m = r.matcher(line);
		if (m.find()) {
			String service = m.group(1).trim();
			String inputClass = m.group(2).trim();
			String outputClass = m.group(5).trim();
			ServiceObject so = new ServiceObject(service, inputClass, outputClass);
			serviceList.put(service, so);
			logger.info("processRPCLine(): Service: " + service + " inputClass: " + inputClass + " outputClass: "
					+ outputClass);

		} else {
			logger.info("processRPCLine(): NO MATCH");
		}
	}

	/**
	 * Extract message information based on proto file
	 * 
	 * @param arr
	 * @param idx
	 * @return
	 */
	private int processMessage(String[] arr, int idx) {
		logger.info("Inside processMessage(): ");

		String msgname = arr[idx + 1].endsWith("{") ? arr[idx + 1].substring(0, arr[idx + 1].length() - 1)
				: arr[idx + 1];
		logger.info("Adding message: " + msgname);
		classNames.add(msgname);
		for (int i = idx + 1; i < arr.length; i++) {
			if (arr[i].indexOf("}") >= 0) {
				idx = i;
				break;
			}
		}
		return idx;
	}

	/**
	 * Extract package information from proto file
	 * 
	 * @param arr
	 * @param idx
	 * @return
	 */
	private int processPackage(String[] arr, int idx) {
		for (int i = idx; i <= idx + 2; i++) {
			int first = arr[i].indexOf("\"");
			if (first >= 0) {
				int second = arr[i].indexOf("\"", first + 1);
				if (second >= 0) {
					pluginPkgName = arr[i].substring(first + 1, second);
					logger.info("Plugin package name : " + pluginPkgName);
					idx = i;
				}
			}
		}
		return idx;
	}

	/**
	 * @param protoString
	 * @return boolean
	 * @throws Exception
	 */
	private boolean setProtoAttributes(String protoString) throws Exception {
		if (protoString == null)
			return false;

		attributes = new ArrayList<String>();
		attributeTypes = new ArrayList<String>();

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

	/**
	 * @param protoString
	 * @return boolean
	 * @throws Exception
	 */
	private boolean setAllProtoAttributes(String protoString) throws Exception {
		if (protoString == null)
			return false;
		for (String cname : classNames) {
			setMessageProtoAttributes(cname, protoString);
		}
		return true;
	}

	private boolean setMessageProtoAttributes(String cname, String protoString) throws Exception {
		String pattern = "(message\\s+" + cname + "\\s+)";

		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(protoString);
		if (!m.find()) {
			logger.error("Cannot find message " + cname);
			return false;
		}
		String search = m.group(0);
		logger.info("SetMessageProtoAttributes: search pattern = [" + search + "]");

		int idx_msg1 = protoString.indexOf(search);
		int idx_begincurly1 = protoString.indexOf("{", idx_msg1);
		int idx_endcurly1 = protoString.indexOf("}", idx_begincurly1);
		if (idx_msg1 == -1 || idx_begincurly1 == -1 || idx_endcurly1 == -1) {
			logger.error("Wrong proto String format!");
			return false;
		}
		MessageObject mobj = classList.get(cname);
		if (mobj == null) {
			mobj = new MessageObject(cname);
			classList.put(cname, mobj);
		}

		String curMsg = protoString.substring(idx_begincurly1 + 2, idx_endcurly1 - 1);
		StringTokenizer st = new StringTokenizer(curMsg, ";");

		while (st.hasMoreTokens()) {
			boolean isRepeated = false;
			boolean isOptional = false;
			boolean isRequired = false;
			String attribute = null;
			String type = null;

			String line = st.nextToken();
			int idx_equal = line.indexOf("=");
			if (idx_equal == -1) {
				logger.error("Wrong proto string format!");
				return false;
			}
			String subline = line.substring(0, idx_equal);
			String pat = null;
			if(subline.indexOf("repeated") != -1) {
				pat = "(\\s*)repeated(\\s*)(\\w+\\s*)(\\w+\\s*)";
				isRepeated = true;
			}
			else if(subline.indexOf("optional") != -1) {
				pat = "(\\s*)optional(\\s*)(\\w+\\s*)(\\w+\\s*)";
				isOptional = true;
			}
			else if(subline.indexOf("required") != -1) {
				pat = "(\\s*)required(\\s*)(\\w+\\s*)(\\w+\\s*)";
				isRequired = true;
			}
			else 
				pat = "(\\s*)(\\w+\\s*)(\\w+\\s*)";
			
			Pattern r = Pattern.compile(pat);
			Matcher mproto = r.matcher(subline);
			if(mproto.find()) {
				if(!isRepeated && !isOptional && !isRequired) {
					type = mproto.group(2).trim();
					attribute = mproto.group(3).trim();
					logger.info("setMessageProtoAttributes(): type = [" + type + "] attribute = [" + attribute + "]");
				}
				else {
					type = mproto.group(3).trim();
					attribute = mproto.group(4).trim();
					logger.info("setMessageProtoAttributes(): type = [" + type + "] attribute = [" + attribute + "]");
				}
			}
			if (attribute != null && type != null)
				mobj.addAttribute(attribute, type, isRepeated);
		}

		return true;
	}

	private void generateProto(String protoString) throws Exception {
		if (protoString == null)
			protoString = getDefaultProtoString();

		if (protoString == null)
			protoString = generateMockMessage();

		classList.clear();
		classNames.clear();
		serviceList.clear();

		setProtoClasses(protoString);
		setProtoAttributes(protoString);
		setAllProtoAttributes(protoString);
		String protoFilename = protoFilePath + SEP + "dataset.proto";
		writeProtoFile(protoString, protoFilename);
		generateJavaProtoCode();
		buildProtoClasses();
		updateProtoJar();
	}

	private String getDefaultProtoString() throws IOException {
		String result = null;

		defaultProtofile = new String(PROJECTROOT + defaultProto);

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

	private void writeProtoFile(String protoString, String filename) throws IOException {
		BufferedWriter bw = null;
		FileWriter fw = null;

		try {
			fw = new FileWriter(filename);
			bw = new BufferedWriter(fw);
			bw.write(protoString);

			logger.info("Done writing dataset.proto");
		} finally {
			if (bw != null)
				bw.close();

			if (fw != null)
				fw.close();
		}
	}

	/**
	 * Generate JAVA code based on the .proto file
	 */
	private void generateJavaProtoCode() {
		String cmd;
		int exitVal = -1;

		cmd = PROJECTROOT + SEP + "bin" + SEP + "protoc -I=" + protoFilePath + " --java_out=" + protoOutputPath + " "
				+ protoFilePath + SEP + "dataset.proto";

		try {
			exitVal = runCommand(cmd);
			String path = (pluginPkgName == null) ? protoOutputPath
					: protoOutputPath + SEP + pluginPkgName.replaceAll("\\.", SEP);

			if (!pluginPkgName.equals("com.google.protobuf"))
				insertImport(path + SEP + "DatasetProto.java");

		} catch (Exception ex) {
			logger.error("Failed generating Java Protobuf source code: ", ex);
		}

		if (exitVal != 0)
			logger.error("Failed generating DatasetProto.java");
		else
			logger.info("Complete generating DatasetProto.java!");
	}

	/**
	 * 
	 * @param filename
	 *            : Java source file generated by the protocol buffer compiler
	 */
	public void insertImport(String filename) {
		try {
			List<String> old_list = java.nio.file.Files.readAllLines(Paths.get(filename));
			List<String> new_list = new ArrayList<>();

			String import_line = "import com.google.protobuf.*;";
			int index = 0;
			for (String line : old_list) {
				new_list.add(line);
				if (index == 2) {
					new_list.add(import_line);
				}
				if (line.startsWith("package")) {
					new_list.remove(3);
					new_list.add("");
					new_list.add(import_line);
				}
				index++;
			}

			Writer fileWriter = new FileWriter(filename, false); // overwrites file
			for (String out_line : new_list) {
				fileWriter.write(out_line);
				fileWriter.write(System.lineSeparator());
			}
			fileWriter.close();
		} catch (Exception e) {
			logger.error("Failed to process file:" + filename);
		}
	}

	/**
	 * Download Protobuf Java Runtime Jar
	 */
	private void downloadProtoJavaRuntimeJar() {

		try {
			String cmd0 = PROJECTROOT + SEP + "bin" + SEP + "getVersion.sh";
			protoRTVersion = execCommand(cmd0);

			String mavenUrl = "https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/" + protoRTVersion
					+ "/protobuf-java-" + protoRTVersion + ".jar";
			logger.info("mavenurl = " + mavenUrl);
			logger.info("Protobuf Runtime Version is " + protoRTVersion);

			String cmd = "curl -o " + pluginRoot + SEP + "protobuf-java-" + protoRTVersion + ".jar " + mavenUrl;
			logger.info("executing command " + cmd);
			int exitVal = -1;
			exitVal = runCommand(cmd);

			if (exitVal != 0)
				logger.error("Failed downloading Protobuf Runtime Library!");
			else
				logger.info("Completed downloading Protobuf Runtime Library!");

		} catch (Exception ex) {

			logger.error("Failed in downloading the latest protobuf Java runtime library from maven:", ex);
		}
	}

	/**
	 * Generate Java Classes based on .proto file
	 */
	private void buildProtoClasses() {
		String buildPath;
		String protoJavaRuntimeJar;
		String cmd;
		int exitVal = -1;

		// buildPath = protoOutputPath + SEP + "com" + SEP + "google" + SEP + "protobuf"
		// + SEP;
		buildPath = protoOutputPath + SEP + pluginPkgName.replaceAll("\\.", SEP);
		protoJavaRuntimeJar = pluginRoot;
		cmd = "javac -cp " + protoJavaRuntimeJar + SEP + "protobuf-java-" + protoRTVersion + ".jar " + buildPath + SEP
				+ "DatasetProto.java -d " + pluginClassPath;

		try {
			exitVal = runCommand(cmd);

		} catch (Exception ex) {
			logger.error("Failed in buildProtoClasses(): ", ex);
		}
		if (exitVal != 0)
			logger.error("Failed creating proto class files");
		else
			logger.info("Completed creating class files!");
	}

	/**
	 * Include generated proto classes in the Protobuf RUNTIME jar
	 */
	private void updateProtoJar() {
		int exitVal = -1;

		try {
			String protort = pluginRoot + SEP + "protobuf-java-" + protoRTVersion + ".jar";
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
			JarOutputStream target = new JarOutputStream(new FileOutputStream(protoJarPath));
			add(plugin, plugin, target);
			target.close();

		} catch (Exception ex) {

			logger.error("Failed producing/updating pbuff.jar ", ex);
			return;
		}
		if (exitVal != 0)
			logger.error("Failed extracting protobuf JAVA runtime");
		else
			logger.info("Completed producing/updating pbuff.jar!");

	}

	/**
	 * 
	 * @param root
	 * @param source
	 * @param target
	 * @throws IOException
	 */
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

	/**
	 * 
	 * @param cmd
	 *            command to be run with no output
	 * @return error code
	 * @throws Exception
	 */
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

	/**
	 * https://stackoverflow.com/questions/5711084/java-runtime-getruntime-getting-output-from-executing-a-command-line-program
	 * 
	 * @param cmd
	 *            command to be executed
	 * @return result of executing command
	 * @throws IOException
	 *             On failure
	 */
	public String execCommand(String cmd) throws IOException {

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
	 * Deleting a directory recursively :
	 * http://www.baeldung.com/java-delete-directory
	 * 
	 * @param directoryToBeDeleted
	 * @return true or false
	 */
	boolean deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				deleteDirectory(file);
			}
		}
		return directoryToBeDeleted.delete();
	}

	/**
	 * Adds the content pointed by the URL to the classpath.
	 * 
	 * @param u
	 *            the URL pointing to the content to be added
	 * @throws IOException
	 *             On failure
	 */
	public static void addURL(URL u) throws IOException {
		URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		Class<?> sysclass = URLClassLoader.class;
		try {
			Method method = sysclass.getDeclaredMethod("addURL", parameters);
			method.setAccessible(true);
			method.invoke(sysloader, new Object[] { u });
		} catch (Throwable t) {
			logger.error("Failed in addURL(): ", t);
			throw new IOException("Error, could not add URL to system classloader");
		}
	}

	/**
	 * Adds a file to the classpath.
	 * 
	 * @param s
	 *            a String pointing to the file
	 * @throws IOException
	 *             On failure
	 */
	public static void addFile(String s) throws IOException {
		File f = new File(s);
		addFile(f);
	}

	/**
	 * Adds a file to the classpath
	 * 
	 * @param f
	 *            the file to be added
	 * @throws IOException
	 *             On failure
	 */
	public static void addFile(File f) throws IOException {
		addURL(f.toURI().toURL());
	}

}
