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
import com.google.protobuf.ByteString;

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

	private HashMap<String, MessageObject> classList = new HashMap<>();
	private HashMap<String, ServiceObject> serviceList = new HashMap<>();
	private ArrayList<String> classNames = new ArrayList<>();
	private Properties prop = new Properties();
	private String protoRTVersion = null;
	private ClassLoader cl = null;
	private int protoIdx = 0;

	/**
	 * End point for a modeler to upload a model
	 * 
	 * @param model
	 *            Uploaded model
	 * @return ResponseEntity
	 */
	@ApiOperation(value = "Upload a machine learning model to replace the current model", response = Map.class)
	@RequestMapping(value = "/putModel", method = RequestMethod.PUT)
	public ResponseEntity<Map<String, String>> putModel(@RequestPart("model") MultipartFile model) {
		logger.info("Receiving /putModel PUT request...");
		Map<String, String> results = new LinkedHashMap<>();

		try {
			if (model != null && !model.isEmpty()) {
				byte[] bytes = model.getBytes();

				// Create the file on server
				File modelFile = new File(PROJECTROOT + defaultModel);
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
	@ApiOperation(value = "Upload a protofile to replace the current default protofile", response = ResponseEntity.class)
	@RequestMapping(value = "/putProto", method = RequestMethod.PUT)
	public ResponseEntity<Map<String, String>> putProto(@RequestPart("proto") MultipartFile proto) {
		logger.info("Receiving /putProto PUT request...");
		Map<String, String> results = new LinkedHashMap<>();

		try {
			if (proto != null && !proto.isEmpty()) {
				byte[] bytes = proto.getBytes();

				// Create the file on server
				File protoFile = new File(PROJECTROOT + defaultProto);
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
	 * End point to upload a new model configuration properties file
	 * 
	 * @param configFile
	 *            modelConfig.properties file
	 * @return ResponseEntity
	 */
	@ApiOperation(value = "Upload a model config file to replace current model configuration properties used by Generic Model Runner. H2O Model Runner does not use this file", response = ResponseEntity.class)
	@RequestMapping(value = "/putModelConfig", method = RequestMethod.PUT)
	public ResponseEntity<Map<String, String>> putModelConfig(@RequestPart("modelConfig") MultipartFile configFile) {
		logger.info("Receiving /putModelConfig PUT request...");
		Map<String, String> results = new LinkedHashMap<>();

		try {
			if (configFile != null && !configFile.isEmpty()) {
				byte[] bytes = configFile.getBytes();

				// Create the file on server
				File configPropertiesFile = new File(PROJECTROOT + modelConfig);
				BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(configPropertiesFile));
				stream.write(bytes);
				stream.close();

				logger.info("Model config properties file location=" + configPropertiesFile.getAbsolutePath());
			}
		} catch (Exception ex) {
			logger.error("Failed in uploading configfile: ", ex);
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
	@ApiOperation(value = "Converts the csv file to a binary stream in protobuf format using default.proto. The header is required in the csv file. The header fields must match with ones in the proto file")
	@RequestMapping(value = "/getBinaryDefault", method = RequestMethod.POST, produces = "application/octet-stream")
	public byte[] getBinaryDefault(@RequestPart("csvFile") MultipartFile csvFile, String operation) {
		logger.info("Receiving /getBinaryDefault POST request...");
		return getNewBinary_(csvFile, null, operation);
	}

	/**
	 * 
	 * @param csvFile
	 *            CSV file to be serialized
	 * @param proto
	 *            Protobuf file
	 * @param operation
	 *            one of the operations matching service structure in protofile
	 * @return
	 */
	@ApiOperation(value = "Serialize the csv file based on the .proto file provided here. This .proto file will not replace the default protofile ")
	@RequestMapping(value = "/getBinary", method = RequestMethod.POST, produces = MediaType.APPLICATION_OCTET_STREAM)
	public byte[] getBinary(@RequestPart("csvFile") MultipartFile csvFile, @RequestPart("proto") MultipartFile proto,
			String operation) {
		logger.info("Receiving /getBinary POST request...");

		return getNewBinary_(csvFile, proto, operation);
	}

	/**
	 * Serialize the csv file based on the proto file into binary protobuf format
	 * 
	 * @param file
	 *            csv file containing headers
	 * @param proto
	 *            proto file
	 * @param operation
	 *            name of operation specified in the service structure of the proto
	 *            file
	 * @return
	 */
	private byte[] getNewBinary_(MultipartFile file, MultipartFile proto, String operation) {
		try {
			Object df = getInputClassBuilder(file, proto, operation); // df is of {InputClass}.Builder type
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
	@ApiOperation(value = "Gets a prediction binary stream in protobuf format for the training data in the provided csv file using default .proto file")
	@RequestMapping(value = "/transformDefault", method = RequestMethod.POST)
	public byte[] transform(@RequestPart("csvFile") MultipartFile csvFile, String operation) {
		logger.info("Receiving /transformDefault POST Request...");
		return transform_(csvFile, null, null, operation);
	}

	/**
	 * @param csvFile
	 *            CSV File
	 * @param model
	 *            an ML model
	 * @param proto
	 *            Protobuf file
	 * @return prediction binary stream in protobuf format
	 */
	@ApiOperation(value = "Gets a prediction binary stream in protobuf format for the training data in the provided csv file using the ML model and .proto file provided here")
	@RequestMapping(value = "/transform", method = RequestMethod.POST)
	public byte[] transform(@RequestPart("csvFile") MultipartFile csvFile, @RequestPart("model") MultipartFile model,
			@RequestPart("proto") MultipartFile proto, String operation) {
		logger.info("Receiving /transform POST Request...");
		return transform_(csvFile, model, proto, operation);
	}

	/**
	 * 
	 * @param file
	 *            Data
	 * @param model
	 *            Model
	 * @param proto
	 *            Protofile
	 * @param operation
	 *            one of the operation in the service structure of the protofile
	 * @return prediction binary stream in protobuf format
	 */
	private byte[] transform_(MultipartFile file, MultipartFile model, MultipartFile proto, String operation) {
		try {
			Object df = getInputClassBuilder(file, proto, operation); // df is of {InputClass}.Builder

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

			if (serviceList.isEmpty() || classList.isEmpty()) {
				logger.error("Wrong protofile format - must specify message and service!");
				return null;
			}

			outputClassName = serviceList.get(operation).getOutputClass();

			if (!modelType.equalsIgnoreCase("G"))
				return doPredictH2O(obj, modelLoc);
			else
				return doPredictGeneric(obj, modelLoc);
		} catch (Exception ex) {
			logger.error("transform_(): Failed transforming csv file and getting prediction results: ", ex);

		}

		return new byte[0];
	}

	/**
	 * Get {InputClass}.Builder class based on uploaded data file and proto file
	 * 
	 * @param file
	 * @param proto
	 * @return {InputClass}.Builder
	 * @throws Exception
	 */

	private Object getInputClassBuilder(MultipartFile file, MultipartFile proto, String operation) throws Exception {
		if (file.isEmpty()) {
			logger.error("You failed to upload " + file.getOriginalFilename() + " because the file was empty.");
			return null;
		}

		Object inputBuilder = null;

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

		ServiceObject so = serviceList.get(operation);
		inputClassName = so.getInputClass();

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

		inputBuilder = getInputBuilder(inputClassName, lines);

		return inputBuilder;
	}

	private Object getInputBuilder(String thisClassName, String[] lines) {
		try {
			Object thisBuilder = null;
			Class<?> thisClass = classList.get(thisClassName).getCls();
			Method thisBuilderMethod = thisClass.getMethod("newBuilder");
			thisBuilder = thisBuilderMethod.invoke(null); // thisBuilder is of {thisClass}.Builder

			MessageObject thisMsg = classList.get(thisClassName);
			ArrayList<AttributeEntity> inputAttributes = thisMsg.getAttributes();
			Method inputAddOrSetRow = null;

			String headerLine = lines[0];
			String[] headerFields = headerLine.split(",");
			logger.info("getInputBuilder(): Header Line is: [" + headerLine + "]");

			String[] array;

			int idx;

			for (AttributeEntity ae : inputAttributes) {
				for (int i = 1; i < lines.length; i++) { // ignore the first line which is header
					String line = lines[i];
					logger.info("getInputBuilder(): current line is: [" + line + "]");

					array = line.split(",");

					if (ae.isRepeated()) {
						String iAttrMethodName = StringUtils.camelCase("add_" + ae.getName(), '_');
						switch (ae.getType()) {
						case DOUBLE:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, double.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;
								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, Double.parseDouble(array[idx]));
							}
							break;
						case FLOAT:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, float.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;
								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, Float.parseFloat(array[idx]));
							}
							break;
						case INT32:
						case UINT32:
						case SINT32:
						case FIXED32:
						case SFIXED32:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, int.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;
								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, Integer.parseInt(array[idx]));
							}
							break;
						case INT64:
						case UINT64:
						case SINT64:
						case FIXED64:
						case SFIXED64:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, long.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;
								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, Long.parseLong(array[idx]));
							}
							break;
						case BOOL:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, boolean.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;
								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, Boolean.parseBoolean(array[idx]));
							}
							break;
						case STRING:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, String.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, array[idx]);
							}
							break;
						case BYTES:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, ByteString.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									ByteString byteStr = ByteString.copyFrom(array[idx].getBytes());
									inputAddOrSetRow.invoke(thisBuilder, byteStr);
								}
							}
							break;
						default:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName,
									classList.get(ae.getType()).getCls());

							Object innerBuilder = getInnerBuilder(ae.getType(), line, headerLine);
							Method innerBuildMethod;
							if (innerBuilder != null) {
								innerBuildMethod = innerBuilder.getClass().getMethod("build");
								Object innerObj = innerBuildMethod.invoke(innerBuilder);
								inputAddOrSetRow.invoke(thisBuilder, innerObj);
							}
							break;
						}

					} else {
						String iAttrMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');

						switch (ae.getType()) {
						case DOUBLE:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, double.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									inputAddOrSetRow.invoke(thisBuilder, Double.parseDouble(array[idx]));
									break;
								}
							}

							break;
						case FLOAT:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, float.class);

							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									inputAddOrSetRow.invoke(thisBuilder, Float.parseFloat(array[idx]));
									break;
								}
							}
							break;
						case INT32:
						case UINT32:
						case SINT32:
						case FIXED32:
						case SFIXED32:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, int.class);

							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									inputAddOrSetRow.invoke(thisBuilder, Integer.parseInt(array[idx]));
									break;
								}
							}
							break;
						case INT64:
						case UINT64:
						case SINT64:
						case FIXED64:
						case SFIXED64:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, long.class);

							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									inputAddOrSetRow.invoke(thisBuilder, Long.parseLong(array[idx]));
									break;
								}
							}
							break;
						case BOOL:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, boolean.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									inputAddOrSetRow.invoke(thisBuilder, Boolean.parseBoolean(array[idx]));
									break;
								}
							}
							break;
						case STRING:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, String.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									inputAddOrSetRow.invoke(thisBuilder, array[idx]);
									break;
								}
							}
							break;
						case BYTES:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, ByteString.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									ByteString byteStr = ByteString.copyFrom(array[idx].getBytes());

									inputAddOrSetRow.invoke(thisBuilder, byteStr);
									break;
								}
							}
							break;
						default:
							Class<?> innerCls = classList.get(ae.getType()).getCls();
							Object innerBuilder = getInnerBuilder(ae.getType(), line, headerLine);

							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, innerCls);
							Method innerBuildMethod;
							if (innerBuilder != null) {
								innerBuildMethod = innerBuilder.getClass().getMethod("build");
								Object innerObj = innerBuildMethod.invoke(innerBuilder);
								inputAddOrSetRow.invoke(thisBuilder, innerObj);
							}
							break;

						}
					}
				}
			}
			return thisBuilder;
		} catch (Exception ex) {
			logger.error("Failed in getInputBuilder(): ", ex);
			return null;
		}
	}

	/**
	 * 
	 * @param thisClassName
	 * @param line
	 * @param headerLine
	 * @return return a
	 */
	private Object getInnerBuilder(String thisClassName, String line, String headerLine) {
		try {
			logger.info("getInnerBuilder(): current line is [" + line + "] header is [" + headerLine + "]");
			Object thisBuilder = null;
			Class<?> thisClass = classList.get(thisClassName).getCls();
			Method thisBuilderMethod = thisClass.getMethod("newBuilder");
			thisBuilder = thisBuilderMethod.invoke(null); // thisBuilder is of {thisClass}.Builder

			MessageObject thisMsg = classList.get(thisClassName);
			ArrayList<AttributeEntity> inputAttributes = thisMsg.getAttributes();
			Method inputAddOrSetRow = null;

			String[] headerFields = headerLine.split(",");
			String[] array = line.split(",");

			int idx;

			for (AttributeEntity ae : inputAttributes) {
				if (ae.isRepeated()) {
					String iAttrMethodName = StringUtils.camelCase("add_" + ae.getName(), '_');
					switch (ae.getType()) {
					case DOUBLE:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, double.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;
							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, Double.parseDouble(array[idx]));
						}
						break;
					case FLOAT:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, float.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;
							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, Float.parseFloat(array[idx]));
						}
						break;
					case INT32:
					case UINT32:
					case SINT32:
					case FIXED32:
					case SFIXED32:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, int.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;
							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, Integer.parseInt(array[idx]));
						}
						break;
					case INT64:
					case UINT64:
					case SINT64:
					case FIXED64:
					case SFIXED64:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, long.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;
							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, Long.parseLong(array[idx]));
						}
						break;
					case BOOL:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, boolean.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;
							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, Boolean.parseBoolean(array[idx]));
						}
						break;
					case STRING:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, String.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, array[idx]);
						}
						break;
					case BYTES:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, ByteString.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName())) {
								ByteString byteStr = ByteString.copyFrom(array[idx].getBytes());
								inputAddOrSetRow.invoke(thisBuilder, byteStr);
							}
						}
						break;
					default:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName,
								classList.get(ae.getType()).getCls());

						Object innerBuilder = getInnerBuilder(ae.getType(), line, headerLine);
						Method innerBuildMethod;
						if (innerBuilder != null) {
							innerBuildMethod = innerBuilder.getClass().getMethod("build");
							Object innerObj = innerBuildMethod.invoke(innerBuilder);
							inputAddOrSetRow.invoke(thisBuilder, innerObj);
						}
						break;
					}

				} else {
					String iAttrMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');

					switch (ae.getType()) {
					case DOUBLE:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, double.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName())) {
								inputAddOrSetRow.invoke(thisBuilder, Double.parseDouble(array[idx]));
								break;
							}
						}

						break;
					case FLOAT:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, float.class);

						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName())) {
								inputAddOrSetRow.invoke(thisBuilder, Float.parseFloat(array[idx]));
								break;
							}
						}
						break;
					case INT32:
					case UINT32:
					case SINT32:
					case FIXED32:
					case SFIXED32:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, int.class);

						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName())) {
								inputAddOrSetRow.invoke(thisBuilder, Integer.parseInt(array[idx]));
								break;
							}
						}
						break;
					case INT64:
					case UINT64:
					case SINT64:
					case FIXED64:
					case SFIXED64:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, long.class);

						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName())) {
								inputAddOrSetRow.invoke(thisBuilder, Long.parseLong(array[idx]));
								break;
							}
						}
						break;
					case BOOL:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, boolean.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName())) {
								inputAddOrSetRow.invoke(thisBuilder, Boolean.parseBoolean(array[idx]));
								break;
							}
						}
						break;
					case STRING:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, String.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName())) {
								inputAddOrSetRow.invoke(thisBuilder, array[idx]);
								break;
							}
						}
						break;
					case BYTES:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, ByteString.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName())) {
								ByteString byteStr = ByteString.copyFrom(array[idx].getBytes());

								inputAddOrSetRow.invoke(thisBuilder, byteStr);
								break;
							}
						}
						break;
					default:
						Class<?> innerCls = classList.get(ae.getType()).getCls();
						Object innerBuilder = getInnerBuilder(ae.getType(), line, headerLine);

						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, innerCls);
						Method innerBuildMethod;
						if (innerBuilder != null) {
							innerBuildMethod = innerBuilder.getClass().getMethod("build");
							Object innerObj = innerBuildMethod.invoke(innerBuilder);
							inputAddOrSetRow.invoke(thisBuilder, innerObj);
						}
						break;

					}
				}

			}
			return thisBuilder;
		} catch (Exception ex) {
			logger.error("Failed in getInnerBuilder(): ", ex);
			return null;
		}
	}

	/*
	 * in the case of nestedmsg.proto First iteration : parent - DataFrame, child -
	 * DataFrameRow Second iteration: parent - DataFrameRow, child - SubFrameRow
	 */
	private void getRowString(Object df, String parentName, String attributeName, StringBuffer rowStr) {
		try {
			logger.info("getRowString(): " + parentName);
			MessageObject parentMsg = classList.get(parentName);
			Class<?> parentCls = parentMsg.getCls();
			ArrayList<AttributeEntity> parentAttributes = parentMsg.getAttributes();

			for (AttributeEntity ae : parentAttributes) {
				if (attributeName != null && !attributeName.equals(ae.getName()))
					continue;

				if (ae.isRepeated()) {
					String pAttrMethodName = StringUtils.camelCase("get_" + ae.getName(), '_');
					Method getCount = parentCls.getMethod(pAttrMethodName + "Count");
					int rowCount = (int) getCount.invoke(df);
					logger.info("We have: " + rowCount + " row(s) of " + ae.getName());

					Method getList = parentCls.getMethod(pAttrMethodName + "List");
					List<?> list = (List<?>) getList.invoke(df); // list of child objects or list of primitive types

					Object obj;
					for (int j = 0; j < rowCount; j++) {
						obj = list.get(j);

						switch (ae.getType()) {
						case DOUBLE:
							double attrValDouble = ((Double) obj).doubleValue();
							if (rowStr.length() != 0)
								rowStr.append(",");
							rowStr.append(attrValDouble);
							break;

						case FLOAT:
							float attrValFloat = ((Float) obj).floatValue();
							if (rowStr.length() != 0)
								rowStr.append(",");
							rowStr.append(attrValFloat);
							break;

						case INT32:
						case UINT32:
						case SINT32:
						case FIXED32:
						case SFIXED32:
							int attrValInt = ((Integer) obj).intValue();
							if (rowStr.length() != 0)
								rowStr.append(",");
							rowStr.append(attrValInt);
							break;

						case INT64:
						case UINT64:
						case SINT64:
						case FIXED64:
						case SFIXED64:
							long attrValLong = ((Long) obj).longValue();
							if (rowStr.length() != 0)
								rowStr.append(",");
							rowStr.append(attrValLong);
							break;

						case BOOL:
							boolean attrValBool = ((Boolean) obj).booleanValue();
							if (rowStr.length() != 0)
								rowStr.append(",");
							rowStr.append(attrValBool);
							break;

						case STRING:
							String attrValStr = (String) obj;
							if (rowStr.length() != 0)
								rowStr.append(",");
							rowStr.append(attrValStr);
							break;

						case BYTES:
							byte[] attrValByte = ((ByteString) obj).toByteArray();
							if (rowStr.length() != 0)
								rowStr.append(",");
							rowStr.append(attrValByte);
							break;

						default:
							getRowString(obj, ae.getType(), null, rowStr);

							break;
						}
					}
				} else {
					String pAttrMethodName = StringUtils.camelCase("get_" + ae.getName(), '_');
					Method pAttrMethod = parentCls.getMethod(pAttrMethodName);
					Object obj = pAttrMethod.invoke(df);

					switch (ae.getType()) {
					case DOUBLE:
						double gcValDouble = ((Double) obj).doubleValue();
						if (rowStr.length() != 0)
							rowStr.append(",");
						rowStr.append(gcValDouble);
						break;

					case FLOAT:
						float gcValFloat = ((Float) obj).floatValue();
						if (rowStr.length() != 0)
							rowStr.append(",");
						rowStr.append(gcValFloat);
						break;

					case INT32:
					case UINT32:
					case SINT32:
					case FIXED32:
					case SFIXED32:
						int gcValInt = ((Integer) obj).intValue();
						if (rowStr.length() != 0)
							rowStr.append(",");
						rowStr.append(gcValInt);
						break;

					case INT64:
					case UINT64:
					case SINT64:
					case FIXED64:
					case SFIXED64:
						long gcValLong = ((Long) obj).longValue();
						if (rowStr.length() != 0)
							rowStr.append(",");
						rowStr.append(gcValLong);
						break;

					case BOOL:
						boolean gcValBool = ((Boolean) obj).booleanValue();
						if (rowStr.length() != 0)
							rowStr.append(",");
						rowStr.append(gcValBool);
						break;

					case STRING:
						String gcValStr = (String) obj;
						if (rowStr.length() != 0)
							rowStr.append(",");
						rowStr.append(gcValStr);
						break;
					case BYTES:
						byte[] gcValByte = ((ByteString) obj).toByteArray();
						if (rowStr.length() != 0)
							rowStr.append(",");
						rowStr.append(gcValByte);
						break;
					default: // TODO
						getRowString(obj, ae.getType(), null, rowStr);
						break;
					}
				}
				// rowStr.append("\n");

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

			getRowString(df, inputClassName, null, rowString);
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
			List<?> predictList = null;

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
				predictList = (List<?>) methodPredict.invoke(null, file);
				break;

			case "java.lang.String":
				methodPredict = modelClass.getDeclaredMethod(modelMethodName, String.class);
				predictList = (List<?>) methodPredict.invoke(null, rowString.toString());
				break;

			default:
				break;
			}

			if (predictList == null) {
				logger.debug("predictlist is null");
				return null;
			}
			int row_count = predictList.size();
			Object[] predictor = new Object[row_count + 2];
			for (int i = 0; i <= row_count + 1; i++)
				predictor[i] = null;
			Object pobj = getPredictionRow(outputClassName, predictList);

			Method toByteArray = pobj.getClass().getMethod("toByteArray");
			byte[] results = (byte[]) (toByteArray.invoke(pobj));
			logger.info("In doPredictGeneric() : Prediction results - " + results.toString());
			logger.info(
					"In doPredictGeneric() : Done Prediction, returning binary serialization of above prediction results - "
							+ Arrays.toString(results));

			return results;

		} catch (Exception ex) {
			logger.error("Failed in doPredictGeneric: ", ex);
			return null;
		}

	}

	/**
	 * 
	 * @param predictionClassName
	 *            output message name from proto file. Could be nested inner message
	 *            name.
	 * @param predictList
	 *            this list contains un-serialized prediction data
	 * @return object containing serialized prediction in protobuf format
	 */
	private Object getPredictionRow(String predictionClassName, List<?> predictList) {
		try {

			List<?> copy = new ArrayList<>(predictList);
			Class<?> predictionCls = classList.get(predictionClassName).getCls();
			Method newBuilder = predictionCls.getMethod("newBuilder");
			Object object = newBuilder.invoke(null);
			Method addPrediction;
			boolean started = false, predictionAdded = false;
			String predictMethodName = null;

			ArrayList<AttributeEntity> outputAttributes = classList.get(predictionClassName).getAttributes();
			for (AttributeEntity ae : outputAttributes) {
				predictMethodName = null;
				started = false;

				switch (ae.getType()) {
				case INT32:
				case UINT32:
				case SINT32:
				case FIXED32:
				case SFIXED32:
					// addPrediction = object.getClass().getMethod("addAllPrediction",
					// java.lang.Iterable.class);
					if (ae.isRepeated()) {
						predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);

						List<Integer> intList = new ArrayList<>();
						for (Object obj : predictList) {
							if (obj instanceof Integer) {
								if (!started)
									started = true;
								intList.add((Integer) obj);
								predictionAdded = true;
							} else {
								if (started)
									break;
							}
						}

						if (!intList.isEmpty()) {
							addPrediction.invoke(object, intList);
							predictList.removeAll(intList);
						}
					} else {
						predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, int.class);

						for (Object obj : predictList) {
							if (obj instanceof Integer) {
								addPrediction.invoke(object, (Integer) obj);
								predictionAdded = true;
								predictList.remove(obj);
								break;
							}
						}
					}
					break;

				case INT64:
				case UINT64:
				case SINT64:
				case FIXED64:
				case SFIXED64:
					if (ae.isRepeated()) {
						predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);

						List<Long> longList = new ArrayList<>();
						for (Object obj : predictList) {
							if (obj instanceof Long) {
								if (!started)
									started = true;
								longList.add((Long) obj);
								predictionAdded = true;
							} else {
								if (started)
									break;
							}
						}
						if (!longList.isEmpty()) {
							addPrediction.invoke(object, longList);
							predictList.removeAll(longList);
						}
					} else {
						predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, long.class);

						for (Object obj : predictList) {
							if (obj instanceof Long) {
								addPrediction.invoke(object, (Long) obj);
								predictionAdded = true;
								predictList.remove(obj);
								break;
							}
						}
					}
					break;

				case FLOAT:
					if (ae.isRepeated()) {
						predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);

						// List<Float> floatList = predictList.stream().map(obj -> (Float)
						// obj).collect(Collectors.toList());

						List<Float> floatList = new ArrayList<>();
						for (Object obj : predictList) {
							if (obj instanceof Float) {
								if (!started)
									started = true;
								floatList.add((Float) obj);
								predictionAdded = true;
							} else {
								if (started)
									break;
							}
						}
						if (!floatList.isEmpty()) {
							addPrediction.invoke(object, floatList);
							predictList.removeAll(floatList);
						}
					} else {
						predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, float.class);

						for (Object obj : predictList) {
							if (obj instanceof Float) {
								addPrediction.invoke(object, (Float) obj);
								predictionAdded = true;
								predictList.remove(obj);
								break;
							}
						}
					}
					break;
				case DOUBLE:
					if (ae.isRepeated()) {
						predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);

						List<Double> doubleList = new ArrayList<>();
						for (Object obj : predictList) {
							if (obj instanceof Double) {
								if (!started)
									started = true;

								doubleList.add((Double) obj);
								predictionAdded = true;
							} else {
								if (started)
									break;
							}
						}
						if (!doubleList.isEmpty()) {
							addPrediction.invoke(object, doubleList);
							predictList.removeAll(doubleList);
						}
					} else {
						predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, double.class);

						for (Object obj : predictList) {
							if (obj instanceof Double) {
								addPrediction.invoke(object, (Double) obj);
								predictionAdded = true;
								predictList.remove(obj);
								break;
							}
						}
					}
					break;

				case BOOL:
					if (ae.isRepeated()) {
						predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);

						// List<Boolean> boolList = predictList.stream().map(obj -> (Boolean)
						// obj).collect(Collectors.toList());
						List<Boolean> boolList = new ArrayList<>();
						for (Object obj : predictList) {
							if (obj instanceof Boolean) {
								if (!started)
									started = true;
								boolList.add((Boolean) obj);
								predictionAdded = true;

							} else {
								if (started)
									break;
							}
						}
						if (!boolList.isEmpty()) {
							addPrediction.invoke(object, boolList);
							predictList.removeAll(boolList);
						}
					} else {
						predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, boolean.class);

						for (Object obj : predictList) {
							if (obj instanceof Boolean) {
								addPrediction.invoke(object, (Boolean) obj);
								predictionAdded = true;
								predictList.remove(obj);
								break;
							}
						}
					}
					break;
				case BYTES:
					if (ae.isRepeated()) {
						predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);

						// List<ByteString> byteList = predictList.stream().map(obj -> (ByteString)
						// obj).collect(Collectors.toList());
						List<ByteString> byteList = new ArrayList<>();
						List<Object> byteArrayList = new ArrayList<>();
						for (Object obj : predictList) {
							logger.info("Object is " + obj.getClass().getName());
							if (obj.getClass().getName().equals("[B")) {
								if (!started)
									started = true;
								ByteString byteStr = ByteString.copyFrom((byte[]) obj);
								byteList.add(byteStr);
								byteArrayList.add(obj);
								predictionAdded = true;

							} else {
								if (started)
									break;
							}
						}
						if (!byteList.isEmpty()) {
							addPrediction.invoke(object, byteList);
							predictList.removeAll(byteArrayList);
						}
					} else {
						predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName,
								com.google.protobuf.ByteString.class);

						for (Object obj : predictList) {
							if (obj.getClass().getName().equals("[B")) {
								ByteString byteStr = ByteString.copyFrom((byte[]) obj);
								addPrediction.invoke(object, byteStr);
								predictionAdded = true;
								predictList.remove(obj);
								break;
							}
						}
					}
					break;

				case STRING:
					if (ae.isRepeated()) {
						predictMethodName = StringUtils.camelCase("add_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, String.class);
						List<String> found = new ArrayList<>();
						for (int i = 1; i <= predictList.size(); i++) {
							if (predictList.get(i - 1) instanceof String) {
								if (!started)
									started = true;
								addPrediction.invoke(object, (String) predictList.get(i - 1));
								found.add((String) predictList.get(i - 1));
								predictionAdded = true;
							} else {
								if (started)
									break;
							}
							// addPrediction.invoke(object, String.valueOf(predictlist.get(i - 1)));
						}
						if (!found.isEmpty())
							predictList.removeAll(found);
					} else {
						predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, String.class);
						for (Object obj : predictList) {
							if (obj instanceof String) {
								addPrediction.invoke(object, (String) obj);
								predictionAdded = true;
								predictList.remove(obj);
								break;
							}
						}
					}
					break;

				default:
					if (ae.isRepeated()) {
						predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);

						List msgList = new ArrayList();
						while (!predictList.isEmpty()) {
							Object innerObj = getPredictionRow(ae.getType(), predictList);
							if (innerObj == null)
								break;
							msgList.add(innerObj);
							predictionAdded = true;
						}

						if (!msgList.isEmpty())
							addPrediction.invoke(object, msgList);
					} else { // TODO
						predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
						addPrediction = object.getClass().getMethod(predictMethodName,
								classList.get(ae.getType()).getCls());
						Object innerObj = getPredictionRow(ae.getType(), predictList);
						if (innerObj != null) {
							addPrediction.invoke(object, innerObj);
							predictionAdded = true;
						}
					}
					break;
				}
			}
			// None of the data matches
			if (!predictionAdded)
				return null;

			Method build = object.getClass().getMethod("build");
			Object pobj = build.invoke(object);
			return pobj;

		} catch (Exception ex) {
			logger.error("Failed in getPredictionRow() : ", ex);
			return null;

		}
	}

	/**
	 * Decide input format of the model
	 * 
	 * @param modelClass
	 * @param modelMethodName
	 * @return
	 */
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
	 * Populates rows for input of H2O model
	 * 
	 * @param df
	 *            builder class that contains the serialized data
	 * @param outerClassName
	 * @param rows
	 *            rows to be populated
	 */
	private void getH2ORowData(Object df, String outerClassName, List<RowData> rows) {
		try {
			logger.info("getH2ORowData() : " + outputClassName);
			RowData row = new RowData();
			MessageObject outerMsg = classList.get(outerClassName);
			Class<?> outerCls = outerMsg.getCls();
			ArrayList<AttributeEntity> outerAttributes = outerMsg.getAttributes();

			String innerClassName = null;
			MessageObject innerMsg = null;
			Class<?> innerCls = null;
			ArrayList<AttributeEntity> innerAttributes = null;

			for (AttributeEntity oae : outerAttributes) {
				if (oae.isRepeated()) {
					String oAttrMethodName = StringUtils.camelCase("get_" + oae.getName(), '_');
					Method getCount = outerCls.getMethod(oAttrMethodName + "Count");
					int rowCount = (int) getCount.invoke(df);
					logger.info("We have: " + rowCount + " row(s) of " + oae.getName());
					Method getList = outerCls.getMethod(oAttrMethodName + "List");
					List<?> list = (List<?>) getList.invoke(df); // list of inner objects or list of primitive types

					Object obj;
					for (int j = 0; j < rowCount; j++) {
						obj = list.get(j);

						switch (oae.getType()) {
						case DOUBLE:
							double attrValDouble = ((Double) obj).doubleValue();
							row.put(oae.getName(), attrValDouble);
							break;

						case FLOAT:
							float attrValFloat = ((Float) obj).floatValue();
							row.put(oae.getName(), attrValFloat);
							break;

						case INT32:
						case UINT32:
						case SINT32:
						case FIXED32:
						case SFIXED32:
							int attrValInt = ((Integer) obj).intValue();
							row.put(oae.getName(), attrValInt);
							break;

						case INT64:
						case UINT64:
						case SINT64:
						case FIXED64:
						case SFIXED64:
							long attrValLong = ((Long) obj).longValue();
							row.put(oae.getName(), attrValLong);
							break;

						case BOOL:
							boolean attrValBool = ((Boolean) obj).booleanValue();
							row.put(oae.getName(), attrValBool);
							break;

						case STRING:
							String attrValStr = (String) obj;
							row.put(oae.getName(), attrValStr);
							break;
						case BYTES:
							byte attrValByte = ((Byte) obj).byteValue();
							row.put(oae.getName(), attrValByte);
							break;
						default:
							innerClassName = oae.getType();
							innerMsg = classList.get(innerClassName);
							innerCls = innerMsg.getCls();
							innerAttributes = innerMsg.getAttributes();
							for (AttributeEntity iae : innerAttributes) {
								String iAttrMethodName = StringUtils.camelCase("get_" + iae.getName(), '_');
								Method iAttrMethod = innerCls.getMethod(iAttrMethodName);
								Object iobj = iAttrMethod.invoke(obj);
								switch (iae.getType()) {
								case DOUBLE:
									double iValDouble = ((Double) iobj).doubleValue();
									row.put(iae.getName(), iValDouble);
									break;
								case FLOAT:
									break;
								case INT32:
								case UINT32:
								case SINT32:
								case FIXED32:
								case SFIXED32:
									int iValInt = ((Integer) iobj).intValue();
									row.put(iae.getName(), iValInt);
									break;
								case INT64:
								case UINT64:
								case SINT64:
								case FIXED64:
								case SFIXED64:
									long iValLong = ((Long) iobj).longValue();
									row.put(iae.getName(), iValLong);
									break;
								case BOOL:
									boolean iValBool = ((Boolean) iobj).booleanValue();
									row.put(iae.getName(), iValBool);
									break;
								case STRING:
									String iValStr = (String) iobj;
									row.put(iae.getName(), iValStr);
									break;

								case BYTES:
									byte[] iValByte = ((ByteString) iobj).toByteArray();
									row.put(iae.getName(), iValByte);
									break;

								default:
									getH2ORowData(iobj, iae.getType(), rows);

									break;
								}
							}
							break;
						}
					}
				} else {
					String oAttrMethodName = StringUtils.camelCase("get_" + oae.getName(), '_');
					Method oAttrMethod = outerCls.getMethod(oAttrMethodName);
					Object obj = oAttrMethod.invoke(df);

					switch (oae.getType()) {
					case DOUBLE:
						double attrValDouble = ((Double) obj).doubleValue();
						row.put(oae.getName(), attrValDouble);
						break;

					case FLOAT:
					case INT32:
					case UINT32:
					case SINT32:
					case FIXED32:
					case SFIXED32:
						int attrValInt = ((Integer) obj).intValue();
						row.put(oae.getName(), attrValInt);
						break;

					case INT64:
					case UINT64:
					case SINT64:
					case FIXED64:
					case SFIXED64:
						long attrValLong = ((Long) obj).longValue();
						row.put(oae.getName(), attrValLong);
						break;

					case BOOL:
						boolean attrValBool = ((Boolean) obj).booleanValue();
						row.put(oae.getName(), attrValBool);
						break;

					case STRING:
						String attrValStr = (String) obj;
						row.put(oae.getName(), attrValStr);
						break;
					case BYTES:
						byte[] attrValByte = ((ByteString) obj).toByteArray();
						row.put(oae.getName(), attrValByte);
						break;
					default:
						innerClassName = oae.getType();
						innerMsg = classList.get(innerClassName);
						innerCls = innerMsg.getCls();
						innerAttributes = innerMsg.getAttributes();
						for (AttributeEntity iae : innerAttributes) {
							String iAttrMethodName = StringUtils.camelCase("get_" + iae.getName(), '_');
							Method iAttrMethod = innerCls.getMethod(iAttrMethodName);
							Object iobj = iAttrMethod.invoke(obj);
							switch (iae.getType()) {
							case DOUBLE:
								double iValDouble = ((Double) iobj).doubleValue();
								row.put(iae.getName(), iValDouble);
								break;
							case FLOAT:
								break;
							case INT32:
							case UINT32:
							case SINT32:
							case FIXED32:
							case SFIXED32:
								int iValInt = ((Integer) iobj).intValue();
								row.put(iae.getName(), iValInt);
								break;
							case INT64:
							case UINT64:
							case SINT64:
							case FIXED64:
							case SFIXED64:
								long iValLong = ((Long) iobj).longValue();
								row.put(iae.getName(), iValLong);
								break;
							case BOOL:
								boolean iValBool = ((Boolean) iobj).booleanValue();
								row.put(iae.getName(), iValBool);
								break;
							case STRING:
								String iValStr = (String) iobj;
								row.put(iae.getName(), iValStr);
								break;

							case BYTES:
								byte iValByte = ((Byte) iobj).byteValue();
								row.put(iae.getName(), iValByte);
								break;

							default:
								getH2ORowData(iobj, iae.getType(), rows);
								break;
							}
						}
						logger.info(row.toString());
						// rows.add(row);
					}
					logger.info(row.toString());
				}
			}
			rows.add(row);
		} catch (Exception ex) {
			logger.error("Failed in getH2ORowData(): ", ex);
		}

	}

	/**
	 * This procedure uses H2O model to do prediction
	 * 
	 * @param df
	 * @param modelLoc
	 * @return prediction results in protobuf format
	 */
	private byte[] doPredictH2O(Object df, String modelLoc) {
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

			ArrayList<RowData> rows = new ArrayList<>();
			getH2ORowData(df, inputClassName, rows);

			ArrayList<Object> predictList = new ArrayList<>();

			for (RowData row : rows) {
				/*
				 * We handle the following model categories: Binomial Multinomial Regression
				 * Clustering AutoEncoder DimReduction WordEmbedding Unknown
				 */

				String current_model_category = mojo.getModelCategory().toString();
				logger.info("model category again: " + current_model_category);

				String pr = null;
				Object p = null;

				try {

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

				if (p != null) {
					predictList.add(p);
					p = null;
				}
			}

			// Create a Prediction and set its value depending on the output of
			// the H2o predict method
			Object pobj = getPredictionRow(outputClassName, predictList);

			Method toByteArray = pobj.getClass().getMethod("toByteArray");

			logger.info("In predict method: Done Prediction, returning binary serialization of prediction. ");
			return (byte[]) (toByteArray.invoke(pobj));

		} catch (Exception ex) {
			logger.error("Failed in doPredictH2O() ", ex);
			return null;

		}

	}

	/**
	 * This is API end point for enhanced generic model runner
	 * 
	 * @param dataset
	 *            : A serialized version of input data in binary data stream
	 * @param operation
	 *            : this specifies the operation from the service structure in
	 *            protobuf file
	 * @return : A serialized version of prediction in binary stream
	 */
	@RequestMapping(value = "/operation/{operation}", method = RequestMethod.POST)
	public byte[] operation(@RequestBody byte[] dataset, @PathVariable("operation") String operation) {
		logger.info("/operation/" + operation + " GETTING POST REQUEST:");
		logger.info(Arrays.toString(dataset));

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

			// dframe = DataFrame.parseFrom(datain);
			Class<?> inputClass = classList.get(inputClassName).getCls();
			Method method = inputClass.getMethod("parseFrom", new Class[] { byte[].class });

			Object df = method.invoke(null, dataset);

			if (!modelType.equalsIgnoreCase("G"))
				return doPredictH2O(df, null);
			else
				return doPredictGeneric(df, null);

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

		protoIdx = rand.nextInt(10000) + 1;

		pluginClassPath = new String(pluginRoot + SEP + "classes");
		protoJarPath = pluginClassPath + SEP + "pbuff" + protoIdx + ".jar";
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

		for (String cname : classNames) {
			MessageObject mobj = classList.get(cname);
			if (mobj == null) {
				classList.put(cname, mobj);
			}
			String className = "DatasetProto" + protoIdx + "$" + cname;
			if (pluginPkgName != null)
				className = pluginPkgName + ".DatasetProto" + protoIdx + "$" + cname;

			logger.info("Inside init(): loading class " + className);
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
	 * Set all proto classes and plugin package name
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

		logger.info("Inside setProtoClasses(): Plugin package: " + pluginPkgName);
		for (String cname : classNames) {
			logger.info("Inside setProtoClasses(): plugin class = " + cname);
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
	 * Based on protoString, set all attributes names and types for all messages
	 * specified.
	 * 
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

	/**
	 * Set attribute names and types for individual given message
	 * 
	 * @param cname
	 * @param protoString
	 * @return
	 * @throws Exception
	 */
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
			if (subline.indexOf("repeated") != -1) {
				pat = "(\\s*)repeated(\\s*)(\\w+\\s*)(\\w+\\s*)";
				isRepeated = true;
			} else if (subline.indexOf("optional") != -1) {
				pat = "(\\s*)optional(\\s*)(\\w+\\s*)(\\w+\\s*)";
				isOptional = true;
			} else if (subline.indexOf("required") != -1) {
				pat = "(\\s*)required(\\s*)(\\w+\\s*)(\\w+\\s*)";
				isRequired = true;
			} else
				pat = "(\\s*)(\\w+\\s*)(\\w+\\s*)";

			Pattern r = Pattern.compile(pat);
			Matcher mproto = r.matcher(subline);
			if (mproto.find()) {
				if (!isRepeated && !isOptional && !isRequired) {
					type = mproto.group(2).trim();
					attribute = mproto.group(3).trim();
					logger.info("setMessageProtoAttributes(): type = [" + type + "] attribute = [" + attribute + "]");
				} else {
					type = mproto.group(3).trim();
					attribute = mproto.group(4).trim();
					logger.info("setMessageProtoAttributes(): type = [" + type + "] attribute = [" + attribute + "]");
				}
			}
			if (attribute != null && type != null)
				mobj.addAttribute(attribute, type, isRepeated, isRequired);
		}

		return true;
	}

	private void generateProto(String protoString) throws Exception {
		if (protoString == null)
			protoString = getDefaultProtoString();

		if (protoString == null)
			protoString = generateMockMessage();

		/* Reset everything */
		pluginPkgName = null;
		classList.clear();
		classNames.clear();
		serviceList.clear();

		setProtoClasses(protoString);
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

		FileWriter fw = null;
		String option = "option java_outer_classname = \"DatasetProto" + protoIdx + "\";\n";

		try {
			fw = new FileWriter(filename);
			String[] old_list = protoString.split(NEWLINE);
			List<String> new_list = new ArrayList<>();

			if (protoString.contains("java_outer_classname")) {

				for (String line : old_list) {
					if (!line.contains("java_outer_classname"))
						new_list.add(line);
					else
						new_list.add(option);
				}
			} else {
				int index = 0;
				for (String line : old_list) {
					new_list.add(line);
					if (index == 0) {
						new_list.add(option);
					}
					index++;
				}
			}
			for (String out_line : new_list) {
				fw.write(out_line);
				fw.write(NEWLINE);
			}

			logger.info("Done writing dataset.proto");
		} finally {

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

		cmd = "protoc -I=" + protoFilePath + " --java_out=" + protoOutputPath + " " + protoFilePath + SEP
				+ "dataset.proto";

		try {
			exitVal = runCommand(cmd);
			String path = (pluginPkgName == null) ? protoOutputPath
					: protoOutputPath + SEP + pluginPkgName.replaceAll("\\.", SEP);

			if (pluginPkgName == null || !pluginPkgName.equals("com.google.protobuf"))
				insertImport(path + SEP + "DatasetProto" + protoIdx + ".java");

		} catch (Exception ex) {
			logger.error("Failed generating Java Protobuf source code: ", ex);
		}

		if (exitVal != 0)
			logger.error("Failed generating DatasetProto" + protoIdx + ".java");
		else
			logger.info("Complete generating DatasetProto" + protoIdx + ".java!");
	}

	/**
	 * 
	 * @param filename
	 *            : Java source file generated by the protocol buffer compiler
	 */
	private void insertImport(String filename) {
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
		String buildPath = protoOutputPath;
		String protoJavaRuntimeJar;
		String cmd;
		int exitVal = -1;

		if (pluginPkgName != null)
			buildPath = protoOutputPath + SEP + pluginPkgName.replaceAll("\\.", SEP);

		protoJavaRuntimeJar = pluginRoot;
		cmd = "javac -cp " + protoJavaRuntimeJar + SEP + "protobuf-java-" + protoRTVersion + ".jar " + buildPath + SEP
				+ "DatasetProto" + protoIdx + ".java -d " + pluginClassPath;

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
	private String execCommand(String cmd) throws IOException {

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
	private static void addURL(URL u) throws IOException {
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
