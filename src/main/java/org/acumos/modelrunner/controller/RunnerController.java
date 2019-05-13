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
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.core.MediaType;

import org.acumos.modelrunner.domain.*;
import org.acumos.modelrunner.domain.MessageObject.AttributeEntity;
import org.acumos.modelrunner.utils.*;
import org.apache.commons.io.FileUtils;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
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
import hex.ModelCategory;
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
	private ArrayList<String> enumNames = new ArrayList<>();
	private Properties prop = new Properties();
	private String protoRTVersion = "3.5.1";
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
	@RequestMapping(value = "/model", method = RequestMethod.PUT)
	public ResponseEntity<Map<String, String>> putModel(@RequestPart("model") MultipartFile model) {
		logger.info("Receiving /model PUT request...");
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
	 *            An .proto file that defines input/output messages corresponding to input/output of the ML model
	 * @return ResponseEntity
	 */
	@ApiOperation(value = "Upload a protofile to replace the current default protofile", response = ResponseEntity.class)
	@RequestMapping(value = "/proto", method = RequestMethod.PUT)
	public ResponseEntity<Map<String, String>> putProto(@RequestPart("proto") MultipartFile proto) {
		logger.info("Receiving /proto PUT request...");
		Map<String, String> results = new LinkedHashMap<>();

		try {
			if (proto != null && !proto.isEmpty()) {
				byte[] bytes = proto.getBytes();

				// Create the file on server
				File protoFile = new File(PROJECTROOT + defaultProto);
				BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(protoFile));
				stream.write(bytes);
				stream.close();

				logger.info("putProto: Proto File Location=" + protoFile.getAbsolutePath());
			}
		} catch (Exception ex) {
			logger.error("putProto: Failed in uploading protofile: ", ex);
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
	 *            The modelConfig.properties file for generic java ML model
	 * @return ResponseEntity
	 */
	@ApiOperation(value = "Upload a model config file to replace current model configuration properties used by Generic Model Runner. H2O Model Runner does not use this file", response = ResponseEntity.class)
	@RequestMapping(value = "/model/configuration", method = RequestMethod.PUT)
	public ResponseEntity<Map<String, String>> putModelConfig(@RequestPart("modelConfig") MultipartFile configFile) {
		logger.info("Receiving /model/configuration PUT request...");
		Map<String, String> results = new LinkedHashMap<>();
		BufferedOutputStream stream = null;
		try {
			if (configFile != null && !configFile.isEmpty()) {
				byte[] bytes = configFile.getBytes();

				// Create the file on server
				File configPropertiesFile = new File(PROJECTROOT + modelConfig);
				stream = new BufferedOutputStream(new FileOutputStream(configPropertiesFile));
				stream.write(bytes);
				stream.close();

				logger.info("putModelConfig: Model config properties file location="
						+ configPropertiesFile.getAbsolutePath());
			}
		} catch (Exception ex) {
			logger.error("putModelConfig: Failed in uploading configfile: ", ex);
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
	 *            CSV file that contains the header and dataset
	 * @param operation
	 *            One of the operations specified in the .proto file
	 * @return The serialized input binary stream in protocol buffers format as inputs for the predictor
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
	 *            An .proto file that defines input/output messages corresponding to input/output of the ML model
	 * @param operation
	 *            One of the operations specified in the service structure of the .proto file
	 * @return 
	 *            The prediction in binary stream in protocol buffers format
	 */
	@ApiOperation(value = "Serialize the csv file based on the .proto file provided here. This .proto file will not replace the default protofile ")
	@RequestMapping(value = "/getBinary", method = RequestMethod.POST, produces = MediaType.APPLICATION_OCTET_STREAM)
	public byte[] getBinary(@RequestPart("csvFile") MultipartFile csvFile, @RequestPart("proto") MultipartFile proto,
			String operation) {
		logger.info("Receiving /getBinary POST request...");

		return getNewBinary_(csvFile, proto, operation);
	}
	
	/**
	 * getBinaryJSONDefault converts the uploaded json file based on the default .proto
	 * file
	 * 
	 * @param jsonFile
	 *            JSON file
	 * @param operation
	 *            one of the operations matching service structure in protofile
	 * @return binary stream in protobuf format as inputs for the predictor
	 */
	@ApiOperation(value = "Converts the JSON file to a binary stream in protobuf format using default.proto.")
	@RequestMapping(value = "/getBinaryJSONDefault", method = RequestMethod.POST, produces = "application/octet-stream")
	public byte[] getBinaryJSONDefault(@RequestPart("jsonFile") MultipartFile jsonFile, String operation) {
		logger.info("Receiving /getBinaryJSONDefault POST request...");
		return getJsonBinary_(jsonFile, null, operation);
	}

	/**
	 * getBinaryJSON converts the uploaded JSON file based on the default.proto
	 * 
	 * @param jsonFile
	 *            JSON file
	 * @param proto
	 *            The .proto file
	 * @param operation
	 *            One of the operations of service structure in the .proto file
	 * @return Serialized input binary stream in protocol buffer format as inputs for the predictor
	 */
	@ApiOperation(value = "Converts the JSON file to a binary stream in protobuf format using default.proto.")
	@RequestMapping(value = "/getBinaryJSON", method = RequestMethod.POST, produces = "application/octet-stream")
	public byte[] getBinaryJSON(@RequestPart("jsonFile") MultipartFile jsonFile, @RequestPart("proto") MultipartFile proto,
			String operation) {
		logger.info("Receiving /getBinaryJSON POST request...");
		return getJsonBinary_(jsonFile, proto, operation);
	}

	/**
	 * Serialize the JSON file based on the .proto file into binary protocol buffer format
	 * 
	 * @param file
	 *            JSON input file file
	 * @param proto
	 *            The .proto file
	 * @param operation
	 *            name of operation specified in the service structure of the .proto file
	 *            file
	 * @return
	 */
	private <T extends Message> byte[] getJsonBinary_(MultipartFile jsonFile, MultipartFile proto, String operation) {
		try {
			
			Message message = (Message) getInputMessageBuilder(jsonFile, proto, operation);

			logger.info("getJsonBinary_: Returning the following byte[] :");
			byte[] barray = message.toByteArray();
			logger.info(Arrays.toString(barray));
			return barray;

		} catch (Exception ex) {
			logger.error("getJsonBinary_: Failed getting binary stream inputs:", ex);
			return new byte[0];
		}
	}

	public <T extends Message> List<T> convertJsonArrayToProto(T prototype, JsonNode array, String extensionName) {
		List<T> messages = Lists.newArrayList();
		for (JsonNode messageNode : array) {
			try {
				String nodeJson = new ObjectMapper().writeValueAsString(messageNode);
				T message = convertJsonToProto(prototype, nodeJson, extensionName);
				if (!message.equals(prototype)) {
					messages.add(message);
				}
			} catch (IOException ex) {
				// Should not be possible to throw in newer versions of ObjectMapper.
				logger.error(String.format(
						"The extension %s does not match the schema. It should be a %s. Please refer "
								+ "to documentation of the extension.",
						extensionName, prototype.getDescriptorForType().getName()));
			}
		}
		return messages;
	}

	// Suppress because Proto has terrible type awareness with builders.
	@SuppressWarnings("unchecked")
	public <T extends Message> T convertJsonToProto(T prototype, String json, String extensionName) {
		try {
			Builder builder = prototype.newBuilderForType();
			JsonFormat.parser()
					.usingTypeRegistry(TypeRegistry.newBuilder().add(prototype.getDescriptorForType()).build())
					.merge(json, builder);
			
			return (T) builder.build();
		} catch (InvalidProtocolBufferException ex) {
			logger.error("Extension " + extensionName + " cannot be converted into proto type "
					+ prototype.getDescriptorForType().getFullName() + " - Details: " + ex.getMessage());

			return prototype;
		}
	}
	////
	/**
	 * This is the first step of serialization for dataset in JSON format
	 * @param file
	 * @param proto
	 * @return MessageOrBuilder
	 * @throws Exception
	 */
	private MessageOrBuilder getInputMessageBuilder(MultipartFile jsonFile, MultipartFile proto, String operation) {
		try {
			if (jsonFile.isEmpty()) {
				logger.error("getInputMessageBuilder: You failed to upload " + jsonFile.getOriginalFilename()
						+ " because the file was empty.");
				return null;
			}

			if (operation == null) {
				logger.error("getInputMessageBuilder: needs to specify operation");
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
				protoInput.close();
			}
			
			init(protoString);
			ServiceObject so = serviceList.get(operation);
			inputClassName = so.getInputClass();
			long size = jsonFile.getSize();

			File file = new File(jsonFile.getOriginalFilename());

			// Create the file using the touch method of the FileUtils class.
			// FileUtils.touch(file);

			// Write bytes from the multipart file to disk.
			FileUtils.writeByteArrayToFile(file, jsonFile.getBytes());
			ObjectMapper mapper = new ObjectMapper();
			JsonNode jsonData = mapper.readTree(file);

			logger.info("json input = " + jsonData.toString());

			Object defInstance = null;
			Class<?> thisClass = classList.get(inputClassName).getCls();
			Method defaultInstanceMethod = thisClass.getMethod("getDefaultInstance");
			defInstance = defaultInstanceMethod.invoke(null); // thisMessage.getDefaultInstance()

			String nodeJson = new ObjectMapper().writeValueAsString(jsonData);
			Message message = convertJsonToProto((Message) defInstance, nodeJson, "model");
			return message;

		} catch (Exception ex) {
			logger.error("getInputMessageBuilder(): Failed getting binary stream inputs:", ex);
			return null;
		}
	}
	
	/**
	 * Serialize the CSV input file based on the .proto file into binary protocol buffer format
	 * 
	 * @param file
	 *            The CSV input file containing headers
	 * @param proto
	 *            The .proto file
	 * @param operation
	 *            One of operation specified in the service structure of the .proto file
	 * @return Serialized input binary stream in protocol buffer format as inputs for the predictor
	 */
	private byte[] getNewBinary_(MultipartFile file, MultipartFile proto, String operation) {
		try {
			Object df = getInputClassBuilder(file, proto, operation); // df is of {InputClass}.Builder type
			if (df == null) {
				logger.error("getNewBinary_: Failed getting binary stream inputs:");
				return new byte[0];
			}
			Method dfBuilder = df.getClass().getMethod("build");
			Object obj = dfBuilder.invoke(df);

			Method tobytearray = obj.getClass().getSuperclass().getSuperclass().getSuperclass()
					.getDeclaredMethod("toByteArray");
			byte[] barray = (byte[]) tobytearray.invoke(obj);
			logger.info("getNewBinary_: Returning the following byte[] :");
			logger.info(Arrays.toString(barray));
			return barray;

		} catch (Exception ex) {
			logger.error("getNewBinary_: Failed getting binary stream inputs:", ex);
			return new byte[0];
		}
	}

	/**
	 * 
	 * @param csvFile
	 *            Input data set in CSV format
	 * @param operation
	 *            One of the operations from the .proto file
	 * @return 
	 *            The prediction in binary stream in protobuf format            
	 */
	@ApiOperation(value = "Gets a prediction binary stream in protobuf format for the training data in the provided csv file using default .proto file")
	@RequestMapping(value = "/transformCSVDefault", method = RequestMethod.POST)
	public byte[] transform(@RequestPart("csvFile") MultipartFile csvFile, String operation) {
		logger.info("Receiving /transformCSVDefault POST Request...");
		return transform_(csvFile, null, null, operation);
	}

	/**
	 * 
	 * @param csvFile
	 *            Input CSV File for the ML model
	 * @param model
	 *            An ML model
	 * @param proto
	 *            The protobuf file defining the input and output format of the ML model
	 * @param operation
	 *            One of the operations in the protobuf file
	 * @return The prediction in binary stream in protobuf format
	 */
	@ApiOperation(value = "Gets a prediction binary stream in protobuf format for the training data in the provided csv file using the ML model and .proto file provided here")
	@RequestMapping(value = "/transformCSV", method = RequestMethod.POST)
	public byte[] transform(@RequestPart("csvFile") MultipartFile csvFile, @RequestPart("model") MultipartFile model,
			@RequestPart("proto") MultipartFile proto, String operation) {
		logger.info("Receiving /transformCSV POST Request...");
		return transform_(csvFile, model, proto, operation);
	}
	
	/**
	 * 
	 * @param jsonFile
	 *            JSON file
	 * @param operation
	 *            Operation specified in the Service structure of the .proto file            
	 * @return The Prediction in binary stream in protocol buffers format
	 */
	@ApiOperation(value = "Gets the output of the model in protobuf format.")
	@RequestMapping(value = "/transformJSONDefault", method = RequestMethod.POST)
	public byte[] transformJSON(@RequestPart("jsonFile") MultipartFile jsonFile, String operation) {
		logger.info("Receiving /transformJSONDefault POST Request...");
		return transform_(jsonFile, null, null, operation);
	}

	/**
	 * @param jsonFile
	 *            JSON File
	 * @param model
	 *            An ML model
	 * @param proto
	 *            An .proto file that defines input/output messages corresponding to input/output of the ML model 
	 * @param operation
	 *            Operation specified in the Service structure of the .proto file
	 * @return The prediction in binary stream in protocol buffers format
	 */
	@ApiOperation(value = "Gets the output of the model in protobuf format.")
	@RequestMapping(value = "/transformJSON", method = RequestMethod.POST)
	public byte[] transformJSON(@RequestPart("jsonFile") MultipartFile jsonFile, @RequestPart("model") MultipartFile model,
			@RequestPart("proto") MultipartFile proto, String operation) {
		logger.info("Receiving /transformJSON POST Request...");
		return transform_(jsonFile, model, proto, operation);
	}


	/**
	 * @param file
	 *            Input Data
	 * @param model
	 *            An ML Model
	 * @param proto
	 *            An .proto file that defines input/output messages corresponding to input/output of the ML model
	 * @param operation
	 *            One of the operation(s) in the service structure of the .proto file
	 * @return The prediction in binary stream in protocol buffers format
	 */
	private byte[] transform_(MultipartFile file, MultipartFile model, MultipartFile proto, String operation) {
		try {
			String contentType = file.getContentType();
			Object df = null;
			Object obj =  null;
			if ("application/vnd.ms-excel".equalsIgnoreCase(contentType) || "text/csv".equalsIgnoreCase(contentType)) {
				df = getInputClassBuilder(file, proto, operation); // df is of {InputClass}.Builder

				Method dfBuilder = df.getClass().getMethod("build");
				obj = dfBuilder.invoke(df);
			}
			else if(contentType.toLowerCase().contains("json")) {
				obj = (Object)getInputMessageBuilder(file, proto, operation); // obj is MesssageOrBuilder
			}
			else {
				logger.error("transform_(): unsupported file type: " + contentType);
				return null;
			}

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
				logger.info("transform_: model File Location=" + modelFile.getAbsolutePath());
			}

			if (serviceList.isEmpty() || classList.isEmpty()) {
				logger.error("transform: Wrong protofile format - must specify message and service!");
				return null;
			}

			outputClassName = serviceList.get(operation).getOutputClass();

			if (!modelType.equalsIgnoreCase("G"))
				return doPredictH2O(obj, modelLoc);
			else
				return doPredictGeneric(obj, modelLoc);
		} catch (Exception ex) {
			logger.error("transform_: Failed transforming csv file and getting prediction results: ", ex);

		}

		return new byte[0];
	}

	/**
	 * Get {InputClass}.Builder class based on uploaded data file and proto file.
	 * This is the first step for serialization.
	 * 
	 * @param file
	 * @param proto
	 * @return {InputClass}.Builder
	 * @throws Exception
	 */
	private Object getInputClassBuilder(MultipartFile file, MultipartFile proto, String operation) throws Exception {
		if (file.isEmpty()) {
			logger.error("getInputClassBuilder: You failed to upload " + file.getOriginalFilename()
					+ " because the file was empty.");
			return null;
		}

		Object inputBuilder = null;

		String contentType = file.getContentType();
		if (!"application/vnd.ms-excel".equalsIgnoreCase(contentType) && !"text/csv".equalsIgnoreCase(contentType)) {
			logger.error("getInputClassBuilder: Wrong file type. Current content type is " + contentType);
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
			protoInput.close();
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
			logger.info("getInputBuilder: Header Line is: [" + headerLine + "]");

			String[] array;

			int idx;

			for (AttributeEntity ae : inputAttributes) {
				for (int i = 1; i < lines.length; i++) { // ignore the first line which is header
					String line = lines[i];
					logger.info("getInputBuilder: current line is: [" + line + "]");

					array = line.split(",");

					if (ae.isRepeated()) {
						String iAttrMethodName = StringUtils.camelCase("add_" + ae.getName(), '_');
						switch (ae.getType()) {
						case DOUBLE:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, double.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
									continue;
								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, Double.parseDouble(array[idx]));
							}
							break;
						case FLOAT:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, float.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
									continue;
								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, Long.parseLong(array[idx]));
							}
							break;
						case BOOL:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, boolean.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
									continue;
								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, Boolean.parseBoolean(array[idx]));
							}
							break;
						case STRING:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, String.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName()))
									inputAddOrSetRow.invoke(thisBuilder, array[idx]);
							}
							break;
						case BYTES:
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, ByteString.class);
							for (idx = 0; idx < headerFields.length; idx++) {
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									ByteString byteStr = ByteString.copyFrom(array[idx].getBytes());
									inputAddOrSetRow.invoke(thisBuilder, byteStr);
								}
							}
							break;
						default:
							String innerClassName = ae.getType();
							MessageObject mo = classList.get(innerClassName);
							if (mo == null) {
								innerClassName = thisClassName + "." + ae.getType();
								mo = classList.get(innerClassName);
								if (mo == null) {
									innerClassName = thisClassName + "$" + ae.getType();
									mo = classList.get(innerClassName);
								}
							}
							Class<?> innerCls = mo.getCls();
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, innerCls);

							if (mo.isEnum()) {
								Method forNumberMethod = innerCls.getDeclaredMethod("forNumber", int.class);

								for (idx = 0; idx < headerFields.length; idx++) {
									if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
										continue;
									if (headerFields[idx].equals(ae.getName())) {
										inputAddOrSetRow.invoke(thisBuilder,
												forNumberMethod.invoke(null, Integer.parseInt(array[idx])));
									}
								}
								break;
							}

							Object innerBuilder = getInnerBuilder(innerClassName, line, headerLine);
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
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
								if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
									continue;

								if (headerFields[idx].equals(ae.getName())) {
									ByteString byteStr = ByteString.copyFrom(array[idx].getBytes());

									inputAddOrSetRow.invoke(thisBuilder, byteStr);
									break;
								}
							}
							break;
						default:
							String innerClassName = ae.getType();
							MessageObject mo = classList.get(innerClassName);
							if (mo == null) {
								innerClassName = thisClassName + "." + ae.getType();
								mo = classList.get(innerClassName);
								if (mo == null) {
									innerClassName = thisClassName + "$" + ae.getType();
									mo = classList.get(innerClassName);
								}
							}
							Class<?> innerCls = mo.getCls();
							inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, innerCls);

							if (mo.isEnum()) {
								Method forNumberMethod = innerCls.getDeclaredMethod("forNumber", int.class);

								for (idx = 0; idx < headerFields.length; idx++) {
									if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
										continue;
									if (headerFields[idx].equals(ae.getName())) {
										logger.info("getDeclaredMethods: "
												+ Arrays.toString(innerCls.getDeclaredMethods()));
										logger.info("getMethods: " + Arrays.toString(innerCls.getMethods()));
										Object obj = forNumberMethod.invoke(null, Integer.parseInt(array[idx]));
										inputAddOrSetRow.invoke(thisBuilder, obj);
										break;
									}
								}
								break;
							}

							Object innerBuilder = getInnerBuilder(innerClassName, line, headerLine);

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
			logger.error("getInputBuilder: ", ex);
			return null;
		}
	}

	/**
	 * 
	 * @param thisClassName
	 * @param line
	 * @param headerLine
	 * @return return builder object of {thisClass}.Builder
	 */
	private Object getInnerBuilder(String thisClassName, String line, String headerLine) {
		try {
			logger.info("getInnerBuilder: current line is [" + line + "] header is [" + headerLine + "]");
			Object thisBuilder = null;
			MessageObject thisMsg = classList.get(thisClassName);
			Class<?> thisClass = thisMsg.getCls();
			Method thisBuilderMethod = thisClass.getMethod("newBuilder");
			thisBuilder = thisBuilderMethod.invoke(null); // thisBuilder is of {thisClass}.Builder

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
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
								continue;
							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, Double.parseDouble(array[idx]));
						}
						break;
					case FLOAT:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, float.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
								continue;
							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, Long.parseLong(array[idx]));
						}
						break;
					case BOOL:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, boolean.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
								continue;
							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, Boolean.parseBoolean(array[idx]));
						}
						break;
					case STRING:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, String.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
								continue;

							if (headerFields[idx].equals(ae.getName()))
								inputAddOrSetRow.invoke(thisBuilder, array[idx]);
						}
						break;
					case BYTES:
						inputAddOrSetRow = thisBuilder.getClass().getMethod(iAttrMethodName, ByteString.class);
						for (idx = 0; idx < headerFields.length; idx++) {
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
							if (array[idx] == null  && array[idx].length() == 0) // skip missing field
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
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
							if (idx >= array.length || array[idx] == null || array[idx].length() == 0) // skip missing field
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
			logger.error("getInnerBuilder: ", ex);
			return null;
		}
	}

	/*
	 * in the case of nestedmsg.proto First iteration : parent - DataFrame, child -
	 * DataFrameRow Second iteration: parent - DataFrameRow, child - SubFrameRow
	 */
	private void getRowString(Object df, String parentName, String attributeName, StringBuffer rowStr, StringBuffer headerStr) {
		try {
			logger.info("getRowString: " + parentName);
			MessageObject parentMsg = classList.get(parentName);
			Class<?> parentCls = parentMsg.getCls();
			ArrayList<AttributeEntity> parentAttributes = parentMsg.getAttributes();

			for (AttributeEntity ae : parentAttributes) {
				if (attributeName != null && !attributeName.equals(ae.getName()))
					continue;
				boolean commaAppended = false;
				if (ae.isRepeated()) {
					String pAttrMethodName = StringUtils.camelCase("get_" + ae.getName(), '_');
					Method getCount = parentCls.getMethod(pAttrMethodName + "Count");
					int rowCount = (int) getCount.invoke(df);
					logger.info("getRowString: We have: " + rowCount + " row(s) of " + ae.getName());

					Method getList = parentCls.getMethod(pAttrMethodName + "List");
					List<?> list = (List<?>) getList.invoke(df); // list of child objects or list of primitive types

					Object obj;
					int j;
					
					for (j = 0; j < rowCount; j++) {
						obj = list.get(j);
						
						commaAppended = false;
						if (rowStr.length() != 0 && rowStr.charAt(rowStr.length()-1) != '\n') {
							rowStr.append(",");
							commaAppended = true;
						}

						switch (ae.getType()) {
						case DOUBLE:
							double attrValDouble = ((Double) obj).doubleValue();
							rowStr.append(attrValDouble);
							appendHeader(headerStr, ae.getName());
							break;

						case FLOAT:
							float attrValFloat = ((Float) obj).floatValue();
							rowStr.append(attrValFloat);
							appendHeader(headerStr, ae.getName());
							break;

						case INT32:
						case UINT32:
						case SINT32:
						case FIXED32:
						case SFIXED32:
							int attrValInt = ((Integer) obj).intValue();
							rowStr.append(attrValInt);
							appendHeader(headerStr, ae.getName());
							break;

						case INT64:
						case UINT64:
						case SINT64:
						case FIXED64:
						case SFIXED64:
							long attrValLong = ((Long) obj).longValue();
							rowStr.append(attrValLong);
							appendHeader(headerStr, ae.getName());
							break;

						case BOOL:
							boolean attrValBool = ((Boolean) obj).booleanValue();
							rowStr.append(attrValBool);
							appendHeader(headerStr, ae.getName());
							break;

						case STRING:
							String attrValStr = (String) obj;
							rowStr.append(attrValStr);
							appendHeader(headerStr, ae.getName());
							break;

						case BYTES:
							byte[] attrValByte = ((ByteString) obj).toByteArray();
							rowStr.append(attrValByte);
							appendHeader(headerStr, ae.getName());
							break;

						default:
							String innerClassName;

							// Check whether it is an enum type or a message type
							if (enumNames.contains(innerClassName = ae.getType())
									|| enumNames.contains(innerClassName = (parentName + "$" + ae.getType()))) {
								// it's an enum type
								Class<?> cls = classList.get(innerClassName).getCls();
								Method getNumber = cls.getMethod("getNumber");
								int attrValEnum = (int) getNumber.invoke(obj);
								rowStr.append(attrValEnum);
							} else if (classNames.contains(innerClassName = ae.getType())
									|| classNames.contains(innerClassName = (parentName + "." + ae.getType()))) {
								// strip newly added ","
								if(commaAppended)
									rowStr.deleteCharAt(rowStr.length()-1);
								getRowString(obj, innerClassName, null, rowStr, headerStr);
							} else {
								// strip newly added ","
								if(commaAppended)
									rowStr.deleteCharAt(rowStr.length()-1);
								logger.error("getRowString: class " + ae.getType() + " or class " + innerClassName
										+ " not found");
							}
							break;
						}
					}
					
				} else {
					String pAttrMethodName = StringUtils.camelCase("get_" + ae.getName(), '_');
					Method pAttrMethod = parentCls.getMethod(pAttrMethodName);
					Object obj = pAttrMethod.invoke(df);
					commaAppended = false;
					if (rowStr.length() != 0 && rowStr.charAt(rowStr.length()-1) != '\n') {
						rowStr.append(",");
						commaAppended = true;
					}

					switch (ae.getType()) {
					case DOUBLE:
						double gcValDouble = ((Double) obj).doubleValue();
						rowStr.append(gcValDouble);
						appendHeader(headerStr, ae.getName());	
						break;

					case FLOAT:
						float gcValFloat = ((Float) obj).floatValue();
						rowStr.append(gcValFloat);
						appendHeader(headerStr, ae.getName());
						break;

					case INT32:
					case UINT32:
					case SINT32:
					case FIXED32:
					case SFIXED32:
						int gcValInt = ((Integer) obj).intValue();
						rowStr.append(gcValInt);
						appendHeader(headerStr, ae.getName());
						break;

					case INT64:
					case UINT64:
					case SINT64:
					case FIXED64:
					case SFIXED64:
						long gcValLong = ((Long) obj).longValue();
						rowStr.append(gcValLong);
						appendHeader(headerStr, ae.getName());
						break;

					case BOOL:
						boolean gcValBool = ((Boolean) obj).booleanValue();
						rowStr.append(gcValBool);
						appendHeader(headerStr, ae.getName());
						break;

					case STRING:
						String gcValStr = (String) obj;
						rowStr.append(gcValStr);
						appendHeader(headerStr, ae.getName());
						break;
						
					case BYTES:
						byte[] gcValByte = ((ByteString) obj).toByteArray();
						rowStr.append(gcValByte);
						appendHeader(headerStr, ae.getName());
						break;
						
					default:
						String innerClassName;

						// check if this is an enum type
						if (enumNames.contains(innerClassName = ae.getType())
								|| enumNames.contains(innerClassName = (parentName + "$" + ae.getType()))) {
							// it's an enum type
							Class<?> cls = classList.get(innerClassName).getCls();
							Method getNumber = cls.getMethod("getNumber");
							int attrValEnum = (int) getNumber.invoke(obj);
							rowStr.append(attrValEnum);
						} else if (classNames.contains(innerClassName = ae.getType())
								|| classNames.contains(innerClassName = (parentName + "." + ae.getType()))) {
							// strip newly added ","
							if(commaAppended)
								rowStr.deleteCharAt(rowStr.length()-1);
							getRowString(obj, innerClassName, null, rowStr, headerStr);
						} else {
							// strip newly added ","
							if(commaAppended)
								rowStr.deleteCharAt(rowStr.length()-1);
							logger.error("getRowString: class " + ae.getType() + " or class " + innerClassName
									+ " not found");
						}
						break;
					}
				}
				logger.info(rowStr.toString());
			}
			// testing
			appendHeader(headerStr, "\n");
			rowStr.append("\n");
		} catch (Exception ex) {
			logger.error("Failed in getRowString(): ", ex);
		}
	}

	private void appendHeader(StringBuffer headerStr, String field) {
		if(headerStr.indexOf("\n") == -1) {
			if(headerStr.length() != 0 && !field.equals("\n"))
				headerStr.append(",");
			headerStr.append(field);
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
			// Load model property
			String propFile = new String(PROJECTROOT + modelConfig);
			Properties prop = new Properties();
			InputStream input = new FileInputStream(propFile);
			prop.load(input);

			String modelMethodName = prop.getProperty("modelMethod");
			String modelClassName = prop.getProperty("modelClassName");
			String modelInputType = prop.getProperty("modelInputType");
			String modelOutputType = prop.getProperty("modelOutputType");
			
			if(modelInputType == null)
				modelInputType = new String("CSV");
			
			if(modelOutputType == null)
				modelOutputType = new String("CSV");

			logger.info("doPredictGeneric(): model class name = " + modelClassName + " | model method = " + modelMethodName + " | model input = " + modelInputType );
			
			
			String resultStr = null;
			String tempPath = TMPPATH + SEP + "tmpFiles";
			File dir = new File(tempPath);
			if (!dir.exists())
				dir.mkdirs();
			String genfile = tempPath + SEP + UUID.randomUUID() + "_genfile";
			
			switch(modelInputType.toUpperCase()) {
			case "CSV":
				StringBuffer rowString = new StringBuffer();
				StringBuffer headerStr = new StringBuffer();
				getRowString(df, inputClassName, null, rowString, headerStr);
				logger.info(resultStr = headerStr.toString() + rowString.toString());
				genfile += ".csv";
				break;
			
			case "JSON":
				Class<?> inputClass = classList.get(inputClassName).getCls();
				Object defInstance = null;
				Method defaultInstanceMethod = inputClass.getMethod("getDefaultInstance");
				defInstance = defaultInstanceMethod.invoke(null); // thisMessage.getDefaultInstance()
				Printer printer = JsonFormat.printer().preservingProtoFieldNames().usingTypeRegistry(
						TypeRegistry.newBuilder().add(((Message) defInstance).getDescriptorForType()).build());

				try {
					resultStr = printer.print((MessageOrBuilder) df);
					logger.info("doPredictGeneric(): converting dataset to JSON string: " + resultStr);
				} catch (Exception ex) {
					resultStr = null;
					logger.error("doPredictGeneric(): dataset is not in JSON Format: ", ex);
				}
				
				genfile += ".json";
				break;
				
			case "ARFF":
				genfile += ".arff";
				break;
			
			default:
				break;
			}
			
			FileWriter ff = new FileWriter(genfile);
			ff.write(resultStr);
			ff.close();

			// model invoke and preparation

			File modelSource = new File(PROJECTROOT + defaultModel);

			File modelJarPath = new File(pluginClassPath + SEP + UUID.randomUUID() + modelSource.getName());
			Files.copy(modelSource, modelJarPath);

			cl = RunnerController.class.getClassLoader();
			AddUrlUtil.addFile(modelJarPath);
			logger.info("Jar file path=" + modelJarPath);
			List<?> predictList = null;

			Class<?> modelClass = cl.loadClass(modelClassName);

			logger.info("getDeclaredMethods: " + Arrays.toString(modelClass.getDeclaredMethods()));
			logger.info("getMethods: " + Arrays.toString(modelClass.getMethods()));

			String paramType = getMethodParamType(modelClass, modelMethodName);
			if (paramType == null) {
				logger.debug("doPredictGeneric: paramType of model method is NULL");
				return null;
			}
			logger.info(modelMethodName + " method parameter type=" + paramType);
			Method methodPredict = null;

			switch (paramType) {
			case "java.io.File":
				File file = new File(genfile);
				methodPredict = modelClass.getDeclaredMethod(modelMethodName, File.class);
				if (methodPredict == null) {
					logger.debug("doPredictGeneric: cannot getDeclaredMethod " + modelMethodName);
					return null;
				}
				predictList = (List<?>) methodPredict.invoke(null, file);
				break;

			case "java.lang.String":
				methodPredict = modelClass.getDeclaredMethod(modelMethodName, String.class);
				if (methodPredict == null) {
					logger.debug("doPredictGeneric: cannot getDeclaredMethod " + modelMethodName);
					return null;
				}
				predictList = (List<?>) methodPredict.invoke(null, resultStr);
				break;

			default:
				break;
			}

			if (predictList == null) {
				logger.debug("predictlist is null");
				return null;
			}
			
			byte[] results = new byte[0];
			switch (modelOutputType.toUpperCase()) {
			case "CSV":
				Object pobj = getPredictionRow(outputClassName, predictList);

				Method toByteArray = pobj.getClass().getMethod("toByteArray");
				results = (byte[]) (toByteArray.invoke(pobj));
				logger.info("In doPredictGeneric() : Prediction results - " + results.toString());
				logger.info(
						"In doPredictGeneric() : Done Prediction, returning binary serialization of above prediction results - "
								+ Arrays.toString(results));

				return results;

			case "JSON": 
				Object defInstance = null;
				Class<?> outputClass = classList.get(outputClassName).getCls();
				Method defaultInstanceMethod = outputClass.getMethod("getDefaultInstance");
				defInstance = defaultInstanceMethod.invoke(null); // thisMessage.getDefaultInstance()
				ObjectMapper mapper = new ObjectMapper();
				// predictList should contain Json Ouput String from the predictor
				JsonNode jsonData = mapper.readTree((String)predictList.get(0));
				String nodeJson = new ObjectMapper().writeValueAsString(jsonData);
				Message message = convertJsonToProto((Message) defInstance, nodeJson, "model");

				logger.info("doPredictGeneric: Returning the following byte[] :");
				results = message.toByteArray();
				logger.info(Arrays.toString(results));
				return results;

			default:
				return results;
			}

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
					String innerClassName;

					// It's an enum type
					if (enumNames.contains(innerClassName = ae.getType())
							|| enumNames.contains(innerClassName = ae.getType().replaceAll("\\.", "\\$"))
							|| enumNames.contains(innerClassName = predictionClassName + "$" + ae.getType())) {

						Method forNumberMethod;
						MessageObject mo = classList.get(innerClassName);
						Class<?> innerCls = mo.getCls();

						forNumberMethod = innerCls.getDeclaredMethod("forNumber", int.class);
						if (ae.isRepeated()) {
							predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
							addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);

							List<Object> enumList = new ArrayList<>();
							List<Object> enumArrayList = new ArrayList<>();
							for (Object obj : predictList) {
								logger.info("Object is " + obj.getClass().getName());
								if (obj instanceof Integer) {
									if (!started)
										started = true;

									if (forNumberMethod.invoke(null, (Integer) obj) != null) {
										enumList.add(forNumberMethod.invoke(null, (Integer) obj));
										enumArrayList.add(obj);
										predictionAdded = true;
									}
								} else {
									if (started)
										break;
								}
							}

							if (!enumList.isEmpty()) {
								addPrediction.invoke(object, enumList);
								predictList.removeAll(enumArrayList);
							}
						} else {
							predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
							addPrediction = object.getClass().getMethod(predictMethodName, innerCls);

							for (Object obj : predictList) {
								logger.info("Object is " + obj.getClass().getName());
								if (obj instanceof Integer) {
									if (forNumberMethod.invoke(null, (Integer) obj) != null) {
										addPrediction.invoke(object, forNumberMethod.invoke(null, (Integer) obj));
										predictionAdded = true;
										predictList.remove(obj);
										break;
									}
								}
							}
						}

					} else if (classNames.contains(innerClassName = ae.getType())
							|| classNames.contains(innerClassName = predictionClassName + "." + ae.getType())) {
						if (ae.isRepeated()) {
							predictMethodName = StringUtils.camelCase("add_all_" + ae.getName(), '_');
							addPrediction = object.getClass().getMethod(predictMethodName, java.lang.Iterable.class);

							List msgList = new ArrayList();
							while (!predictList.isEmpty()) {
								Object innerObj = getPredictionRow(innerClassName, predictList);
								if (innerObj == null)
									break;
								msgList.add(innerObj);
								predictionAdded = true;
							}

							if (!msgList.isEmpty())
								addPrediction.invoke(object, msgList);
						} else {
							predictMethodName = StringUtils.camelCase("set_" + ae.getName(), '_');
							addPrediction = object.getClass().getMethod(predictMethodName,
									classList.get(innerClassName).getCls());
							Object innerObj = getPredictionRow(innerClassName, predictList);
							if (innerObj != null) {
								addPrediction.invoke(object, innerObj);
								predictionAdded = true;
							}
						}
						break;
					}
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
						/*
						 * Force all the numeric fields into Double since H2O RowData only Supports
						 * String and Double according to
						 * https://h2o-release.s3.amazonaws.com/h2o/rel-turing/10/docs-website/h2o-
						 * genmodel/javadoc/hex/genmodel/easy/exception/PredictUnknownTypeException.html
						 */
						case DOUBLE:
							double attrValDouble = ((Double) obj).doubleValue();
							row.put(oae.getName(), attrValDouble);
							break;

						case FLOAT:
							float attrValFloat = ((Float) obj).floatValue();
							row.put(oae.getName(), Double.parseDouble(new Float(attrValFloat).toString()));
							break;

						case INT32:
						case UINT32:
						case SINT32:
						case FIXED32:
						case SFIXED32:
							int attrValInt = ((Integer) obj).intValue();
							row.put(oae.getName(), Double.parseDouble(new Integer(attrValInt).toString()));
							break;

						case INT64:
						case UINT64:
						case SINT64:
						case FIXED64:
						case SFIXED64:
							long attrValLong = ((Long) obj).longValue();
							row.put(oae.getName(), Double.parseDouble(new Long(attrValLong).toString()));
							break;

						/* RowData does not support boolean, so convert it to String */
						case BOOL:
							boolean attrValBool = ((Boolean) obj).booleanValue();
							row.put(oae.getName(), Boolean.toString(attrValBool));
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
							// It's an enum type
							if (enumNames.contains(innerClassName = oae.getType())
									|| enumNames.contains(innerClassName = (outerClassName + "$" + oae.getType()))) {
								Class<?> cls = classList.get(innerClassName).getCls();
								Method getNumber = cls.getMethod("getNumber");
								int attrValEnum = (int) getNumber.invoke(obj);
								row.put(oae.getName(), attrValEnum);
							} else if (classNames.contains(innerClassName = oae.getType())
									|| classNames.contains(innerClassName = (outerClassName + "." + oae.getType()))) {
								getH2ORowData(obj, innerClassName, rows);
							} else {
								logger.error("getH2ORowString: class " + oae.getType() + " or class " + innerClassName
										+ " not found");
							}
							break;
						}
					}
				} else {
					String oAttrMethodName = StringUtils.camelCase("get_" + oae.getName(), '_');
					Method oAttrMethod = outerCls.getMethod(oAttrMethodName);
					Object obj = oAttrMethod.invoke(df);

					switch (oae.getType()) {
					/*
					 * Force all the numeric fields into Double since H2O RowData only Supports
					 * String and Double according to
					 * https://h2o-release.s3.amazonaws.com/h2o/rel-turing/10/docs-website/h2o-
					 * genmodel/javadoc/hex/genmodel/easy/exception/PredictUnknownTypeException.html
					 */
					case DOUBLE:
						double attrValDouble = ((Double) obj).doubleValue();
						row.put(oae.getName(), attrValDouble);
						break;

					case FLOAT:
						float attrValFloat = ((Float) obj).floatValue();
						row.put(oae.getName(), Double.parseDouble(new Float(attrValFloat).toString()));
						break;

					case INT32:
					case UINT32:
					case SINT32:
					case FIXED32:
					case SFIXED32:
						int attrValInt = ((Integer) obj).intValue();
						row.put(oae.getName(), Double.parseDouble(new Integer(attrValInt).toString()));
						break;

					case INT64:
					case UINT64:
					case SINT64:
					case FIXED64:
					case SFIXED64:
						long attrValLong = ((Long) obj).longValue();
						row.put(oae.getName(), Double.parseDouble(new Long(attrValLong).toString()));
						break;

					/* RowData does not support boolean, so convert it to String */
					case BOOL:
						boolean attrValBool = ((Boolean) obj).booleanValue();
						row.put(oae.getName(), Boolean.toString(attrValBool));
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
						// It's an enum type
						if (enumNames.contains(innerClassName = oae.getType())
								|| enumNames.contains(innerClassName = (outerClassName + "$" + oae.getType()))) {
							Class<?> cls = classList.get(innerClassName).getCls();
							Method getNumber = cls.getMethod("getNumber");
							int attrValEnum = (int) getNumber.invoke(obj);
							row.put(oae.getName(), attrValEnum);
						} else if (classNames.contains(innerClassName = oae.getType())
								|| classNames.contains(innerClassName = (outerClassName + "." + oae.getType()))) {
							getH2ORowData(obj, innerClassName, rows);
						} else {
							logger.error("getH2ORowString: class " + oae.getType() + " or class " + innerClassName
									+ " not found");
						}
						break;
					}
					logger.info(row.toString());
				}
			}
			if (!row.isEmpty())
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
				logger.error("doPredictH2O: Failed in loading H2O Model: ", ie);
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
				ModelCategory currentModelCategory = mojo.getModelCategory();
				logger.info("doPredictH2O: model category is " + currentModelCategory.toString() + " | current row is ["
						+ row.toString() + "]");
				Object p = null;
				Object pobj = null;

				try {
					switch (currentModelCategory) {
					case Binomial:
						p = model.predictBinomial(row);
						pobj = ((BinomialModelPrediction) p).label;
						break;

					case Multinomial:
						p = model.predictMultinomial(row);
						pobj = ((MultinomialModelPrediction) p).label;
						break;

					case Regression:
						p = model.predictRegression(row);
						// pobj is double type
						pobj = ((RegressionModelPrediction) p).value;
						break;

					case Clustering:
						p = model.predictClustering(row);
						// pobj is int type
						pobj = ((ClusteringModelPrediction) p).cluster;
						break;

					case AutoEncoder:
						p = model.predictAutoEncoder(row);
						// pobj is double[] type
						pobj = ((AutoEncoderModelPrediction) p).reconstructed;
						break;

					case DimReduction:
						p = model.predictDimReduction(row);
						// pobj is double[] type
						pobj = ((DimReductionModelPrediction) p).dimensions;
						break;

					// TODO: See if this works
					case WordEmbedding:
						p = model.predictWord2Vec(row);
						// pobj is HashMap<String, float[]>
						pobj = ((Word2VecPrediction) p).wordEmbeddings;
						break;

					case Unknown:
						logger.error(
								"Unknown model category. Results not available. Refer to http://docs.h2o.ai/h2o/latest-stable/h2o-genmodel/javadoc/hex ModelCategory.html");
						pobj = "Unknown h2o model category. Results not available. Refer to http://docs.h2o.ai/h2o/latest-stable/h2o-genmodel/javadoc/hex/ModelCategory.html";
						break;

					default:
						logger.error(
								"Model category not recognized. Results not guaranteed.Refer to http://docs.h2o.ai/h2o/latest-stable/h2o-genmodel/javadoc/hex/ModelCategory.html");
						pobj = "Your model did not match any supported category. Results not available.Refer to http://docs.h2o.ai/h2o/latest-stable/h2o-genmodel/javadoc/hex/ModelCategory.html";
						break;
					}
				} // try ends
				catch (PredictException pe) {
					logger.error("doPredictH2O: Failed getting prediction results from H2O model:", pe);
					pe.getMessage();
				} // catch ends
				logger.info("doPredictH2O: the prediction is  " + pobj.toString());
				if (pobj != null) {
					predictList.add(pobj);
					pobj = null;
					p = null;
				}
			}
			// Create a Prediction and set its value depending on the output of
			// the H2o predict method
			Object prow = getPredictionRow(outputClassName, predictList);
			if (prow == null) {
				logger.info("doPredictionH2O: No prediction");
				return new byte[0];
			}
			Method toByteArray = prow.getClass().getMethod("toByteArray");
			logger.info("doPredictH2O: Done Prediction, returning binary serialization of prediction. ");
			return (byte[]) (toByteArray.invoke(prow));
		} catch (Exception ex) {
			logger.error("Failed in doPredictH2O() ", ex);
			return new byte[0];
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
	@RequestMapping(value = "/model/methods/{name}", consumes = { "application/x-protobuf", "application/vnd.google.protobuf", "application/json", "text/plain" }, method = RequestMethod.POST)
	public byte[] operation(@RequestBody byte[] dataset, @PathVariable("name") String operation) {
		logger.info("/model/methods/" + operation + " GETTING POST REQUEST:");
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
			if (so == null) {
				logger.error("Inside operation(): Invalid operation [" + operation + "]");
				return null;
			}
			inputClassName = so.getInputClass();
			outputClassName = so.getOutputClass();

			// First step is to de-serialize the binary stream
			// dframe = DataFrame.parseFrom(datain);
			Class<?> inputClass = classList.get(inputClassName).getCls();

			Method method = inputClass.getMethod("parseFrom", new Class[] { byte[].class });

			Object df;
			try {
				df = method.invoke(null, dataset);
			} catch (Exception ex) {
				logger.error(
						"Inside operation(): possibly INVALID dataset - dataset needs to be in binary protobuf format: ",
						ex);

				return null;
			}

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
			boolean deleted = ExecCmdUtil.deleteDirectory(dir);
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

		downloadProtoJavaRuntime();
		generateProto(protoString); // Use null for now.

		cl = RunnerController.class.getClassLoader();

		AddUrlUtil.addFile(protoJarPath);

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
			if (arr[idx].equals("message") || arr[idx].equals("enum")) {
				idx = processMessageOrEnum(arr, idx, null);
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
	private int processMessageOrEnum(String[] arr, int idx, String outerClsName) {
		logger.info("Inside processMessageOrEnum(): ");

		boolean isEnum = false;
		if (arr[idx].equals("enum"))
			isEnum = true;

		String msgname = "";
		if (outerClsName != null && outerClsName.length() > 0) {
			// msgname = outerClsName + ((isEnum) ? "$" : ".");
			msgname = outerClsName + "$";
		}

		msgname += arr[idx + 1].endsWith("{") ? arr[idx + 1].substring(0, arr[idx + 1].length() - 1) : arr[idx + 1];

		logger.info("Inside processMessageOrEnum() : Adding message: " + msgname);
		classNames.add(msgname);

		if (isEnum)
			enumNames.add(msgname);

		for (int i = idx + 1; i < arr.length; i++) {
			if (arr[i].equals("message") || arr[i].equals("enum")) {
				int j = processMessageOrEnum(arr, i, msgname);
				i = j;
				continue;
			}
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
		MessageObject mobj = classList.get(cname);

		if (mobj == null) {
			mobj = new MessageObject(cname);
			classList.put(cname, mobj);
		}
		mobj.setEnum(enumNames.contains(cname));
		String cnamearr[] = (mobj.isEnum()) ? cname.split("\\$") : cname.split("\\.");
		String pattern;
		Pattern p;
		Matcher m;
		int idx_msg1 = 0;
		if (cnamearr.length == 0) {
			cnamearr = new String[1];
			cnamearr[0] = new String(cname);
		}
		for (String element : cnamearr) {
			pattern = "(message\\s+" + element + "\\s+)";
			p = Pattern.compile(pattern);
			m = p.matcher(protoString);

			if (!m.find()) {
				pattern = "(enum\\s+" + element + "\\s+)";
				p = Pattern.compile(pattern);
				m = p.matcher(protoString);

				if (!m.find()) {
					logger.error("Cannot find message or enum " + cname);
					return false;
				}
			}
			String search = m.group(0);
			logger.info("SetMessageProtoAttributes: search pattern = [" + search + "]");

			idx_msg1 = protoString.indexOf(search, idx_msg1);
		}

		int idx_begincurly1 = protoString.indexOf("{", idx_msg1);
		int idx_endcurly1 = protoString.indexOf("}", idx_begincurly1);
		if (idx_msg1 == -1 || idx_begincurly1 == -1 || idx_endcurly1 == -1) {
			logger.error("Wrong proto String format!");
			return false;
		}

		String curMsg = protoString.substring(idx_begincurly1 + 2, idx_endcurly1 - 1);
		// Remove inner message or enum
		boolean allRemoved = false;
		int idx_innerMsg, idx_innerEnum, idx_inner;
		StringBuffer pbuf = new StringBuffer(protoString);
		while (!allRemoved) {
			idx_innerMsg = curMsg.lastIndexOf("message");
			idx_innerEnum = curMsg.lastIndexOf("enum");
			if (idx_innerMsg == -1 && idx_innerEnum == -1) {
				allRemoved = true;
				break;
			}
			idx_inner = (idx_innerMsg >= idx_innerEnum) ? idx_innerMsg : idx_innerEnum;

			pbuf.replace(idx_begincurly1 + 2 + idx_inner, idx_endcurly1 + 1, "");
			idx_endcurly1 = pbuf.toString().indexOf("}", idx_begincurly1);
			curMsg = pbuf.toString().substring(idx_begincurly1 + 2, idx_endcurly1 - 1);
		}
		//
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
				logger.debug("Can't find \"=\" from line " + line);
				return false;
			}
			String subline = line.substring(0, idx_equal);
			String pat = null;
			if (subline.indexOf("repeated") != -1) {
				pat = "(\\s*)repeated(\\s*)(\\w+(\\.?\\w+)*\\s*)(\\w+\\s*)";
				isRepeated = true;
			} else if (subline.indexOf("optional") != -1) {
				pat = "(\\s*)optional(\\s*)(\\w+(\\.?\\w+)*\\s*)(\\w+\\s*)";
				isOptional = true;
			} else if (subline.indexOf("required") != -1) {
				pat = "(\\s*)required(\\s*)(\\w+(\\.?\\w+)*\\s*)(\\w+\\s*)";
				isRequired = true;
			} else if (mobj.isEnum())
				pat = "(\\s*)(\\w+\\s*)";
			else
				pat = "(\\s*)(\\w+(\\.?\\w+)*\\s*)(\\w+\\s*)";

			Pattern r = Pattern.compile(pat);
			Matcher mproto = r.matcher(subline);
			int count = mproto.groupCount();
			if (mproto.find()) {
				if (mobj.isEnum()) {
					attribute = mproto.group(count).trim();
					logger.info("setMessageProtoAttributes(): type = [int64] attribute = [" + attribute + "]");
				} else {
					for (int i = 0; i <= count; i++) {
						logger.info("setMessageProtoAttributes(): mproto(" + i + ") = " + mproto.group(i));
					}
					type = mproto.group(count - 2).trim();
					attribute = mproto.group(count).trim();
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
		enumNames.clear();
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
			exitVal = ExecCmdUtil.runCommand(cmd);
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
	private void downloadProtoJavaRuntime() {
		logger.info("downloadProtoJavaRuntime: The default protobuf runtime version is " + protoRTVersion);
		try {
			String cmdStr = "curl --silent https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/maven-metadata.xml | grep -Po '(?<=<version>)([\\S]*?)(?=<\\/version>)' | sort --version-sort -r | head -n 1";

			ImmutableList<String> cmd01 = ImmutableList.of("/bin/bash", "-c", cmdStr);
			ProcessBuilder pb = new ProcessBuilder(cmd01);
			logger.info("downloadProtoJavaRuntime: executing command: " + cmdStr);

			Process p = pb.start();
			// get the error stream of the process and print it
			InputStream error = p.getErrorStream();
			ExecCmdUtil.printCmdError(error);
			PrintWriter printWriter = new PrintWriter(p.getOutputStream());
			BufferedReader bufferedReader0 = new BufferedReader(new InputStreamReader(p.getInputStream()));
			ArrayList<String> output = ExecCmdUtil.printCmdOutput(bufferedReader0);
			if (!output.isEmpty())
				protoRTVersion = output.get(0);

			printWriter.flush();
			int exitVal0 = p.waitFor();

			String mavenUrl = "https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/" + protoRTVersion
					+ "/protobuf-java-" + protoRTVersion + ".jar";
			logger.info("downloadProtoJavaRuntime: mavenurl = " + mavenUrl);
			logger.info("The latest protobuf runtime Version is " + protoRTVersion);

			String cmd = "curl -o " + pluginRoot + SEP + "protobuf-java-" + protoRTVersion + ".jar " + mavenUrl;
			logger.info("executing command " + cmd);
			int exitVal = -1;
			exitVal = ExecCmdUtil.runCommand(cmd);

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
			exitVal = ExecCmdUtil.runCommand(cmd);
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
			// Find $JAVA_HOME
			ImmutableList<String> cmd = ImmutableList.of("/bin/bash", "-c", "echo $JAVA_HOME");
			ProcessBuilder pb = new ProcessBuilder(cmd);
			logger.info("updateProtoJar: executing command: \"echo $JAVA_HOME\"");

			Process p = pb.start();
			// get the error stream of the process and print it
			InputStream error = p.getErrorStream();
			ExecCmdUtil.printCmdError(error);
			PrintWriter printWriter = new PrintWriter(p.getOutputStream());
			BufferedReader bufferedReader0 = new BufferedReader(new InputStreamReader(p.getInputStream()));
			ArrayList<String> output = ExecCmdUtil.printCmdOutput(bufferedReader0);
			String javaPath = "";
			if (!output.isEmpty())
				javaPath = output.get(0);
			printWriter.flush();
			exitVal = p.waitFor();

			logger.info("updateProtoJar: Exit Value for which \"echo $JAVA_HOME\": " + exitVal);

			String jarCmd = "jar";

			if (javaPath != null && javaPath.length() != 0) // use absolute path
				jarCmd = javaPath + SEP + "bin" + SEP + "jar";

			String protort = pluginRoot + SEP + "protobuf-java-" + protoRTVersion + ".jar";
			pb = new ProcessBuilder(jarCmd, "-xvf", protort);
			pb.directory(new File(pluginClassPath));

			logger.info("Setting directory to : " + pluginClassPath + " before producing/updating pbuff.jar");
			logger.info("executing command: \"" + jarCmd + " -xvf " + protort + "\" from directory " + pluginClassPath);
			p = pb.start();
			// get the error stream of the process and print it
			error = p.getErrorStream();
			ExecCmdUtil.printCmdError(error);

			printWriter = new PrintWriter(p.getOutputStream());
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			ExecCmdUtil.printCmdOutput(bufferedReader);

			printWriter.flush();
			exitVal = p.waitFor();

			logger.info("Exit Value: " + exitVal);
			File plugin = new File(pluginClassPath);
			JarOutputStream target = new JarOutputStream(new FileOutputStream(protoJarPath));
			AddUrlUtil.add(plugin, plugin, target);
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
}
