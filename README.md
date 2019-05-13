# Generic/H2O Model Runner

In the application.properties file under resources directory, the property, model\_type, defines the type of model runner this is. If model_type is defined and the value is G, then this is a generic Java Model Runner, invoking generic models internally; otherwise, this is a H2O Model Runner, running H2O models instead.

The model runner takes a proto string, extracts attributes information from
this proto string, and writes this proto string to dataset.proto file. The model
runner then invokes protoc compiler to compile this dataset.proto file to generate
DatasetProto.java file. After that the model runner invokes javac compiler
and compiles this java file to the corresponding class files. 

There are eight end points in the Model Runner, five of them are POST requests - /model/methods/{name}, /transform, /transformDefault, /getBinary and /getBinaryDefault; and the rest are PUT requests - /putProto, /putModel, /putModelConfig. 

For /model/methods/{name} API, the request body contains a binary string that the model runner needs to parse before passing it to the predictor. The {name} must match one of the service methods specified in the proto file. To parse, the model runner dynamically load all the relevant DatasetProto$*** classes generated by protoc and javac compiler at run time so that it can use its de-serialization methods. After the binary stream gets de-serialized, the results will be used to construct the row data(for H2O Models) or CSV-like row strings(for Generic Java Model) in the format that the predictor accepts. The Model Runner then re-serialize the results and send it back to the client.
 
The /transform and /transformDefault APIs allow the users to directly upload a .csv file that contains all the columns of data that match with what's specified in the default.proto file. All the CSV files must have a header. The fields in the header should match the fields in the proto file. The end point /transform allows users to upload different proto file and model archive(*.zip or *.jar). The end point /transformDefault uses the default proto file and model.zip/model.jar as specified in the application.properties file. The model runner will build the binary representation of the .csv file using the DatasetProto$*** classes which saves the users from having to convert the .csv file to binary stream themselves. Both end points will return the prediction results. 

The /getBinary and /getBinaryDefault APIs are utilities allowing users to upload a .csv file and returning its binary representation in array of byte[]. The users can use this returned byte string as input data to the /predict API. 

The /putModel, /putProto, /putModelConfig allows users to replace the current model, protofile, and modelConfig.properties file.

## Requirements

In order for the Model Runner to be able to dynamically load the plugin jar that contains the proto classes at run time, the plugin jar must be outside the project directories. The application.properties specifies the default plugin root directory, ${plugin_root}, which if not existed, will be created when the Model Runner starts.  When the Model Runner receives a POST request, it will put the generated JAVA code under ${plugin_root}/src directory and generated class files under ${plugin_root}/classes directory. Therefore, these two directories, ${plugin_root}/src and ${plugin_root}/classes, must also be present. If not, the model runner will create them. 

## Supported Methods and Objects

The micro service methods and objects are documented using Swagger. A running server documents itself at a URL like the following, but consult the server's application.properties file for the exact port number ("8334") and context root ("modelrunner") in use:

	http://localhost:8334/modelrunner/swagger-ui.html

## Build Prerequisites

The build machine needs the following:

1. Java version 1.8
2. Maven version 3
3. Connectivity to Maven Central (for most jars)
4. protoc compiler for JAVA
5. protobuf JAVA Runtime Library 3.4.0

## Build and Package

Use maven to build and package the service into a single "fat" jar using this command:

	mvn clean install

## Launch Prerequisites

1. Java version 1.8
2. A valid application.properties file.
3. protoc compiler for JAVA
4. protobuf JAVA Runtime Library 3.4.0

### Launch Instructions

Start the microservice for development and testing like this:

	mvn clean spring-boot:run

To launch from Eclipse, run the class org.acumos.modelrunner.Application
 
To launch from the command line with an external configuration file, type like this:

	java -jar ./target/modelrunner-2.1.0-SNAPSHOT.jar --spring.config.location=./application.properties