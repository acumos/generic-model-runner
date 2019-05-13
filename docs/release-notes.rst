.. ===============LICENSE_START=======================================================
.. Acumos CC-BY-4.0
.. ===================================================================================
.. Copyright (C) 2017-2018 AT&T Intellectual Property & Tech Mahindra. All rights reserved.
.. ===================================================================================
.. This Acumos documentation file is distributed by AT&T and Tech Mahindra
.. under the Creative Commons Attribution 4.0 International License (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at
..
.. http://creativecommons.org/licenses/by/4.0
..
.. This file is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.
.. ===============LICENSE_END=========================================================

==================================
Generic Model Runner Release Notes
==================================

The server is deployed within a Docker image in the Docker registry.

Version 2.3.0, 12 May 2019
--------------------------

* Rename /{operation} to /model/methods/{name} (ACUMOS-732)

Version 2.2.5, 22 April 2019
----------------------------

* Generic model runner run containerized process as unprivileged user

Version 2.2.4, 2 February 2019
------------------------------

* Tweak command that always downloads the latest protobuf JAVA runtime library 

Version 2.2.3, 20 August 2018
-----------------------------

* Allow missing data in the last field of CSV input dataset (ACUMOS-1613)
* Remove Java Doc warnings

Version 2.2.2, 13 August 2018
-----------------------------

* Auto-generate a header for all generic Java models when the model input is CSV format. The model runner will combine this header with de-serialized data after un-marshaling binary stream and feed it into the model (ACUMOS-1580)

Version 2.2.1, 18 July 2018
---------------------------

* Add /getBinaryJSON (ACUMOS-1164)
* Add /getBinaryJSONDefault (ACUMOS-1164)
* Add /transformJSON (ACUMOS-1164)
* Add /transformJSONDefault (ACUMOS-1164)

Version 2.2.0, 30 April 2018
----------------------------

* Change /operation/{operation} to /{operation} (ACUMOS-769)
* Change /transform to /transformCSV
* Change /transformDefault to /transformCSVDefault
* Change /putProto to /proto
* Change /putModel to /model
* Change /putModelConfig to /model/configuration

Version 2.1.3, 25 April 2018
----------------------------

* Add conditional check to make sure empty rowData will not be fed into the H2O model (ACUMOS-728)
* Force all the numeric input fields of H2O models to double/String (ACUMOS-716)

Version 2.1.2, 16 April 2018
----------------------------

* Remove the use of getVersion.sh which gets the latest version of protobuf runtime library and implement corresponding steps using ProcessBuilder Java class (ACUMOS-673) 

Version 2.1.1, 5 April 2018
---------------------------

* Support non-scalar ENUM data type in the proto file for all POST end points (ACUMOS-631)
* The ENUM can be standalone or embedded in a message (ACUMOS-631)

Version 2.1.0, 4 April 2018
---------------------------

* Support embedded messages - messages defined inside another messages (ACUMOS-632)

Version 2.0.1, 15 March 2018
----------------------------

* Enhance end points /transform, /transformDefault, /getBinary, /getBinaryDefault to accept nested message proto files with no naming restriction
* Add /putModelConfig to allow uploading new modelConfig.properties
* Modify /putProto to allow replacing current default protofile
* Modify /putModel to allow replacing current model.
* Add JUnit test cases for all above end points.

Version 2.0.0, 22 February 2018
-------------------------------

* Add /operation/{operation} end point 
* Only proto3 syntax is supported. 
* Please note that required fields are not allowed in proto3. 
* Please also note that explicit 'optional' labels are disallowed in the Proto3 syntax. To define 'optional' fields in Proto3, simply remove the 'optional' label, as fields are 'optional' by default.
* Remove all restrictions on the naming and number of input and output messages.  
* The service structure must be present. Model Runner based on this structure to find operation name, input messages, and output messages.
* Support 15 scalar data types in all defined messages.

Version 1.0.3, 3 January 2018
-----------------------------

* Add /putModel end point and add /putProto end point

Version 1.0.2, 6 December 2017
------------------------------

* Support /predict, /transform, /transformDefault, /getBinary, /getBinaryDefault end points
* The first line of the proto file must specify proto3 syntax 
* The proto file must define three messages: DataFrameRow, DataFrame, and Prediction 
* The input message is always DataFrame which contains only one field as "repeated DataFrameRow rows = 1;"
* The output message is always Prediction which contains only one field as "repeated string prediction = 1;"
* Support 15 scalar data types in the DataFrameRow message as defined in https://developers.google.com/protocol-buffers/docs/proto3#generating
* The service structure is not required in this release. 
