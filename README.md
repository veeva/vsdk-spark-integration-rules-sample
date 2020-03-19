# Vault Java SDK - vsdk-spark-integration-rules-sample

**Please see the [project wiki](https://github.com/veeva/vsdk-spark-integration-rules-sample/wiki) for a detailed walkthrough.**

The vsdk-spark-integration-rules-sample project covers the use of Spark Messaging to propagate messages from one vault (source) to another vault (target) using integration rules to map and transform values on route. The project will step through:

The vsdk-spark-integration-rules-sample project covers the use of Spark Messaging to propagate messages from one vault (source) to another vault (target) using integration rules to map and transform values between the differing data models on route. The project will step through:

* Setup of the necessary Vault to Vault components
  * Vault Connection records
  * Vault Queues for the inbound and outbound Spark Messages
  * Connection Integrations to organize an integration business process
  * Integration Integration Points to organize full round-trip step in an integration
  * Vault MDL for the Integration Rules
  * Various Vault components for the sample project
  * Vault Reference Lookups

* Sample Code for:
  * Message with integration rules and an HTTP Callback - a user action on the Warranties record in the source vault sends a message from a source vault to a target vault. This initiates a message processor in the target vault which retrieves the Integration Rules, before HTTP Callout in the target vault using the Vault API to query for more information the source vault, before transforming the data using the integration rules and creating or updating a Claims Warranties record in the target vault.

**You will need two sandbox vaults to run through this project.**
## How to import

Import as a Maven project. This will automatically pull in the required Vault Java SDK dependencies. 

For Intellij this is done by:
- File -> Open -> Navigate to project folder -> Select the 'pom.xml' file -> Open as Project

For Eclipse this is done by:
- File -> Import -> Maven -> Existing Maven Projects -> Navigate to project folder -> Select the 'pom.xml' file
	    
## License

This code serves as an example and is not meant for production use.

Copyright 2020 Veeva Systems Inc.
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
