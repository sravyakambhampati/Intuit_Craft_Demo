# Intuit_Craft_Demo

### Where I can find source code?
* Source Code : Intuit_Craft_Demo >> etl >> source
* Input Files : Intuit_Craft_Demo >> etl >> data
* Tests: Intuit_Craft_Demo >> tests
* Output Files: Intuit_Craft_Demo >> etl >> output (This is git ignore. So you will not find any. Include in the zip file )
  
### How to run the code and test cases?

Clone this repo to local system

#### Pycharm IDE:

##### To run the source code:
Open the project. In terminal give following command to submit the pyspark code in local mode
spark-submit --master local --num-executors 2 --executor-memory 1g etl/source/main.py 

##### To run test cases:
Right click on tests folder and select Run 
