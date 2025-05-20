## README

Written by Dr. June Skeeter

Scripts to parse timeseries data from assorted dataloggers in order to generate consistently formatted data with standardized metadata.

Supported formats from Campbell Scientific dataloggers:

* binary [TOB2/3](https://help.campbellsci.com/loggernet-manual/ln_manual/campbell_scientific_file_formats/tob2_or_tob3.htm?TocPath=Campbell%20Scientific%20File%20Formats%7CDatalogger%20Data%20Formats%7C_____1)

* ascii [TOA5](https://help.campbellsci.com/loggernet-manual/ln_manual/campbell_scientific_file_formats/toa5.htm?TocPath=Campbell%20Scientific%20File%20Formats%7CComputer%20File%20Data%20Formats%7CTOA5%7C_____0)

* HOBOware csv files

* More file types to come ...

See templates/ for an example of desired input variables (templateInputs.yml) which correspond to the example file (templateExampleData.csv).  The input paramterss desired are common across input types, but the syntax of the input variables will depend on

## Setup & Installation

1. This repo has a submodule "helperFunctions" which is required for it to work.  In order to clone the repository **and** the relevant submodule, use this command:

`git clone --recurse-submodules https://github.com/gsc-permafrost/parseFiles`

2. Enter the repository:

`cd parseFiles`

3. It is best to run this in a virtual environment to avoid potential conflicts with other python packages.  Create a virtual environment using the following command:

`python -m venv .venv`

4. Activate the virtual environment:

* On windows: `.venv\scripts\activate`
* On mac: 
* On linux: 

5. Install the required libraries:

`pip install -r requirments.txt`