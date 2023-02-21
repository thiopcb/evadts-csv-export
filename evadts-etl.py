# Python 3.9
# Coding: utf-8
# Import OS module to get all files in a directory
import os
from pathlib import Path
# Get a list of all files and directories:
path = Path(os.path.abspath(os.getcwd()) + '//EVADTS_processing' + '//rawdata')
directory_list = os.listdir(path)
# File processing: Get lines from file and store them in variable
# Open and read the file's contents:
filename = Path(str(path) + '//' + str(directory_list[9]))

# Define a function to process and extract the data in the content line:
"""
This function processes and extract data in the content line from a BlueRed file.
It finds the positions of all '*' delimiters, stores them in list. These positions
are used as follow: 
- To generate a subfix, concatenate it to the code prefix, then store it as dictionary key.
- To index line to find values and store them as dictionary value.
All created dictionaries are concatenated to a list.
"""
def extBlueRedfile(delimiter, line):
    # Count total quantity of delimiter in the content line:
    numPosition = line.count(delimiter)
    # Set initial values for working variables:
    index = numPosition
    prevPosition = 0
    nextPosition = line.find(delimiter)
    positions = []
    # Find all positions of delimiter in the content line:
    while index > 0:
        positions.append(nextPosition)
        prevPosition = nextPosition
        nextPosition = line.find(delimiter, prevPosition+1)
        index -= 1
    # Set initial values for working variables:
    index = numPosition - 1
    prevIndex = 0
    nextIndex = 1
    tempCode = numPosition - index
    tempDictionary = {}
    tempList = []
    # Index and find all data in the content line, and load them into a list:
    while index > 0:
        # Checking index number before concatenate to EVA-DTS code
        if tempCode < 10:
            tempDictionary = {
                line[:3] + '0' + str(tempCode) : 
                line[positions[prevIndex]+1 : positions[nextIndex]].strip()
                }
        else:
            tempDictionary = {
                line[:3] + str(tempCode) : 
                line[positions[prevIndex]+1 : positions[nextIndex]].strip()
                }
        # Do not append dictionary into list if value is not empty
        tempValue = list(tempDictionary.values())
        if tempValue[0] != '':
                tempList.append(tempDictionary)
        else:
            None
        index -= 1
        prevIndex = nextIndex
        nextIndex += 1
        tempCode = numPosition - index
    # Get the last data in the content line, and load it into a list:
    # Checking index number before concatenate to EVA-DTS code
    if tempCode < 10:
        tempDictionary = {
            line[:3] + '0' + str(tempCode) : 
            line[positions[prevIndex]+1 : len(line)].strip()
            }
    else:
        tempDictionary = {
            line[:3] + str(tempCode) : 
            line[positions[prevIndex]+1 : len(line)].strip()
            }
    # Do not append dictionary into list if value is not empty
    tempValue = list(tempDictionary.values())
    if tempValue[0] != '':
        tempList.append(tempDictionary)
    else:
        None
    # Return list of key:value pairs in the content line:
    return tempList

################################################
# Extraction processing...... file into memory #
################################################
# Loop through a file to copy line by line until end of line:
with open(filename, 'r') as openFile:
    # Initialize working variables "listContent" for storing extracted contents into list:
    lineReader = openFile.readlines()
    charDelimiter = ''
    listContent = []
    # Use 'extBlueRedfile()' function to process a file, line by line, store data into list:
    for row in lineReader:
        if row[:2].isalpha() and row[2:3].isdigit():
            if int(row[2:3]) > 0:
                if '*' in row:
                    charDelimiter = '*'
                    listContent = listContent + extBlueRedfile(charDelimiter, row.strip())
                else:
                    None
            else:
                None
        else:
            None
openFile.close()

# Import libraries to convert list variables to dataframe
import pandas as pd
import numpy as np
# Initialise working variables, "evadtsCode" and "evadtsValue" for storing as columns:
listCode = []
listValue = []
# Extract data from dictionaries, append data to column variables:
for index in range(len(listContent)):
    tempKeys = list(listContent[index].keys())[0]
    tempValues = list(listContent[index].values())[0]
    listCode.append(tempKeys)
    listValue.append(tempValues)
# Iterate column variables and return as a single list variable
zipList = list(zip(listCode, listValue))
# Convert list to dataframe
dfEvaFile = pd.DataFrame(zipList, columns = ['Code', 'Values'])

# Use pandas 'read_csv' method to file into a dataframe
# Open and read the file's contents:
filename = Path(os.path.abspath(os.getcwd()) + '//EVADTS_processing' + '//EVADTS_MAST.csv')
dfEvaCode = pd.read_csv(filename, header=0)

# Use pandas 'merge()' method to join dataframes: 'dfBlueRed' and 'dfEvaCode', into a dataframe:
dfBlueRed = dfEvaFile.merge(dfEvaCode, how='left', left_on='Code', right_on='EVA-DTS code')
# Drop unwanted columns, 'Line' and 'EVA-DTS code':
dfBlueRed = dfBlueRed.drop(['Line', 'EVA-DTS code'], axis=1)

###################################
# Transformation processing...... #
###################################
# Get serial number reference row equal to 'ID106' in 'Code' column, and create 'VM_serial' column:
valMachCode = str(int(dfBlueRed['Values'].values[dfBlueRed['Code'] == 'ID106']))
dfBlueRed['VM_serial'] = pd.DataFrame({'VM_serial' : [valMachCode]*len(dfBlueRed)})

# Get datetime reference row equal to 'EA302' + 'EA303' in 'Code' column:
valDate = str(int(str(20) + dfBlueRed['Values'].values[dfBlueRed['Code'] == 'EA302']))
valTime = str(int(dfBlueRed['Values'].values[dfBlueRed['Code'] == 'EA303']))
# Check if value in variable 'valTime' has 6 digits, HHMMSS:
if len(valTime) < 6:
    valTime = '0' + valTime
else:
    None

# Concaternate datetime and store in 'valDatetime' variable, and create 'Retrieve_date' column:
valDatetime = pd.to_datetime(valDate + ' ' + valTime, format='%Y%m%d %H:%M:%S')
dfBlueRed['Retrieve_date'] = pd.DataFrame({'Retrieve_date' : [valDatetime]*len(dfBlueRed)})

# Reorder the columns in dataframe:
colReorder = ['Retrieve_date', 'VM_serial', 'Code', 'Description', 'Values']
dfBlueRed = dfBlueRed.loc[:, colReorder]

# Write transformed dataframe to a CSV file:
newFilename = valMachCode + '-' + valDate
filename = Path(os.path.abspath(os.getcwd()) + '//EVADTS_processing' + '//data' + '//' + newFilename + '.csv')
dfBlueRed.to_csv(filename)
