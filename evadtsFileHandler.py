# Python 3.9
# Coding: utf-8
# Version: 1.0
##########################################################################################
# Module of functions to extract an EVA-DTS formatted file, and a CSV file contained the #
# EVA-DTS description master file in CSV. Data in both files are merged to a single      #
# dataframe where it is write to a CSV file for use.                                     #
##########################################################################################

# Import OS module and libraries to get all files in a directory and convert lists to dataframe
import os
from pathlib import Path
import pandas as pd
import numpy as np

def getfileEvaDts(fileName):
    """
    This functions load an EVA-DTS file in memory, processes the delimited values according to
    ID codes and values. Processed ID codes and values are appended output into a dataframe.
    """
    # Set current working directory:
    if 'EVADTS_processing' in os.getcwd():
        cwd = os.getcwd()
    else:
        cwd = os.getcwd() + '//EVADTS_processing'
    try:
        fileEvaDts = str(fileName)
        # Set the directory path where raw data file is stored, and store in a variable:
        if any(x in str(fileName) for x in ['(', ')']):
            getfileEvaDts.executed = False
            return print(f"{fileName} is a duplicate file")
        else:
            getfileEvaDts.executed = True
            None
    except IOError:
        print(f"Source directory or EVA-DTS file not found at {fileName}")
    
    # File processing: Get lines from file and store them in variable
    # Loop through a file to copy line by line until end of line:
    with open(fileEvaDts, 'r') as openFile:
        # Initialize working variables "listContent" for storing extracted contents into list:
        lineReader = openFile.readlines()
        charDelimiter = ''
        listContent = []

    # Define a function to process and extract the data in the content line:
    def extBlueRedfile(delimiter, line):
        """
        This function processes and extract data in the content line from a BlueRed file.
        It finds the positions of all '*' delimiters, stores them in list. These positions
        are used as follow: 
        - To generate a subfix, concatenate it to the code prefix, then store it as dictionary key.
        - To index line to find values and store them as dictionary value.
        All created dictionaries are concatenated to a list.
        """
        # Get items in the content line seperated by delimiter, store them in a list:
        itemsList = line.split(delimiter)
        # Set initial values for working variables:
        numPosition = len(itemsList)
        items = numPosition - 1
        position = numPosition - items
        tempDictionary = []
        tempValue = []
        tempList = []
        # Index and find items in the content line, and load them into a list:
        while items > 0:
            # Checking index number before concatenate to EVA-DTS code
            if position < 10:
                tempDictionary = {itemsList[0] + '0' + str(position) : itemsList[position].strip()}
            else:
                tempDictionary = {itemsList[0] + str(position+1) : itemsList[position].strip()}
            # Do not append dictionary into list if value is not empty
            tempValue = list(tempDictionary.values())
            if tempValue[0] != '':
                tempList.append(tempDictionary)
            else:
                None
            items -= 1
            position = numPosition - items
        return tempList

    # Use 'extBlueRedfile()' function to process a file, line by line, store data into list:
    for row in lineReader: # for loop method to iterate through a file
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

    ###################################
    # Transformation processing...... #
    ###################################
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
    # Note: This file must be placed the same folder as this program
    # Open and read the EVA-DTS fact file's contents:
    try:
        fileMaster = cwd + '//EVADTS_MAST.csv'
        if os.name == 'nt':
            fileMaster = Path(fileMaster)
        else:
            None
    except:
        print(f"Source directory or EVA-DTS description file not found at {fileMaster}")

    # Use pandas 'merge()' method to join dataframes: 'dfBlueRed' and 'dfEvaCode', into a dataframe:
    dfEvaCode = pd.read_csv(fileMaster, header=0)
    dfBlueRed = dfEvaFile.merge(dfEvaCode, how='left', left_on='Code', right_on='EVA-DTS code')
    # Drop unwanted columns, 'Line' and 'EVA-DTS code':
    dfBlueRed = dfBlueRed.drop(['Line', 'EVA-DTS code'], axis=1)

    # Get vending machine serial number, and append to a column:
    # Initialize variable as string
    valMacCode = ''
    # Find row equal to 'ID106' or 'ID101' in 'Code' column to get value at 'Values' column
    listMacCode = dfBlueRed['Values'].values[dfBlueRed['Code'] == 'ID106']
    if listMacCode != '':
        valMacCode = str(int(listMacCode))
    else:
        listMacCode = dfBlueRed['Values'].values[dfBlueRed['Code'] == 'ID101']
        for char in range(len(listMacCode[0])):
            if listMacCode[0][char].isdigit():
                valMacCode += listMacCode[0][char]
            else:
                None
        valMacCode = str(int(valMacCode))

    # Append serial number to new column 'VM_serial' in dataframe
    dfBlueRed['VM_serial'] = pd.DataFrame({'VM_serial' : [valMacCode]*len(dfBlueRed)})

    # Find row equal to 'EA302' + 'EA303' in 'Code' column to get value at 'Values' column:
    valDate = str(int(str(20) + dfBlueRed['Values'].values[dfBlueRed['Code'] == 'EA302']))
    valTime = str(int(dfBlueRed['Values'].values[dfBlueRed['Code'] == 'EA303']))
    # Check if value in variable 'valTime' has 6 digits, HHMMSS:
    if len(valTime) < 6:
        valTime = '0' + valTime
    else:
        None

    # Concaternate datetime and store in a variable, and append to column 'Retrieve_date' in dataframe:
    valDatetime = pd.to_datetime(valDate + ' ' + valTime, format='%Y%m%d %H:%M:%S')
    dfBlueRed['Retrieve_date'] = pd.DataFrame({'Retrieve_date' : [valDatetime]*len(dfBlueRed)})

    # Get filename and append to column 'File_name' in dataframe:
    dfBlueRed['File_name'] = pd.DataFrame({'File_name' : [fileName]*len(dfBlueRed)})

    # Reorder the columns in dataframe:
    colReorder = ['Retrieve_date', 'File_name', 'VM_serial', 'Code', 'Description', 'Values']
    dfBlueRed = dfBlueRed.loc[:, colReorder]

    # Write transformed dataframe to a CSV file:
    global writeFile # Set local variable to global to allow use at other functions
    newFileName = valMacCode + '-' + valDate + valTime + '.csv'
    writeFile = cwd + '//data' + '//' + newFileName
    if os.name == 'nt':
        writeFile = Path(writeFile)
    else:
        None
    return dfBlueRed.to_csv(writeFile)

def showFile(fileName):
    if getfileEvaDts.executed:
        print(f"Parsing file ... {fileName} ...")
        print(f"Output CSV file ... {writeFile} ...")
    else:
        None
    return
