# Python 3.9
# Coding: utf-8
# Version: 2.0
# REMOVE ALL lines of code used for timing this process in the final production
from functools import wraps # Import wraps function from functools library
from time import time # Import time function from time library
def timer(function):
    """
    This decorator function measures how long it takes to execute of a function.
    Usage: add '@timer', a line before the function.
    """
    @wraps(function)
    def wrapper(*args, **kwargs):
        funcName = function.__name__
        startTime = time()
        value = function(*args, **kwargs)
        endTime = time()
        if (endTime - startTime) < 1:
            print(f"Function <{funcName}> execution took {(endTime - startTime) * 1000:.2f} ms ...")
        else:
            print(f"Function <{funcName}> execution took {(endTime - startTime):.2f} s ...")
        return value
    return wrapper

@timer # Decorator to time this function
def main():
    """
    To parse and convert EVA-DTS files then export as CSV files with naming convention 
    for new CSV file as Machine serial code + retrieved date and time. 
    E.g. 1234567-YYYYMMDDHHMMSS.csv
    """
    # Import modules to get all files in a directory:
    import os, sys
    from pathlib import Path
    # Import sys module to append current working directory for using evaFileHanlder module:
    if os.name == 'nt':
        sys.path.append(Path(os.getcwd() + '//EVADTS_processing'))
    else:
        sys.path.append(os.getcwd() + '//EVADTS_processing')
    # Import usr-defined modules:
    import evadtsFileHandler as ed # File handler to process EVA-DTS formatted file to CSV file

    # Get a list of all files from current working directory:
    cwd = os.getcwd() + '//EVADTS_processing' + '//sampledata'
    listDirectory = os.listdir(cwd)
    # Loop X times to parse X files in a directory:
    files = 0 # Initialize file counter that count how many files were converted
    for fname in listDirectory: # Index all files listed inside a directory
        fileName = cwd + '//' + fname
        if os.name == 'nt':
            fileName = Path(fileName)
        else:
            None
        ed.getfileEvaDts(fileName) # Call function in module executes file converison
        ed.showFile(fileName) # Call function in module executes display output of filename
        # If file conversion executed, add 1 to counter
        if ed.getfileEvaDts.executed:
            files += 1
        else:
            None
    print(f"{files} EVA-DTS files converted to CSV files ...")
        
if __name__ == '__main__':
    main() # Call funcion to execute program
