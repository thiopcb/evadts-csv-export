# Export EVA-DTS format file to CSV file
This project is created in Python 3.9. It reads EVA-DTS formatted file, and takes its delimiter to parse code ID codes and values into a pandas.Dataframe. A master file with definition of the ID codes, is read as a pandas.Dataframe then merge with EVA-DTS dataframe. Lastly, the merged dataframe is written to a CSV file for use.
