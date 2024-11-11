
import os
import sys
import pandas as pd

def read_log_book(path):
    """read the log book and corrects ending of the wrongly labeled file names"""
    log = pd.read_csv(path)
    # Apply the function to specific columns
    log['File'] = log['File'].apply(change_ending, args=('.cdx', '.cxd'))
    log['Dark Correction '] = log['Dark Correction '].apply(change_ending, args=('.cdx', '.cxd'))
    
    return log

# Function to change the ending of the values
def change_ending(value, old_ending, new_ending):
        if pd.notna(value) and value.endswith(old_ending):
            return value[:-len(old_ending)] + new_ending
        return value

#function which checks if name is found in the log file
def check_name_in_log(name, log):
    if name in log['File'].values:
        return True
    else:
        return False



#function which checks if name is found in row of pd dataset
def check_name_in_row(name, row):
    if name in row['File'].values:
        return True
    else:
        return False

def get_cxd2do(args, log):
    #check data_path and create a list of all files ending with .cxd
    files = [f for f in os.listdir(args.data_path) if f.endswith('.cxd')]
    #check data_path and create a list of all files ending with .cxi
    files_done = [f for f in os.listdir(args.data_path) if f.endswith('.cxi')]
    files_done_cxd = [f[:-4] + ".cxd" for f in files_done]
    #create a list of files that still need to be processed
    files_to_do = list(set(files) - set(files_done_cxd))
    #exclude the flatfield file from the list
    files_to_do = list(set(files_to_do) - set(["_flatfield01624.cxd", "data01624.cxd"]))
    #order the files according to the file number
    files_to_do.sort()
    #find each file in the log file and check if the value in the first column is equal to "background"
    bg_files = []
    exclude_files = []

    for file in files_to_do:
        try:
            row = log.loc[log['File']== file]                 #find row of file
            description = row['Description'].values[0]        #extract background file name
            if description == "background":
                bg_files.append(file)
               #extract analysis comment
            
        except:
            print(f"ERROR: File {file} not found in log file. Maybe {args.log_file} is outdated.")
            sys.exit(-1)
        
        try:
            analysis_comment = str(row['data analysis'].values[0])
            if analysis_comment is not "":
                    if "exclude" in analysis_comment:
                        exclude_files.append(file)
        except Exception as e:
            print(f"ERROR: Problem with analysis comment. {e}")
            sys.exit(-1)
    
    #substract background- and exclude files from files_to_do
    files_to_do = list(set(files_to_do) - set(bg_files) - set(exclude_files))

    print(f"Number of files to process: {len(files_to_do)}")
    print("found these files to process:")
    print(files_to_do)
    return files_to_do



def get_cxi2do(args, log):
    filenames = [f for f in os.listdir(args.directory) if f.endswith(".cxi")]

    if args.start_number is not None:
        files_to_do = [filename for filename in filenames if int(args.start_number) <= int(filename[4:9])]

    if args.end_number is not None:
        files_to_do = [filename for filename in filenames if int(args.end_number) >= int(filename[4:9])]

    filenames.sort()

    return filenames