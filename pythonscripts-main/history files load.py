import os
import shutil
import glob


src_folder = r"C:\Users\gr\hist files back up\\" #source directory

dst_folder = r"C:\Users\gr\tpi_history\\"  #destination directory

# files_to_move = 'Purchase Ledger Reconciliation - Power.xlsm'

# move file whose name starts with string 'emp'
pattern = src_folder + "2021\*\*.xlsm"
counter=0
for file in glob.iglob(pattern, recursive=True):
    # extract file name form file path
    counter=counter+1
    file_name = os.path.basename(file)
    shutil.copy(file, dst_folder +'src\\'+ '2021_'+str(counter)+'_'+file_name)
    print(file, 'Copied:', '2021_'+str(counter)+'_'+file_name)
