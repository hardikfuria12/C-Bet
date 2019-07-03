import time
import os
import constants as const

def untar_folders():
    root_directory=const.DATA_DIR
    data_folders=os.listdir(root_directory)
    for folder in data_folders:
        if folder == ".DS_Store":
            continue
        folder_path=root_directory+'/'+folder
        folder_tar_list=os.listdir(folder_path)
        for tar_file in folder_tar_list:
            if tar_file==".DS_Store":
                continue
            tar_path=folder_path+'/'+tar_file
            print(tar_path)
            os.system('tar xopf '+tar_path+' -C '+const.UNTAR_DIR)

def unzip_files():
    root_directory=const.BZ2_DIR
    list_of_events=os.listdir(root_directory)
    flag=False
    for event_id in list_of_events:
        if event_id ==".DS_Store":
            continue
        event_folder=root_directory+'/'+event_id
        event_folder_path=os.listdir(event_folder)
        for event_files in event_folder_path:
            if ".bz2" in event_files:
                file_path=event_folder+'/'+event_files
                os.system('bzip2 -d '+file_path)
                if flag==False:
                    print(event_id, ' has bz2 files ',file_path)
                    flag=True
        flag=False


def main():
    untar_folders()
    unzip_files()


if __name__ == '__main__':
    main()