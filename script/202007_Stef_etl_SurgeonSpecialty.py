#!/usr/bin/env python
# coding: utf-8

import os
import re
import pandas as pd

def dict_name_path():
    '''
    get files in data/data_raw
    return a dic_name of the files name
    '''
    try: 
        # Get the absolute path of parent directory 
        parent = os.path.dirname(os.getcwd()) 
        # Get the absolute path of parent input files and output  files
        data_raw_path = os.path.join(parent,'data', 'data_raw')
        data_path = os.path.join(parent,'data')
        # list of files 
        files = [f for f in os.listdir(data_raw_path) if os.path.isfile(os.path.join(data_raw_path, f))]
        dic_name = {}
    except Exception as e:
        print(e)
    else:
        for f in files:
            name = re.split('\W+',f)[0]
            path_in = os.path.join(data_raw_path,f)
            path_out = os.path.join(data_path,f)
            dic_name[name] = [path_in, path_out]
        return(dic_name)

def surgeon(motif, dic_name):
    '''
    get Surgeon.csv file
    return dataframe
    '''
    if motif in dic_name.keys():
        try:
            path_in = dic_name[motif][0]
            path_out = dic_name[motif][1]
            name_file = motif
        except Exception as e:
            print(e)
        else:
            df_surgeon_raw = pd.read_csv(path_in)
            colDropSurgeon = ['Copy Confirmation', 'Email', 'User ID', 'Deleted']
            df_dropSurgeon = df_surgeon_raw.drop(colDropSurgeon, axis=1)
            return(df_dropSurgeon)
    
def request(motif, dic_name):
    '''
    get Request.csv file
    return dataframe
    '''
    if motif in dic_name.keys():
        try:
            path_in = dic_name[motif][0]
            path_out = dic_name[motif][1]
            name_file = motif
        except Exception as e:
            print(e)
        else:        
            df_request_raw = pd.read_csv(path_in, low_memory=False)
            colDropSurgeon = colDropReq = ['Archived By Operating Block', 'Ch Iru Rg Ical Type', 'Chosen Response ID', \
                'Comment', 'First Document Shared Name', 'ID Patient', 'Instrument Type', 'Laboratory Set Is Here', \
                'Laboratory Set Stay Here', 'Message Recovery', 'Need Operating Help',  'Ready', 'Receipt', 'Reason ID', \
                'Regular Iz Ation Number', 'Specialty Ch Iru Rg Ical ID', 'Specialty ID', 'State', 'Take Back', \
                'Type', 'Date', 'Date', 'Date Receipt', 'Date Take Back', 'Deleted Date', 'End Date', 'Expected Delivery Date', \
                'First Operation Date','Last Operation Date', 'Recovery Date', 'Deleted' , 'Recovery Date Limit', 'Reservation ID', \
                'Relaunched', 'Contract ID', 'Date Ready', 'Laboratory Set ID']
            df_dropRequest = df_request_raw.drop(colDropReq, axis=1)
            return(df_dropRequest)

def laboratory(motif, dic_name):
    '''
    get Laboratory.csv
    '''
    if motif in dic_name.keys():
        try:
            path_in = dic_name[motif][0]
            path_out = dic_name[motif][1]
            name_file = motif
        except Exception as e:
            print(e)
        else:
            df_lab = pd.read_csv(path_in)
            thresh = len(df_lab) * .1
            df_drop = df_lab.dropna(thresh = thresh, axis = 1, how='all')
            colDropLab = ['Ad R1', 'Connected', 'Mail', 'Non Member', 'Telephone', 'Non Dis Po' ]
            df_dropLab = df_drop.drop(colDropLab, axis=1)
            return(df_dropLab)

def SurgeonSpeciality(df_dropRequest, df_dropSurgeon, df_dropLab):
    '''
    Join 3 df and saved in SurgeonSpecilaity.csv
    '''
    try:
        df_requestJoin = df_dropRequest.merge(df_dropSurgeon, how='left', left_on='Surgeon Entity ID', right_on='ID')
        df_requestJoin.drop('ID_y', inplace=True, axis=1)
        df_chirTurnOver = df_requestJoin.merge(df_dropLab, how='left', left_on='Chosen Laboratory ID', right_on='ID')
        df_chirTurnOver.drop('ID', inplace=True, axis=1)
        thresh = len(df_chirTurnOver) * .1
        df_SurgeonSpeciality = df_chirTurnOver.dropna(thresh = thresh, axis = 1, how='all')
    except Exception as e:
        print(e)
    else:
        print(f'The SurgeonSpeciality shape is {df_SurgeonSpeciality.shape}, must be 36431 and 16')
        path_out = os.path.join(os.path.dirname(os.getcwd()), 'data', 'data_created')
        df_SurgeonSpeciality.to_csv(path_out+'/SurgeonSpeciality.csv')
        return()
        

def main():
    
    mylist = ['Surgeon', 'Request', 'Laboratory']    
    dic_name = dict_name_path()
    for motif in mylist:
        if motif == 'Surgeon':
            df_dropSurgeon = surgeon(motif, dic_name)
        if motif =='Request':
            df_dropRequest = request(motif, dic_name)
        if motif =='Laboratory':
            df_dropLab = laboratory(motif, dic_name)
    SurgeonSpeciality(df_dropRequest, df_dropSurgeon, df_dropLab)
    

if __name__ == "__main__":
    main()
    print("File SurgeonSpeciality.csv is loaded")