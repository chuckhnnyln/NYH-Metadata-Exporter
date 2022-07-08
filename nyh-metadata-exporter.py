#!/usr/bin/env python3
import requests
import sys
import csv
import codecs
import os
import PySimpleGUI as sg
import queue
import threading
import time
import pandas as pd
import io
import configparser

#GUI Global Settings
sg.theme('LightGreen4')
TitleFont = ('Arial', 14, 'bold')
Font = ('Arial', 12, 'normal')
Pad = (15,15)

def GUICouncilSelect():
    global TitleFont, Font, Pad, TitleVersion
    CouncilList = ["CDLC", "CLRC", "LILRC", "NNYLN", "RRLC", "SCRLC", "SENYLRC", "WNYLRC"]
    CouncilLayout = [   [sg.Text(TitleVersion, font=(TitleFont))],
                        [sg.Text('Step 1: Choose a Council')],
                        [sg.Listbox(CouncilList, select_mode = 'LISTBOX_SELECT_MODE_SINGLE',\
                            size=(10, 8),\
                            default_values = 'NNYLN')],
                        [sg.Button('Select', pad=(Pad)), sg.Button('Quit')]  ]
    window = sg.Window('NYH Metadata Exporter', CouncilLayout, font=(Font))
    SelectedCouncil = ""
    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED or event == 'Quit':
            sys.exit()
        if event == 'Select':
            Delist = values.get(0)
            SelectedCouncil = Delist[0]
            break
    window.close()
    return SelectedCouncil

def GUIAliasSelect(TargetAliasList):
    global TitleFont, Font, TitleVersion
    AliasSelectLayout = [   [sg.Text('Step 2: Which alias would you like a data dump from? ')],
                            [sg.Listbox(TargetAliasList, select_mode = 'multiple',\
                                size=(15, 8))],
                            [sg.Button('Select', pad=(Pad)), sg.Button('Quit')]  ]
    window2 = sg.Window(TitleVersion, AliasSelectLayout, font=(Font))
    while True:
        event, values = window2.read()
        if event == sg.WIN_CLOSED or event == 'Quit':
            sys.exit()
            break
        if event == 'Select':
            DeDict = values[0]
            if DeDict[0] == 'ALL':
                TargetAliasList.pop(0)
                SelectedAliasList = TargetAliasList
                NamingHint = 'ALL'
            else:
                SelectedAliasList = DeDict
                NamingHint = 'selected'
            break
    window2.close()
    return SelectedAliasList, NamingHint

def GUISaveLocSelect(SelectedCouncil, NamingHint):
    global TitleFont, Font, TitleVersion
    FolderBrowseLayout = [  [sg.Text("Step 3: Choose a save folder for the exports.")],\
                            [sg.Input(key="-IN2-" ,change_submits=True, size=(25,1)), sg.FolderBrowse(key="-IN-", pad=(Pad))],
                            [sg.Button("Submit", pad=(Pad)), sg.Button('Quit')]  ]
    window3 = sg.Window(TitleVersion, FolderBrowseLayout, font=(Font))
    while True:
        event, values = window3.read()
        if event == sg.WIN_CLOSED or event == 'Quit':
            sys.exit()
        if event == 'Submit':
            DeDict = values['-IN2-']
            if DeDict != "":
                LocalPath = DeDict
                break

    LogFile = os.path.join(LocalPath, SelectedCouncil + '_' + NamingHint + '_log.txt')
    FileName = os.path.join(LocalPath, SelectedCouncil + '_' + NamingHint + '.tsv')
    MinFileName = os.path.join(LocalPath, SelectedCouncil + '_' + NamingHint + '_minimize.tsv')

    if os.path.isfile(LogFile):
        os.remove(LogFile)
    if os.path.isfile(FileName):
        os.remove(FileName)
    if os.path.isfile(MinFileName):
        os.remove(MinFileName)

    window3.close()
    return LogFile, FileName, MinFileName

def GetAliasList(SelectedCouncil):
    BailOut = 0
    while True:
        BailOut = BailOut + 1
        NYHDataUrl = "https://nyheritage.org/metadata/coll-descrip-data.csv"
        Response = requests.get(NYHDataUrl, timeout=5)
        if Response.status_code == 200:
            FullAliasList = csv.reader(codecs.iterdecode(Response.iter_lines(),\
                                'utf-8'), delimiter=',', quotechar='"')
            FilterAliasList = []
            for row in FullAliasList:
                if row[0] == SelectedCouncil:
                    FilterAliasList.append(row[1])

            TargetAliasList = [ i for n, i in enumerate(FilterAliasList) if i not in FilterAliasList[:n]]
            TargetAliasList.sort()
            TargetAliasList.insert(0, 'ALL')
            break
        if BailOut == 10:
            sys.exit()
        time.sleep(1)
    return TargetAliasList

def LogIt(LogFile, Message):
    f = open(LogFile, "a")
    f.write(Message)
    f.close()

def CreateFullExport(work_id, gui_queue):
    global SelectedAliasList, FileName, LogFile
    nan_value = float("NaN")
    TotalDF = pd.DataFrame()
    Progress = 0
    Username, Password = ReadConfig()
    for Alias in SelectedAliasList:
        time.sleep(3)
        ExportUrl = 'https://server16694.contentdm.oclc.org/cgi-bin/admin/getfile.exe?CISOMODE=1&CISOFILE=/' + Alias + '/index/description/export.txt'
        Response = requests.get(ExportUrl, auth = (Username, Password), timeout = 5)
        if Response.status_code == 200:
            ResponseContent = Response.content
            try:
                AliasDF = pd.read_csv(io.StringIO(ResponseContent.decode('utf-8')), sep = '\t')
            except pd.errors.ParserError:
                Message = Alias + ": Pandas parser error!\n"
                LogIt(LogFile, Message)
            except UnicodeDecodeError:
                Message = Alias + ": UnicodeDecodeError!\n"
                LogIt(LogFile, Message)
            except Exception:
                Message = Alias + ": Unknown error. Ask Chuck!\n"
                LogIt(LogFile, Message)
            else:
                AliasDF.replace("", nan_value, inplace = True)
                AliasDF.dropna(how = 'all', axis = 1, inplace = True)
                try:
                    if AliasDF.columns[0] == "Title":
                        TotalDF = pd.concat([TotalDF, AliasDF], sort = False)
                    else:
                        Message = Alias + ": Error -> No header row detected. (Try regenerating export from CDM admin)\n"
                        LogIt(LogFile, Message)
                except:
                    Message = Alias + ": Error -> Problem reading column data. (Try regenerating export from CDM admin)\n"
                    LogIt(LogFile, Message)
        if Response.status_code == 404:
            Message = Alias + ": Alias not found (404).\n"
        Progress = Progress + 1
        gui_queue.put(Progress)
    if "Description" in TotalDF.columns:
        TotalDF.drop('Description', inplace=True, axis=1)
    if "Transcript" in TotalDF.columns:
        TotalDF.drop('Transcript', inplace=True, axis=1)
    if TotalDF.empty:
        Message = Alias + ": No data to write to TSV.\n"
        LogIt(LogFile, Message)
    else:
        TotalDF.to_csv(FileName, sep='\t', index=False)
    gui_queue.put('Done!')

def GUIPleaseWait(AliasCount):
    global TitleFont, Font, gui_queue, TitleVersion
    DisplayMessage = "Please wait. This will take some time. There are " + str(AliasCount) + " aliases to process."
    PleaseWaitLayout = [    [sg.Text(DisplayMessage)],
                            [sg.ProgressBar(100, orientation = "h",\
                            size = (50,20), border_width = 0,\
                            key='-PROGRESS_BAR-', bar_color=("Pink","Yellow"))],
                            [sg.Button('Cancel')]  ]
    window5 = sg.Window(TitleVersion, PleaseWaitLayout, font=(Font))
    Message = 0
    while True:
        event, values = window5.Read(timeout=100)
        if event == sg.WIN_CLOSED or event == 'Cancel' or Message == "Done!":
            window5.close()
            sys.exit()
        try:
            Message = gui_queue.get_nowait()
        except queue.Empty:
            Message = 0
        else:
            if Message != "Done!":
                PercentComplete = int(round((int(Message)) * 100 / AliasCount))
                print(f'{Message} : {PercentComplete}')
                window5['-PROGRESS_BAR-'].update(PercentComplete)

def ReadConfig():  
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),'NYH-auth.cfg'))
    Username = config.get('auth', 'username')
    Password = config.get('auth', 'password')
    return Username, Password

if __name__ == "__main__":
    TitleVersion = "NYH Metadata Exporter V1.2"
    SelectedCouncil = GUICouncilSelect() #Choose a council
    AliasList = GetAliasList(SelectedCouncil) #Get the collection alias for that council
    SelectedAliasList, NamingHint = GUIAliasSelect(AliasList) #Choose alias or alias to work on
    LogFile, FileName, MinFileName = GUISaveLocSelect(SelectedCouncil, NamingHint) #Choose where to put things
    AliasCount = len(SelectedAliasList)
    gui_queue = queue.Queue()
    work_id = 0
    thread_id = threading.Thread(target=CreateFullExport, args=(work_id, gui_queue,), daemon=True)
    thread_id.start()
    GUIPleaseWait(AliasCount)
