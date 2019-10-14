'''
This Python crawler developed by Chia-Hsing Pi for Hubel Lab scrapes data from clinicaltrials.gov.
Users can specify terms, country and state. 
If you have any questions, please contact Chia-Hsing Pi (pixxx021@umn.edu)

The official resources as below:
Linking to This Site
https://clinicaltrials.gov/ct2/about-site/link-to
Downloading Content for Analysis
https://clinicaltrials.gov/ct2/resources/download

Last updates: 05/14/2019
'''
from tqdm import tqdm
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import requests
import os
import io
import sys
import tkinter as tk

# Set up search parameters
term='mesenchymal+stem+cell'#Generic search
cntry=''#'US'# Country
state=''#'US%3AMN'#State = US%3A+State (eg. MN, MA...etc)

# Set up date
x = datetime.now()
DateTimeSTR = '{}{}{}'.format(
    x.year,
    str(x.month).zfill(2) if len(str(x.month)) < 2 else str(x.month),
    str(x.day).zfill(2) if len(str(x.day)) < 2 else str(x.day))

# File type selector
def filetypesSelect(filedf, fileName, filetypesStr, check):
    if 'csv' in filetypesStr:
        filedf.to_csv('{}_{}.csv'.format(check, fileName), index=False, encoding='utf-8')
    elif 'json' in filetypesStr:
        filedf.to_json('{}_{}.json'.format(check, fileName), orient="records")
    elif 'xlsx' in filetypesStr:
        writer = pd.ExcelWriter('{}_{}.xlsx'.format(check, fileName), engine='xlsxwriter',
                                options={'strings_to_urls': False})
        filedf.to_excel(writer, index=False, encoding='utf-8')
        writer.close()
    elif 'msgpack' in filetypesStr:
        filedf.to_msgpack("{}_{}.msg".format(check, fileName), encoding='utf-8')
    elif 'feather' in filetypesStr:
        filedf.to_feather('{}_{}.feather'.format(check, fileName))
    elif 'parquet' in filetypesStr:
        filedf.to_parquet('{}_{}.parquet'.format(check, fileName), engine='pyarrow', encoding='utf-8')
    elif 'pickle' in filetypesStr:
        filedf.to_pickle('{}_{}.pkl'.format(check, fileName))
########################################################################################################

# Python crawler. Scrape data from the website.
def crawler():
    strLabel = tk.Label(window, text='Processing...')
    strLabel.pack(anchor='center')
    window.update()
    global url
    global zipfileName
    global comboExample
    comboExampleget = fileTypeListbox.get(fileTypeListbox.curselection())
    req = requests.get('https://clinicaltrials.gov/ct2/results?cond=&term='+term+'&cntry='+cntry+'&state='+state+'&city=&dist=&recrs=')
    soup = BeautifulSoup(req.text, 'html5lib')
    CTDataCounts = int(''.join(list(filter(str.isdigit, soup.findAll('div', {'class': 'sr-search-terms'})[1].text))))
    strLabel2 = tk.Label(window, text='Downloads Clinical Trials Data.')
    strLabel2.pack(anchor='center')
    window.update()
    
    for n in tqdm(range(1, CTDataCounts // int(CTDataCounts*0.2)), ascii=True, desc='Downloads Data -> ', ncols=70):
        url = 'https://clinicaltrials.gov/ct2/results/download_fields?cond=&term='+term+'&cntry='+cntry+'&state='+state+'&city=&dist=down_count=100&down_flds=all&down_fmt=csv&down_chunk={}'.format(n)
        s = requests.get(url).content
        allCTData.extend(pd.read_csv(io.StringIO(s.decode('utf-8')), encoding='utf-8').to_dict('records'))
    allCTDataDF = pd.DataFrame(allCTData)
    strLabel3 = tk.Label(window, text='Downloads Clinical Trials Data Done.')
    strLabel3.pack(anchor='center')
    window.update()
    allCTDataDF = allCTDataDF.rename(
        dict(zip(allCTDataDF.columns, [i.replace(' ', '') for i in allCTDataDF.columns])),
        axis=1)
    allCTDataDF_Columns = ['NCTNumber', 'Title', 'Acronym', 'Status', 'Conditions'
        , 'Interventions', 'OutcomeMeasures', 'Sponsor/Collaborators', 'Gender', 'Age'
        , 'Phases', 'Enrollment', 'FundedBys', 'StudyType', 'StudyDesigns'
        , 'OtherIDs', 'StartDate', 'PrimaryCompletionDate', 'CompletionDate', 'FirstPosted'
        , 'ResultsFirstPosted', 'LastUpdatePosted', 'Locations', 'Rank', 'StudyDocuments'
        , 'StudyResults', 'URL']
    allCTDataCounts = 'Clinical Trials Count: {}'.format(len(allCTDataDF))
    with open('ClinicalTrials_DataCounts.txt', 'w', encoding='utf-8') as txt:
        txt.write(allCTDataCounts)
    print('Loading Clinical Trials Data to {}'.format(comboExampleget))
    strLabel4 = tk.Label(window, text='Loading Clinical Trials Data to {}'.format(comboExampleget))
    strLabel4.pack(anchor='center')
    window.update()
    
    try:
        filetypesSelect(allCTDataDF[allCTDataDF_Columns], 'ClinicalTrials_'+term, comboExampleget, DateTimeSTR)
        window.quit()
    except Exception:
        window2 = tk.Tk()
        window2.title('ERROR')
        window2.geometry('400x300')
        error_Text = ''
        e_type, e_value, e_traceback = sys.exc_info()
        error_Text += f'''Error Message：
                        Errortype ==> {e_type.__name__}
                        ErrorInfo ==> {e_value}
                        ErrorFileName ==> {e_traceback.tb_frame.f_code.co_filename}
                        ErrorLineOn ==> {e_traceback.tb_lineno}
                        ErrorFunctionName ==> {e_traceback.tb_frame.f_code.co_name}'''
        with open('errorFileLog.log', 'w+') as errorFileLog:
            errorFileLog.write(error_Text)
        strLabel2 = tk.Label(window2, text='{}\n{}\n{}'.format(e_type, e_value, e_traceback))
        strLabel2.pack(anchor='center')
        window2.mainloop()
    finally:
        pass
########################################################################################################

# Set up window for type selector
window = tk.Tk()
window.title('Select File Type')
window.geometry('400x300')
try:
    allCTData = []
    allCTDataDF = pd.DataFrame()
    path = './{}_ClinicalTrialsData'.format(DateTimeSTR)
    if not os.path.isdir(path):
        os.mkdir(path)
        os.chdir(path)
    else:
        os.chdir(path)
    fileTypeVar = tk.StringVar()
    fileTypeVar.set(('csv', 'json', 'xlsx', 'msgpack', 'feather', 'parquet', 'pickle'))
    fileTypeListbox = tk.Listbox(window, listvariable=fileTypeVar)
    fileTypeListbox.pack(anchor='center')
    saveButton = tk.Button(window, text='Save', command=crawler)
    saveButton.pack(anchor='center')
    window.mainloop()
except Exception:
    window2 = tk.Tk()
    window2.title('ERROR')
    window2.geometry('400x300')
    error_Text = ''
    e_type, e_value, e_traceback = sys.exc_info()
    error_Text += f'''Error Message：
                Errortype ==> {e_type.__name__}
                ErrorInfo ==> {e_value}
                ErrorFileName ==> {e_traceback.tb_frame.f_code.co_filename}
                ErrorLineOn ==> {e_traceback.tb_lineno}
                ErrorFunctionName ==> {e_traceback.tb_frame.f_code.co_name}'''
    with open('errorFileLog.log', 'w+') as errorFileLog:
        errorFileLog.write(error_Text)
    strLabel2 = tk.Label(window2, text='{}\n{}\n{}'.format(e_type, e_value, e_traceback))
    strLabel2.pack(anchor='center')
    window2.mainloop()
finally:
    pass