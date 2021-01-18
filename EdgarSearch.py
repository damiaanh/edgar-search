"""Edgar 10k MD&A Extractor
Usage:
    EdgarSearch.py download [options]
    EdgarSearch.py mdatool [options] --keywords=keyword1,keyword2,keyword_with_spaces
    EdgarSearch.py 10ktool [options] --keywords=keyword1,keyword2,keyword_with_spaces

Options:
    --index-dir=<file>          Directory to save index files [default: ./Edgar/Index]
    --index-10k-path=<file>     CSV file to store 10K indices [default: ./Edgar/Index/index.10k.csv]
    --10k-dir=<file>            Directory for the 10K files [default: ./Edgar/10K]
    --10k-keyword-path=<file>   CSV file to store 10k keyword count [default: ./Edgar/10k_keywords.csv]
    --mda-dir=<file>            Directory for the MD&A files [default: ./Edgar/MDA]
    --mda-keyword-path=<file>   CSV file to store MD&A keyword count [default: ./Edgar/keywords.csv]
    --year-start=<int>          Starting year for download index [default: 1994]
    --year-end=<int>            Ending year for download index [default: 1994]
    --keywords=<string>         Keyword(s) to count in MD&A [default: profit]
"""
#Imports
import csv
import itertools
import os
import re
import time
import random

import unicodedata
from collections import namedtuple
from glob import glob

import requests
from bs4 import BeautifulSoup
from docopt import docopt

#Url for downloading the files
SEC_GOV_URL = 'https://www.sec.gov/Archives'
FORM_INDEX_URL = (SEC_GOV_URL+'/'+'edgar'+'/'+'full-index'+'/'+'{}'+'/'+'QTR{}'+'/'+'form.idx')
IndexRecord = namedtuple(
    "IndexRecord", ["form_type", "company_name", "cik", "date_filed", "filename",'filed_date'])

def parse_row_to_record(row, fields_begin, year):
    #Parses a row to a record
    record = []
    for begin, end in zip(fields_begin[:], fields_begin[1:] + [len(row)]):
        field = row[begin:end].rstrip()
        field = field.strip('\"')
        record.append(field)
    record.append('0')
    faod = record[3]
    record[5] = faod
    record[3] = year
    return record

def download_and_extract_index(opt):
    #Downloads and extracts the index files
    index_dir = opt["--index-dir"]
    if not os.path.exists(index_dir):
        os.makedirs(index_dir)

    year_start = int(opt["--year-start"])
    year_end = int(opt["--year-end"])

    for year, qtr in itertools.product(range(year_start, year_end+1), range(1, 5)):

        form_idx = "{}_qtr{}.index".format(year, qtr)
        form_idx_path = os.path.join(index_dir, form_idx)
        index_url = FORM_INDEX_URL.format(year, qtr)
        file_error = 0
        if not os.path.exists(form_idx_path):
            if year == 2011 and qtr == 4 :
                print('[*] 2011 quarter 4 backup not found, please move it from /Edgar/Index/Backup to /Edgar/Index/')
                file_error += 1
            elif year == 2017 and qtr == 3 :
                print('[*] 2017 quarter 3 backup not found, please move it from /Edgar/Index/Backup to /Edgar/Index/!')
                file_error += 1
            #if file_error == 1:
                exit(0)
            if file_error == 2:
                print("Both included files not found, please move them from /Edgar/Index/Backup to /Edgar/Index/")
                exit(0)
            try:
                print("[*] Downloading file : {}".format(index_url))
                res = requests.get(index_url,timeout=10)
                if res.status_code != requests.codes.OK:
                    raise Exception
                with open(form_idx_path, 'w') as fout:
                    txt = res.text
                    fout.write(txt)

            except(ConnectionError, requests.RequestException) as e:
                print('[!] Download failed for - {} Connection error!'.format(index_url) + str(e))
            except:
                print("[!] Download failed - {}".format(index_url))
        else:
        # The server won't send two specific files, they are included with the script for completion
            if year == 2011 and qtr == 4 :
                print('[*] Found 2011 quarter 4 backup!')
            elif year == 2017 and qtr == 3 :
                print('[*] Foud 2017 quarter 3 backup!')
            else :
                print('[*] Already got file {} Not downloading again'.format(form_idx_path))

    print("[i] Finished downloading the index files")

    records = []
    for index_file in sorted(glob(os.path.join(index_dir, "*.index"))):
        print("[i] Extracting 10K filings from {}".format(index_file))

        with open(index_file, 'r',encoding = "ISO-8859-1") as fin:
            arrived = False
            spacing = opt["--index-dir"]
            start = 1 + len(spacing)
            CYEAR = index_file[start:start + 4]
            for row in fin.readlines():
                if row.startswith("Form Type"):
                    fields_begin = [row.find("Form Type"),
                                    row.find("Company Name"),
                                    row.find('CIK'),
                                    row.find('Date Filed'),
                                    row.find("File Name")]

                elif row.startswith("10-K "):
                    arrived = True
                    rec = parse_row_to_record(row, fields_begin, CYEAR)
                    records.append(IndexRecord(*rec))

                elif arrived == True:
                    break

    index_10k_path = opt["--index-10k-path"]
    with open(index_10k_path, 'w') as fout:
        writer = csv.writer(fout, delimiter=',',
                            quotechar='\"', quoting=csv.QUOTE_ALL)
        for rec in records:
            writer.writerow(tuple(rec))

def download_10k(opt):
    #Downloads the 10K Filings
    print("[i] Downloading the 10K filings")
    index_10k_path = opt["--index-10k-path"]
    if not (os.path.exists(index_10k_path)):
        print("[i] File {} doesn't exist, something went wrong!".format(index_10k_path))
    form10k_dir = opt["--10k-dir"]
    if not os.path.exists(form10k_dir):
        os.makedirs(form10k_dir)
    count = 0
    with open(index_10k_path, 'r') as fin:
        reader = csv.reader(
            fin, delimiter=',')
        for row in reader:
            if(row and row != None):
                CIK = row[2]
                YEAR = row[3]
                FILEDASOFDATE = row[5]
                COMPANY_NAME = row[1].replace(" ", '').replace("/",'')
                _, _, _, _, filename, _ = row
                fname = COMPANY_NAME + "_" + CIK +"_"+ YEAR + "_"+FILEDASOFDATE+".txt"
                text_path = os.path.join(form10k_dir,fname)
                if not os.path.exists(text_path):
                    url = os.path.join(SEC_GOV_URL, filename).replace("\\", "/")
                    try:
                        res = requests.get(url,timeout=10)
                        soup = BeautifulSoup(res.content, 'lxml')
                        text = soup.get_text("\n")
                        text2 = re.sub('[^a-zA-Z0-9.,:\n\t\b]',' ',text)
                        with open(text_path, 'w') as fout:
                            fout.write(text2)
                        #Sleeping between 0.5 and 2 seconds to prevent being detected as a bot by the server.
                        anti_bot = random.uniform(0.5, 2)
                        time.sleep(anti_bot)

                    except Exception as e:
                        print("[!] Couldn't download 10K file - {} - {}".format(url, e))
                else:
                    pass

def extract_mda(opt, keyword_list):
    #Extracts the MDA from the 10K Filings and searches for keywords in the MDA's
    print("[i] Extracting MDA's")
    form10k_dir = opt["--10k-dir"]
    assert os.path.exists(form10k_dir)
    mda_dir = opt["--mda-dir"]
    if not os.path.exists(mda_dir):
        os.makedirs(mda_dir)
    records = []
    records.append(['NAME','CIK','YEAR', 'FILEDASOFDATE','TOTALWORDS'])
    for keyword in keyword_list:
        records[0].append(keyword.upper())
    print("[i] CSV columns :" + str(records))
    for form10k_file in sorted(glob(os.path.join(form10k_dir, "*.txt"))):
        path_info = form10k_file.split("_")
        YEAR = path_info[2]
        NAME = path_info[0][len(form10k_dir) +1:]
        CIK = path_info[1]
        filedasofdate = path_info[3][:-4]
        row = [NAME,CIK,YEAR,'0','0']
        # Read form 10k
        with open(form10k_file, 'r') as fin:
            text = fin.read()
        # Normalize
        text = normalize_text(text)

        # Find the MDA section
        mda, end = parse_mda(text)
        # Parse for a second time if needed
        if mda and len(mda.encode('utf-8')) < 1000:
            mda, _ = parse_mda(text, start=end)

        if mda:
            #If the MDA was found, we'll search for the supplied keywords and the MDA will be extracted to a new file
            filename = os.path.basename(form10k_file)
            name, ext = os.path.splitext(filename)
            mda_name = (name + ".mda")
            mda_path = os.path.join(opt["--mda-dir"], mda_name)
            if not os.path.exists(mda_path):
                with open(mda_path, 'w') as fout:
                    fout.write(mda)
            wordcount = []
            num_keywords = 0
            for keyword in keyword_list:
                num_keywords += 1
                if " " in keyword:
                    wordcount.append(count_words_sentence(mda.lower(), keyword))
                else:
                    wordcount.append(count_words(mda.lower(), keyword))
            totalwords = mda.split()
            totalcount = 0
            for word in totalwords:
                totalcount += 1
            row[4] = totalcount
            row[3] = filedasofdate
            for i in range(0,num_keywords):
                row.append(wordcount[i])
            records.append(row)
    print("[!] Extracted all MDA's!")
    data_path = opt["--mda-keyword-path"]
    with open(data_path, 'w') as fout:
        writer = csv.writer(fout, delimiter=',',
                            quotechar='\"', quoting=csv.QUOTE_ALL)
        for rec in records:
            writer.writerow(tuple(rec))
    print("[*] All done! Keyword files can be found here: " + data_path)

def wordcount_10k(opt, keyword_list):
    #Searches for keywords in the 10K filings
    form10k_dir = opt["--10k-dir"]
    if(os.path.exists(form10k_dir)):
        print("[i] Folder {} already exists! Overwriting!".format(form10k_dir))
    records = []
    records.append(['NAME','CIK','YEAR', 'FILEDASOFDATE','TOTALWORDS'])
    for keyword in keyword_list:
        records[0].append(keyword.upper())
    print("[i] CSV columns :" + str(records))
    for form10k_file in sorted(glob(os.path.join(form10k_dir, "*.txt"))):
        path_info = form10k_file.split("_")
        YEAR = path_info[2]
        NAME = path_info[0][len(form10k_dir) +1:]
        CIK = path_info[1]
        filedasofdate = path_info[3][:-4]
        row = [NAME,CIK,YEAR,'0','0']

        with open(form10k_file, 'r') as fin:
            text = fin.read()

        text = normalize_text(text)
        filename = os.path.basename(form10k_file)
        name, ext = os.path.splitext(filename)
        wordcount = []
        num_keywords = 0
        for keyword in keyword_list:
            num_keywords += 1
            if " " in keyword:
                wordcount.append(count_words_sentence(text.lower(), keyword))
            else:
                wordcount.append(count_words(text.lower(), keyword))
        totalwords = text.split()
        totalcount = 0
        for word in totalwords:
            totalcount += 1
        row[4] = totalcount
        row[3] = filedasofdate
        for i in range(0,num_keywords):
            row.append(wordcount[i])
        records.append(row)

    print("[!] Extracted all MDA's!")
    data_path = opt["--10k-keyword-path"]
    with open(data_path, 'w') as fout:
        writer = csv.writer(fout, delimiter=',',
                            quotechar='\"', quoting=csv.QUOTE_ALL)
        for rec in records:
            writer.writerow(tuple(rec))
    print("[*] All done! Keyword files can be found here: " + data_path)

def normalize_text(text):
    #normalizing the text
    text = unicodedata.normalize("NFKD", text)  # Normalize
    text = '\n'.join(
        text.splitlines())  # Let python take care of unicode break lines

    # Convert to uppercase
    text = text.upper()  # Convert to upper

    # Removing linebreaks and whitespaces
    text = re.sub(r'[ ]+\n', '\n', text)
    text = re.sub(r'\n[ ]+', '\n', text)
    text = re.sub(r'\n+', '\n', text)

    # To find MDA section, reformat item headers
    text = text.replace('\n.\n', '.\n')

    text = text.replace('\nI\nTEM', '\nITEM')
    text = text.replace('\nITEM\n', '\nITEM ')
    text = text.replace('\nITEM  ', '\nITEM ')

    text = text.replace(':\n', '.\n')

    # Replace symbols
    text = text.replace('$\n', '$')
    text = text.replace('\n%', '%')

    # Reformat
    text = text.replace('\n', '\n\n')

    return text

def count_words(text, keyword):
    #Counts keywords in text and returns count
    count = 0
    text = text.split()
    for word in text:
        word.lower()
        if word == keyword:
            count += 1
    return count

def count_words_sentence(text, keyword):
    #Counts keywords with spaces and returns the count
    count = 0
    text = text.replace("\n", ".")
    text = text.split('.')
    for line in text:
        if keyword in line:
            count += 1
    return count


def parse_mda(text, start=0):
    #Parses the MDA section
    mda = ""
    end = 0

    item7_starts = ['\nITEM 7.', '\nITEM 7 –', '\nITEM 7:', '\nITEM 7 ', '\nITEM 7\n']
    item7_ends = ['\nITEM 7A']
    if start != 0:
        item7_ends.append('\nITEM 7')
    item8_starts = ['\nITEM 8']
    text = text[start:]

    # Get start
    for item7 in item7_starts:
        start = text.find(item7)
        if start != -1:
            break

    if start != -1:
        for item7A in item7_ends:
            end = text.find(item7A, start + 1)
            if end != -1:
                break

        if end == -1:
            for item8 in item8_starts:
                end = text.find(item8, start + 1)
                if end != -1:
                    break

        if end > start:
            mda = text[start:end].strip()
        else:
            end = 0
    return mda, end


if __name__ == "__main__":
    #Main function
    opt = docopt(__doc__)
    print('[*] Welcome! ')
    keyword_arguments = opt["--keywords"].lower()
    keywords = keyword_arguments.replace("_"," ").split(',')
    keyword_list = []
    for keyword in keywords:
        keyword_list.append(keyword)
    if opt["download"]:
        #Download all files
        print("[i] Downloading all files, this might take a few days!")
        download_and_extract_index(opt)
        download_10k(opt)
        print("[*] Downloads complete!")
    elif opt["mdatool"]:
        #Extract mda and search for keywords
        extract_mda(opt, keyword_list)
    elif opt["10ktool"]:
        #Search for keywords in the entire 10K filings
        wordcount_10k(opt, keyword_list)
    print("[*] Goodbye!")
