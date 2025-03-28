import os
import re
import sys
import yaml
import numpy as np
import pandas as pd
import dateutil.parser as dateParse
from dataclasses import dataclass,field



# Type: MixedArray
# Table: []
# StationName:
# Logger:
# SerialNo:
# Program:
# Frequency: 
# Timestamp:
# Timezone:
# Array:
#   default:
#     Frequency:
#     arrayContents:
#       default:
#         unit_in:
#         operation:
#         dataType:


# Requires a mixed ascii array file along with corresponding .DEF file from Campbell Scientific logger
# file: must be list [array.dat,program.def] in order to parse properly
# modes:
# 1 - Parse Metadata
# 2 - Read data and dump to numpy array
# 3 - Read data and dump to a timestamped pandas dataframe
# saveTo: self.mode must == 2, save a TOA5 file to specified directory with timestamp in name following format output by card convert
# Timezone: Optionally added to Metadata
# Clip: Optionally limit output to rows[:clip], only needed for small tables which don't fill a frame

# Key elements:
# Metadata - dict of header information
# Data - numpy array or pandas timestamped dataframe depending on self.mode
# Timestamp - numpy array in POSIX format from logger time


def load():
    c = os.path.dirname(os.path.abspath(__file__))
    pth = os.path.join(c,'config_files','defaultMetadata_mixedArray.yml')
    with open(pth,'r') as f:
        defaults = yaml.safe_load(f)
    return(defaults)

@dataclass
class Metadata:
    log: bool = False
    verbose: bool = False
    mode: int = 1
    Metadata: dict = field(default_factory=load)
    Contents: dict = field(init=False)

    def __post_init__(self):
        self.Contents = self.Metadata.pop('Array')

class parseMixedArray(Metadata):
    def __init__(self,**kwds):
        super().__init__(**kwds)

    def parse(self,file,DEF=None,saveTo=None,timezone=None,clip=None):
        if not file.endswith('.dat'):
            self.mode = 0
            return 
        self.isMixedArray = False
        self.Metadata['Timezone'] = timezone
        # Search file's directory for DEF file if not provided
        if DEF is None:
            d = os.path.split(file)[0]
            DEF = [f for f in os.listdir(d) if f.endswith('DEF')]
            if len(DEF)!=1:
                print(file)
                print('Ambiguous, cannot autodetect DEF file')
                DEF = None
            else:
                DEF = os.path.join(d,DEF[0])
        if DEF is None:
            self.mode = 0
            print('Cannot parse without DEF file')
        else:
            self.f = open(file,'r',encoding='utf-8-sig')
            self.MA = [l.rstrip('\n').split(',') for l in self.f.readlines()]
            self.f.close()
            self.f = open(DEF,'r',encoding='utf-8-sig')
            self.DEF = self.f.readlines()
            self.f.close()
            self.getTables()
            for key in self.Contents.keys():
                if self.Metadata['Frequency'] is None:
                    self.Metadata['Frequency'] = float(self.Contents[key]['Frequency'].replace('s',''))
                else:
                    self.Metadata['Frequency'] = min(self.Metadata['Frequency'],
                                    float(self.Contents[key]['Frequency'].replace('s','')))
            self.Metadata['Frequency'] = str(self.Metadata['Frequency'])+'s'
            if self.mode >= 2:
                self.getData()
                if self.mode == 3:
                    for arrID in self.Arrays.keys():
                        self.Arrays[arrID]['Data'] = pd.DataFrame(
                            data=self.Arrays[arrID]['Data'],
                            columns=list(self.Contents[arrID]['arrayContents'].keys()),
                            index = pd.to_datetime(self.Arrays[arrID]['Timestamp'],unit='s'))
                        for key ,value in self.Contents[arrID]['arrayContents'].items():
                            self.Arrays[arrID]['Data'][key] = self.Arrays[arrID]['Data'][key].astype(value['dataType'])

    def getTables(self):
        self.Arrays = {}
        arrID = '-1'
        for i,l in enumerate(self.DEF):
            if i == 0:
                self.Metadata['Timestamp']  = l.rstrip()
            elif i == 1:
                self.Metadata['Timestamp'] = self.Metadata['Timestamp'] + ' ' + l.rstrip()
                self.Metadata['Timestamp'] = pd.to_datetime(self.Metadata['Timestamp'],format='%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%dT%H%M')
            k = 'Program:'
            if k in l:
                self.Metadata['Program'] = l.split(k)[-1].rstrip('-\n').lstrip()
            k = 'Wiring for'
            if k in l:
                self.Metadata['Model'] = l.split(k)[-1].rstrip('-\n').lstrip()
            if 'Output_Table' in l:
                self.isMixedArray = True
                l = l.replace('  ',' ').split(' ')
                arrID = l[0]
                frequency = pd.to_timedelta(self.parseFreq(f"{l[2]} {l[3]}")).total_seconds()
                self.Metadata['Table'].append(str(arrID))
                self.Arrays[arrID] = {}
                self.Arrays[arrID]['Data'] = []
                self.Contents[arrID] = self.Contents['default'].copy()
                self.Contents[arrID]['Frequency'] = str(frequency)+'s'
            elif arrID != '-1' and l == '\n':

                arrID = '-1'
            elif arrID != '-1' :
                l = l.rstrip('\n').split(' ')
                operation = 'Smp'
                if l[0] == '1':
                    name = 'ArrayID'
                    dataType = 'int32'
                else:
                    name = l[1]
                    if len(name.split('_'))>1:
                        operation = name.split('_')[-1]
                    if operation == 'RTM':
                        dataType = 'int32'
                    else:
                        dataType = 'float32'        
                self.Contents[arrID]['arrayContents'][name] = self.Contents[arrID]['arrayContents']['default'].copy()
                self.Contents[arrID]['arrayContents'][name]['operation'] = operation
                self.Contents[arrID]['arrayContents'][name]['dataType'] = dataType
                self.Contents[arrID]['arrayContents'][name]['ignore'] = dataType == 'float32'
                    
        self.Contents.pop('default')
        for arrID in self.Contents.keys():
            self.Contents[arrID]['arrayContents'].pop('default')
        self.Metadata['Table'] = '_'.join(self.Metadata['Table'])


    def parseFreq(self,text):
        text = text.rstrip('\n')
        def split_digit(s):
            match = re.search(r"\d", s)
            if match:
                s = s[match.start():]
            return s 
        freqDict = {'MSEC':'ms','Usec':'us','Sec':'s','MIN':'min','HR':'h'}
        subDict = {'SecUsec':'Sec1Usec','SecMsec':'Sec1Msec'}
        freq = split_digit(text)
        for key,value in freqDict.items():
            freq = re.sub(key.lower(), value, freq, flags=re.IGNORECASE)
        freq = freq.replace(' ','')
        return(freq)
        
    def getData(self):
        for row in self.MA:
            for arrID in self.Arrays.keys():
                if int(row[0]) == int(arrID):
                    for i in range(len(self.Contents[arrID]['arrayContents'].keys())-len(row)):
                        row.append(np.nan)
                    self.Arrays[arrID]['Data'].append(row)
        for arrID in self.Arrays.keys():
            self.Arrays[arrID]['Data'] = np.array(self.Arrays[arrID]['Data'],dtype='float32')
            self.parseDate()
                
    def parseDate(self):
        # Assumes Year_RMT,Day_RMT,Hour_Minute_RMT as is the default output from shortcut
        for arrID in self.Arrays.keys():
            if sum([k in list(self.Contents[arrID]['arrayContents'].keys()) for k in ['Year_RTM', 'Day_RTM', 'Hour_Minute_RTM']]) != 3:
                sys.exit('Timestamp format currently not supported.  Should ba a simple fix')
            Date = self.Arrays[arrID]['Data'][:,1:3].astype(int).astype(str)
            Date = [' '.join(D) for D in self.Arrays[arrID]['Data'][:,1:3].astype(int).astype(str)]
            Date = pd.to_datetime(Date,format = '%Y %j')

            Time = self.Arrays[arrID]['Data'][:,3].astype(int).astype(str)
            Time = [t.zfill(4) for t in Time]
            Time = [t[0:2]+':'+t[2:]+':'+'00' for t in Time]
            Time = pd.to_timedelta(Time)
            self.Arrays[arrID]['Timestamp'] = np.array([x.timestamp() for x in Date+Time])