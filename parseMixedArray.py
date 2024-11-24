import os
import re
import sys
import yaml
import numpy as np
import pandas as pd
import dateutil.parser as dateParse

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

class parseMixedArray():
    def __init__(self,log=False):
        self.log=log
        self.types = ['TOB3','TOA5']
        c = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(c,'config_files','defaultMetadata_mixedArray.yml'),'r') as f:
            self.Metadata = yaml.safe_load(f)
            self.Metadata['Type'] = 'MixedArray'


    def parse(self,file,DEF=None,mode=1,saveTo=None,timezone=None,clip=None):
        self.isMixedArray = False
        self.mode = mode
        self.Metadata['Timezone'] = timezone
        # Search file's directory for DEF file if not provided
        if DEF is None:
            d = os.path.split(file)[0]
            DEF = [f for f in os.listdir(d) if f.endswith('DEF')]
            if len(DEF)!=1:
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
            if self.mode >= 2:
                self.getData()
                if self.mode == 3:
                    for arrID in self.Arrays.keys():
                        self.Arrays[arrID]['Data'] = pd.DataFrame(
                            data=self.Arrays[arrID]['Data'],
                            columns=list(self.Metadata['Array'][arrID]['Header'].keys()),
                            index = pd.to_datetime(self.Arrays[arrID]['Timestamp'],unit='s'))
                        for key ,value in self.Metadata['Array'][arrID]['Header'].items():
                            self.Arrays[arrID]['Data'][key] = self.Arrays[arrID]['Data'][key].astype(value['dataType'])

    def getTables(self):
        self.Arrays = {}
        arrID = -1
        for l in self.DEF:
            k = 'Program:'
            if k in l:
                self.Metadata['Program'] = l.split(k)[-1].rstrip('-\n').lstrip()
            k = 'Wiring for'
            if k in l:
                self.Metadata['LoggerModel'] = l.split(k)[-1].rstrip('-\n').lstrip()
            if 'Output_Table' in l:
                self.isMixedArray = True
                l = l.replace('  ',' ').split(' ')
                arrID = int(l[0])
                frequency = self.parseFreq(f"{l[2]} {l[3]}")
                self.Arrays[arrID] = {}
                self.Arrays[arrID]['Data'] = []
                self.Metadata['Array'][arrID] = self.Metadata['Array']['default'].copy()
                self.Metadata['Array'][arrID]['Frequency'] = frequency
            elif arrID >-0 and l == '\n':

                arrID = -1
            elif arrID > -1:
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
                self.Metadata['Array'][arrID]['Header'][name] = self.Metadata['Array'][arrID]['Header']['default'].copy()
                self.Metadata['Array'][arrID]['Header'][name]['operation'] = operation
                self.Metadata['Array'][arrID]['Header'][name]['dataType'] = dataType
        self.Metadata['Array'].pop('default')
        for arrID in self.Metadata['Array'].keys():
            self.Metadata['Array'][arrID]['Header'].pop('default')

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
        return(freq)
        
    def getData(self):
        for row in self.MA:
            for arrID in self.Arrays.keys():
                if int(row[0]) == arrID:
                    for i in range(len(self.Metadata['Array'][arrID]['Header'].keys())-len(row)):
                        row.append(np.nan)
                    self.Arrays[arrID]['Data'].append(row)
        for arrID in self.Arrays.keys():
            self.Arrays[arrID]['Data'] = np.array(self.Arrays[arrID]['Data'],dtype='float32')
            self.parseDate()
                
    def parseDate(self):
        # Assumes Year_RMT,Day_RMT,Hour_Minute_RMT as is the default output from shortcut
        for arrID in self.Arrays.keys():
            if sum([k in list(self.Metadata['Array'][arrID]['Header'].keys()) for k in ['Year_RTM', 'Day_RTM', 'Hour_Minute_RTM']]) != 3:
                sys.exit('Timestamp format currently not supported.  Should ba a simple fix')
            Date = self.Arrays[arrID]['Data'][:,1:3].astype(int).astype(str)
            Date = [' '.join(D) for D in self.Arrays[arrID]['Data'][:,1:3].astype(int).astype(str)]
            Date = pd.to_datetime(Date,format = '%Y %j')

            Time = self.Arrays[arrID]['Data'][:,3].astype(int).astype(str)
            Time = [t.zfill(4) for t in Time]
            Time = [t[0:2]+':'+t[2:]+':'+'00' for t in Time]
            Time = pd.to_timedelta(Time)
            self.Arrays[arrID]['Timestamp'] = np.array([x.timestamp() for x in Date+Time])