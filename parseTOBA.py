import re
import os
import sys
import yaml
import struct
import argparse
import datetime
import numpy as np
import pandas as pd
from dataclasses import dataclass,field

# Requires a binary TOB3 or ascii TOA5 file from Campbell Scientific logger
# self.modes:
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
    pth = os.path.join(c,'config_files','defaultMetadata.yml')
    with open(pth,'r') as f:
        defaults = yaml.safe_load(f)
    return(defaults)
@dataclass
class Metadata:
    log: bool = False
    verbose: bool = False
    mode: int = 1
    Metadata: dict = field(default_factory=load)
    Contents: dict = field(default_factory=dict)

class parseTOBA(Metadata):
    def __init__(self,**kwds):
        super().__init__(**kwds)
        self.types = ['TOB3','TOA5']
    
    def parse(self,file,saveTo=None,timezone=None,clip=None):
        if not file.endswith('.dat'):
            self.mode = 0
            return 
        self.f = open(file,'rb')
        self.getType(timezone)
        if self.mode >= 1:
            self.readHead()
        if self.mode >= 2:
            if self.Metadata['Type'] != 'TOA5':
                self.Data, self.Timestamp = self.readFrames()
                print(self.Data)
            else:
                self.Data = pd.read_csv(self.f,header=None)
                d = np.where(self.Header.columns=='TIMESTAMP')[0][0]
                self.Timestamp = np.array([d.timestamp() for d in pd.to_datetime(self.Data[d],format='ISO8601')])
                self.Data = self.Data[self.Data.columns[self.Data.columns!=d]]
                excl = self.Header.columns[0]
                self.Header = self.Header[self.Header.columns[self.Header.columns!=excl]]     
                self.frequency = round(self.Timestamp[-1] - self.Timestamp[0])/self.Timestamp.shape[0]
                # if self.frequency >= 1:
                self.Metadata['Frequency'] = f"{self.frequency}s"
                # else:
                #     self.Metadata['Frequency'] = f"{int(self.frequency/pd.Timedelta('1ms').total_seconds())}ms"
        else:
            self.Data= None
        self.f.close()
        if self.Data is not None:
            if clip is not None:
                self.Data = self.Data[:clip]
                self.Timestamp = self.Timestamp[:clip]
            if self.mode == 3:
                self.Data = pd.DataFrame(self.Data)
                self.Data.columns = self.Header.columns
                self.Timestamp = pd.to_datetime(self.Timestamp,unit='s')
                self.Data.index=self.Timestamp.round(self.Metadata['Frequency'])
                self.Data.index.name = 'TIMESTAMP'
                if saveTo is not None:
                    self.Data.columns = pd.MultiIndex.from_arrays([self.Header.columns,self.Header.iloc[0],self.Header.iloc[1]],names=[None,None,None])
                    self.Data = self.Data.reset_index()
                    self.Data.columns = pd.MultiIndex.from_tuples([
                        ('TIMESTAMP', 'TS', '') if col == ('TIMESTAMP', '', '') else col
                        for col in self.Data.columns])
                    Preamble = '"'+'","'.join(self.Preamble[0][:-1]+self.Preamble[1][:1])+'"\n'
                    file = file.split('.dat')[-0]+'_'+self.Metadata['timestamp'].strftime('%Y_%m_%d_%H%M')+'.dat'
                    fileOut = os.path.join(saveTo,os.path.split(file)[-1])
                    print('Converted ',file,' to ',fileOut)
                    with open(fileOut,'w',newline='') as f:
                        f.write(Preamble)
                        self.Data.to_csv(f,index=False)
            log = ['Read',file]
        else:
            log = ['Not Read',file]
        if log:
            return(log)
        
    def getType(self,timezone):
        self.Preamble = self.parseLine(self.f.readline())
        if self.Preamble[0] not in self.types:
            self.mode = 0
        else:
            self.Metadata['Type']=self.Preamble[0]
            self.Metadata['StationName']=self.Preamble[1]
            self.Metadata['LoggerModel']=self.Preamble[2]
            self.Metadata['SerialNo']=self.Preamble[3]
            if self.Metadata['StationName'] == self.Metadata['SerialNo']:
                self.Metadata['StationName'] = None
            self.Metadata['Program']=self.Preamble[-3].split(':')[-1].rsplit('.')[0]
            while self.Metadata['Program'].endswith('_str'):
                self.Metadata['Program'] = self.Metadata['Program'].removesuffix('_str')
            if self.Preamble[0] == 'TOA5':
                search = r'([0-9]{4}\_[0-9]{2}\_[0-9]{2}\_[0-9]{4})'
                fmt = '%Y_%m_%d_%H%M'
                f = os.path.split(self.f.name)[-1]
                self.Metadata['Timestamp'] = pd.to_datetime(datetime.datetime.strptime(
                    re.search(search, f.rsplit('.',1)[0]).group(0),'%Y_%m_%d_%H%M'))
                self.Metadata['Table'] = self.Preamble[-1]
            else:
                self.Metadata['Timestamp'] = pd.to_datetime(self.Preamble[-1])
                self.Preamble = [self.Preamble,self.parseLine(self.f.readline())]
                self.Metadata['Table'] = self.Preamble[1][0]
                # self.Metadata['Frequency'] = self.parseFreq(self.Preamble[1][1])
                # Used for reading
                self.frequency = pd.to_timedelta(self.parseFreq(self.Preamble[1][1])).total_seconds()
                self.Metadata['Frequency'] = str(self.frequency)+'s'
                self.frameSize = int(self.Preamble[1][2])
                self.val_stamp = int(self.Preamble[1][4])        
                self.frameTime = pd.to_timedelta(self.parseFreq(self.Preamble[1][5])).total_seconds()
            self.Metadata['Timezone'] = timezone
                
    
    def parseLine(self,line):
        return(line.decode('ascii').strip().replace('"','').split(','))
    
    def parseFreq(self,text):
        def split_digit(s):
            match = re.search(r"\d", s)
            if match:
                s = s[match.start():]
            return s 
        freqDict = {'MSEC':'ms','Usec':'us','Sec':'s','HR':'h','MIN':'min'}
        subDict = {'SecUsec':'Sec1Usec','SecMsec':'Sec1Msec'}
        freq = split_digit(text)
        for key,value in freqDict.items():
            freq = re.sub(key.lower(), value, freq, flags=re.IGNORECASE)
        freq = freq.replace(' ','')
        return(freq)
    
    def readHead(self):
        columns = self.parseLine(self.f.readline())
        if self.Metadata['Type'] == 'TOA5':
            ix = ['unit_in','operation']
        else:
            ix = ['unit_in','operation','dataType']
        data = [self.parseLine(self.f.readline()) for _ in ix]
        self.Header = pd.DataFrame(columns = columns,data = data,index=ix)
        self.Contents = self.Header.to_dict()
        if self.Metadata['Type'] != 'TOA5':
            self.FP2 = np.where(self.Header.loc['dataType']=='FP2')[0]
            dtype_map_struct = {"IEEE4B": "f","IEEE8B": "d","FP2": "H"}
            self.byteMap = ''.join([dtype_map_struct[val['dataType']] for val in self.Contents.values()])
        dtype_map_numpy = {"IEEE4B": "float32","IEEE8B": "float64","FP2": "float16"}
        for key,value in self.Contents.items():
            if value['dataType'] in dtype_map_numpy.keys():
                self.Contents[key]['dataType'] = dtype_map_numpy[value['dataType']]
                self.Contents[key]['ignore'] = self.Contents[key]['dataType'] != 'float32'
        self.startTime = self.Metadata['Timestamp'].timestamp()        
        self.Metadata['Timestamp'] = self.Metadata['Timestamp'].strftime('%Y-%m-%dT%H%M')

    def readFrames(self):
        Header_size = 12
        Footer_size = 4
        record_size = struct.calcsize('>'+self.byteMap)
        records_per_frame = int((self.frameSize-Header_size-Footer_size)/record_size)
        self.byteMap_Body = '>'+''.join([self.byteMap for r in range(records_per_frame)])
        i = 0
        Timestamp = []
        campbellBaseTime = pd.to_datetime('1990-01-01').timestamp()
        readFrame = True
        while readFrame:         
            sb = self.f.read(self.frameSize)
            if len(sb)!=0:
                Header = sb[:Header_size]
                Header = np.array(struct.unpack('LLL', Header))
                Footer = sb[-Footer_size:]
                Footer = struct.unpack('L', Footer)[0]
                flag_e = (0x00002000 & Footer) >> 14
                flag_m = (0x00004000 & Footer) >> 15
                footer_validation = (0xFFFF0000 & Footer) >> 16

                time_1 = (Header[0]+Header[1]*self.frameTime+campbellBaseTime)
                if footer_validation == self.val_stamp and flag_e != 1 and flag_m != 1:
                    Timestamp.append([time_1+i*self.frequency for i in range(records_per_frame)])
                    Body = sb[Header_size:-Footer_size]
                    Body = struct.unpack(self.byteMap_Body, Body)
                    if self.FP2.shape[0]>0:
                        Body = self.decode_fp2(Body)
                    if i == 0:
                        data = np.array(Body).reshape(-1,len(self.byteMap))
                    else:
                        data = np.concatenate((data,np.array(Body).reshape(-1,len(self.byteMap))),axis=0)
                    i += 1
                else:
                    readFrame = False
            else:
                readFrame = False
        print('Frames ',i)
        if i > 0:
            return (data,np.array(Timestamp).flatten())
        else:
            return (None,None)

    def decode_fp2(self,Body):
        # adapted from: https://github.com/ansell/camp2ascii/tree/cea750fb721df3d3ccc69fe7780b372d20a8160d
        def FP2_map(int):
            sign = (0x8000 & int) >> 15
            exponent =  (0x6000 & int) >> 13 
            mantissa = (0x1FFF & int)       
            if exponent == 0: 
                Fresult=mantissa
            elif exponent == 1:
                Fresult=mantissa*1e-1
            elif exponent == 2:
                Fresult=mantissa*1e-2
            else:
                Fresult=mantissa*1e-3

            if sign != 0:
                Fresult*=-1
            return Fresult
        FP2_ix = [m.start() for m in re.finditer('H', self.byteMap_Body.replace('>','').replace('<',''))]
        Body = list(Body)
        for ix in FP2_ix:
            Body[ix] = FP2_map(Body[ix])
        return(Body)

# If called from command line ...
if __name__ == '__main__':
    
    CLI=argparse.ArgumentParser()
    
    CLI.add_argument(
        "--file", 
        nargs="?",
        type=str,
        default=None,
        )    
    CLI.add_argument(
        "--self.mode", 
        nargs="?",
        type=int,
        default=1,
        )    
    CLI.add_argument(
        "--timezone", 
        nargs="?",
        type=str,
        default=None,
        )    
    args = CLI.parse_args()
    parseTOB3(args.file,args.self.mode,args.timezone)