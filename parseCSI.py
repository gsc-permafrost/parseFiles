try:
    # relative import for use as submodules
    from .baseMethods import * 
except:
    # absolute import for use as standalone
    from baseMethods import * 
import datetime
import struct
import os

@dataclass(kw_only=True)
class asciiHeader(genericLoggerFile):
    fileObject: object = field(default=None,repr=False)
    preamble: list = field(default_factory=lambda:[],repr=False)
    StationName: str = None
    LoggerModel: str = None
    SerialNo: str = None
    program: str = None
    frequency: str = None
    Table: str = None
    byteMap: str = None
    summaryStats: list = field(default_factory=lambda:[],repr=False)
    writeBinary: bool = False

    def __post_init__(self):
        super().__post_init__()

    def parseHeader(self):
        self.preamble = self.parseLine(self.fileObject.readline())
        self.fileType = self.preamble[0]
        if self.fileType != self.__class__.__name__:
            sys.exit(f"error: {__name__}.{self.__class__.__name__} does not support {self.sourceFile}")
        self.StationName=self.preamble[1]
        self.LoggerModel=self.preamble[2]
        self.SerialNo=self.preamble[3]
        if self.fileType == 'TOA5':
            self.Table = self.preamble[-1]
            self.variableMap = updateDict(
                {column:{
                    'units':units,
                    'variableDescription':aggregation
                    } 
                    for column,units,aggregation in zip(
                        self.parseLine(self.fileObject.readline()),
                        self.parseLine(self.fileObject.readline()),
                        self.parseLine(self.fileObject.readline()),
                    )},self.variableMap,overwrite=True)
            f = os.path.split(self.fileObject.name)[-1]
            self.fileTimestamp = pd.to_datetime(datetime.datetime.strptime(re.search(r'([0-9]{4}\_[0-9]{2}\_[0-9]{2}\_[0-9]{4})', f.rsplit('.',1)[0]).group(0),'%Y_%m_%d_%H%M'))
        elif self.fileType == 'TOB3':
            # Get file metadata from header to facilitate parsing
            self.fileTimestamp = pd.to_datetime(self.preamble[-1])
            self.preamble = self.parseLine(self.fileObject.readline())
            self.Table = self.preamble[0]
            self.frequency = pd.to_timedelta(parseFrequency(self.preamble[1])).total_seconds()
            self.frameSize = int(self.preamble[2])
            self.tableSize = int(self.preamble[3])
            self.val_stamp = int(self.preamble[4])    
            self.frameTime = pd.to_timedelta(parseFrequency(self.preamble[5])).total_seconds()
            self.variableMap = updateDict(
                {column:{
                    'units':units,
                    'variableDescription':aggregation,
                    'dtype':dtype
                    }
                    for column,units,aggregation,dtype in zip(
                        self.parseLine(self.fileObject.readline()),
                        self.parseLine(self.fileObject.readline()),
                        self.parseLine(self.fileObject.readline()),
                        self.parseLine(self.fileObject.readline()),
                    )},self.variableMap,overwrite=True)
            dtype_map_struct = {"IEEE4B": "f","IEEE8B": "d","FP2": "H"}
            self.byteMap = ''.join([dtype_map_struct[var['dtype']] for var in self.variableMap.values()])
            self.DataFrame = pd.DataFrame(columns=list(self.variableMap.keys())) 
        
    def parseLine(self,line):
        if type(line) == str:
            return(line.strip().replace('"','').split(','))
        else:
            return(line.decode('ascii').strip().replace('"','').split(','))

@dataclass(kw_only=True)
class TOA5(asciiHeader):

    def __post_init__(self):
        super().__post_init__()
        with open(self.sourceFile) as self.fileObject:
            self.parseHeader()
        self.DataFrame = pd.read_csv(self.sourceFile,header=None,skiprows=4)
        self.DataFrame.columns = list(self.variableMap.keys())
        self.DataFrame = self.DataFrame.set_index(pd.to_datetime(self.DataFrame[self.timestampName],format='ISO8601'))
        self.DataFrame = self.DataFrame.drop(columns=[self.timestampName])
        self.standardize()

@dataclass(kw_only=True)
class TOB3(asciiHeader):
    extract: bool = True
    campbellBaseTime: float = pd.to_datetime('1990-01-01').timestamp()

    def __post_init__(self):
        super().__post_init__()
        self.fileSize = os.path.getsize(self.sourceFile)
        with open(self.sourceFile,'rb') as self.fileObject:
            self.parseHeader()
            if self.extract:
                self.readFrames()
                self.standardize()
        self.fileObject.close()
            
    def readFrames(self):
        self.headerSize = 12
        self.footerSize = 4
        self.recordSize = struct.calcsize('>'+self.byteMap)
        self.recordsPerFrame = int((self.frameSize-self.headerSize-self.footerSize)/self.recordSize)
        nframes = int((self.fileSize-self.fileObject.tell())/self.frameSize)
        self.byteMap_Body = '>'+''.join([self.byteMap for r in range(self.recordsPerFrame)])
        
        # unpack the binary data and parse all the frames with list comprehension
        bindata = self.fileObject.read()
        frames = [
            [
                # Headers
                self.decode_header(bindata[i*self.frameSize:i*self.frameSize+self.headerSize]),
                # Body
                self.decode_body(bindata[i*self.frameSize+self.headerSize:(i+1)*self.frameSize-self.footerSize]),
                # Footers    
                self.decode_footer(bindata[(i+1)*self.frameSize-self.footerSize:(i+1)*self.frameSize])
                ]
            for i in range(nframes)]
        frames = [frame[0][i]+frame[1][i] for j,frame in enumerate(frames) for i in range(self.recordsPerFrame) if frame[2][i][0]]
        self.DataFrame = pd.DataFrame(frames,
            columns=[self.timestampName]+[var for var in self.variableMap]) 
        self.DataFrame.index = pd.to_datetime(self.DataFrame[self.timestampName],unit='s')
        self.frequency = f"{self.frequency}s"
        self.DataFrame.index = self.DataFrame.index.round(self.frequency)
        # Remove implausible timestamps???
        # self.DataFrame = self.DataFrame.loc[self.DataFrame.index<self.fileTimestamp+pd.to_timedelta(self.frequency)]
        self.typeMap = 'd'+self.byteMap.replace('H','f')
        self.typeMap = {c:self.typeMap[i] for i,c in enumerate(self.DataFrame.columns)}
        self.DataFrame = self.DataFrame.astype(self.typeMap) 
        
    def decode_header(self,Header):
        # Get the timestamp from the header
        Header = struct.unpack('LLL', Header)
        Timestamp = Header[0]+Header[1]*self.frameTime+self.campbellBaseTime
        Timestamp = [[Timestamp + i*self.frequency] for i in range(self.recordsPerFrame)]
        return(Timestamp)

    def decode_body(self,Body):
        Body = struct.unpack(self.byteMap_Body, Body)
        if 'H' in self.byteMap_Body:
            Body = self.decode_fp2(Body)
        Body = list(Body)
        npr = int(len(Body)/self.recordsPerFrame)
        Body = [Body[i*npr:(i+1)*npr] for i in range(self.recordsPerFrame)]
        return(Body)
    
    def decode_footer(self,Footer):
        # True/False flag for valid frame
        # Adapted from https://github.com/ansell/camp2ascii/blob/cea750fb721df3d3ccc69fe7780b372d20a8160d/frame_read.c#L109
        Footer = struct.unpack('L',Footer)[0]
        footerOffset     = (0x000007FF & Footer)
        footerValidation = (0xFFFF0000 & Footer) >> 16
        Footer = (footerValidation == self.val_stamp)
        # For handling partial frames
        if footerOffset > 0:
            offset = int(self.recordSize/(self.frameSize-(footerOffset+self.headerSize+self.footerSize)))
        else:
            offset = self.recordsPerFrame
        Footer = [[Footer] if i < offset else [False] for i in range(self.recordsPerFrame)]
        return(Footer)

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

@dataclass(kw_only=True)
class mixedArray():
    # Converts a mixed array to a TOA5 file for standardized processing
    DAT_file: str = field(repr=False) 
    DEF_file: str = field(repr=False)
    ArrayDefs: dict = field(default_factory=lambda: {'Timestamp': '', 'Program': '', 'Model': '', 'Table': {}}, repr=False)
    Tables: dict = field(default_factory=lambda: {}, repr=False)
    verbose: bool = field(default=False, repr=False)
    saveTOA5: bool = field(default=False, repr=False)

    def __post_init__(self):
        # read the mixed array
        f = open(self.DAT_file, 'r', encoding='utf-8-sig')
        self.MA = [l.rstrip('\n').split(',') for l in f.readlines()]
        f.close()
        # read the DEF file
        f = open(self.DEF_file, 'r', encoding='utf-8-sig')
        self.DEF = f.readlines()
        f.close()
        
        # self.Arrays = {}
        arrID = '-1'
        for i,l in enumerate(self.DEF):
            if i == 0:
                self.ArrayDefs['Timestamp']  = l.rstrip()
            elif i == 1:
                self.ArrayDefs['Timestamp'] = self.ArrayDefs['Timestamp'] + ' ' + l.rstrip()
                self.ArrayDefs['Timestamp'] = pd.to_datetime(self.ArrayDefs['Timestamp'],format='%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%dT%H%M')
            k = 'Program:'
            if k in l:
                self.ArrayDefs['Program'] = l.split(k)[-1].rstrip('-\n').lstrip()
            k = 'Wiring for'
            if k in l:
                self.ArrayDefs['Model'] = l.split(k)[-1].rstrip('-\n').lstrip()
            if 'Output_Table' in l:
                l = l.replace('  ',' ').split(' ')
                arrID = l[0]
                TD = parseFrequency(f"{l[2]} {l[3]}")
                frequency = pd.to_timedelta(TD).total_seconds()
                self.ArrayDefs['Table'][str(arrID)] = {
                    'frequency':str(frequency)+'s',
                    'variableMap':{},
                    }
            elif arrID != '-1' and l == '\n':
                arrID = '-1'
            elif arrID != '-1' :
                l = l.rstrip('\n').split(' ')
                operation = 'Smp'
                if l[0] == '1':
                    name = 'ArrayID'
                else:
                    name = l[1]
                    if len(name.split('_'))>1:
                        operation = name.split('_')[-1]
                self.ArrayDefs['Table'][arrID]['variableMap'][name] = {}
                self.ArrayDefs['Table'][arrID]['variableMap'][name]['units'] = name.replace('_'+operation,'')
                self.ArrayDefs['Table'][arrID]['variableMap'][name]['variableDescription'] = operation
        rowCT = {}
        TOA5_string = {}
        for arrID in self.ArrayDefs['Table']:
            rowCT[arrID] = 1
            # Arbitrary junk header
            TOA5_string[arrID] = f'"TOA5","","{self.ArrayDefs["Model"]}","","","{self.ArrayDefs["Program"]}","","{arrID}"\n'
            row = ['"TIMESTAMP"','"RECORD"']
            for i,name in enumerate(self.ArrayDefs['Table'][arrID]['variableMap']):
                if i>3:
                    row.append(f'"{name}"')
            TOA5_string[arrID] = TOA5_string[arrID] + ','.join(row) + '\n'
            row = ['"TS"','"RN"']
            for i,name in enumerate(self.ArrayDefs['Table'][arrID]['variableMap'].values()):
                if i>3:
                    row.append(f'"{name["units"]}"')
            TOA5_string[arrID] = TOA5_string[arrID] + ','.join(row) + '\n'
            row = ['","','"SMP"']
            for i,name in enumerate(self.ArrayDefs['Table'][arrID]['variableMap'].values()):
                if i>3:
                    row.append(f'"{name["variableDescription"]}"')
            TOA5_string[arrID] = TOA5_string[arrID] + ','.join(row) + '\n'
        for row in self.MA:
            if len(row) != len(self.ArrayDefs['Table'][row[0]]['variableMap']):
                if self.verbose:
                    print(f"Warning: Row length mismatch in mixed array for {row[0]}: {len(row)} vs {len(self.ArrayDefs['Table'][row[0]]['variableMap'])}")
                pass
            newRow = [str(s) for s in [self.parseDates(row),rowCT[arrID] ]+[float(f) for f in row[4:]]]
            TOA5_string[arrID] = TOA5_string[arrID] + ','.join(newRow) + '\n'
            rowCT[arrID] += 1
        self.dOuts = {}
        for name,file in TOA5_string.items():
            sourceFile = self.DAT_file.split('.')[0] + f'_ArrayID{name}_{datetime.datetime.now().strftime("%Y_%m_%d_%H%M")}.dat'
            with open(sourceFile,'w') as f:
                f.write(file)
            self.dOuts[name] = TOA5(sourceFile=sourceFile)
            if not self.saveTOA5:
                os.remove(sourceFile)

    def parseDates(self,row):
        Date = ' '.join([str(int(D)) for D in row[1:3]])
        Date = pd.to_datetime(Date,format = '%Y %j')
        Time = str(int(row[3])).zfill(4).ljust(6,'0')
        Time = Time[:2]+':'+Time[2:4]+':'+Time[4:]
        Time = pd.to_timedelta(Time)
        Timestamp = Date + Time
        return(Timestamp.strftime('%Y-%m-%d %H:%M:%S'))
            