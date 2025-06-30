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
                    'unit':unit,
                    'variableDescription':aggregation
                    } 
                    for column,unit,aggregation in zip(
                        self.parseLine(self.fileObject.readline()),
                        self.parseLine(self.fileObject.readline()),
                        self.parseLine(self.fileObject.readline()),
                    )},self.variableMap,overwrite=True)
            f = os.path.split(self.fileObject.name)[-1]
            self.fileTimestamp = pd.to_datetime(datetime.datetime.strptime(re.search(r'([0-9]{4}\_[0-9]{2}\_[0-9]{2}\_[0-9]{4})', f.rsplit('.',1)[0]).group(0),'%Y_%m_%d_%H%M'))
        elif self.fileType == 'TOB3':
            self.fileTimestamp = pd.to_datetime(self.preamble[-1])
            self.preamble = self.parseLine(self.fileObject.readline())
            self.Table = self.preamble[0]
            self.frequency = f"{pd.to_timedelta(parseFrequency(self.preamble[1])).total_seconds()}s"
            self.frameSize = int(self.preamble[2])
            self.val_stamp = int(self.preamble[4])        
            self.frameTime = pd.to_timedelta(parseFrequency(self.preamble[5])).total_seconds()
            self.variableMap = updateDict(
                {column:{
                    'unit':unit,
                    'variableDescription':aggregation,
                    'dtype':dtype
                    }
                    for column,unit,aggregation,dtype in zip(
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

    def __post_init__(self):
        super().__post_init__()
        with open(self.sourceFile,'rb') as self.fileObject:
            self.parseHeader()
            self.readFrames()
            self.standardize()
        self.fileObject.close()
            
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
        frequency = float(self.frequency.rstrip('s'))
        while readFrame:         
            sb = self.fileObject.read(self.frameSize)
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
                    Timestamp.append([time_1+i*frequency for i in range(records_per_frame)])
                    Body = sb[Header_size:-Footer_size]
                    Body = struct.unpack(self.byteMap_Body, Body)
                    if 'H' in self.byteMap_Body:
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
        if self.verbose:
            print(f'Frames {i}')
        if i > 0:
            self.toDataFrame(data,np.array(Timestamp).flatten())
        # else:
        #     return (None,None)

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

    def toDataFrame(self,data,timestamp):
        self.DataFrame = pd.DataFrame(
            data = data,
            index=pd.to_datetime(timestamp,unit='s').round(self.frequency)
            )
        self.DataFrame.columns = [var for var in self.variableMap]
        self.DataFrame.index.name = 'TIMESTAMP'
        if self.summaryStats != []:
            Agg = {}
            for column in self.variableMap:
                Agg[column] = self.DataFrame[column].agg(self.summaryStats)
            self.DataFrame = pd.DataFrame(
                index=[self.DataFrame.index[-1]],
                data = {f"{col}_{agg}":val 
                        for col in Agg.keys() 
                        for agg,val in Agg[col].items()})

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
                self.ArrayDefs['Table'][arrID]['variableMap'][name]['unit'] = name.replace('_'+operation,'')
                self.ArrayDefs['Table'][arrID]['variableMap'][name]['variableDescription'] = operation
        rowCT = {}
        TOA5_string = {}
        for arrID in self.ArrayDefs['Table']:
            rowCT[arrID] = 1
            # Arbitrary junk header
            TOA5_string[arrID] = f'"TOA5","","{self.ArrayDefs['Model']}","","","{self.ArrayDefs['Program']}","","{arrID}"\n'
            row = ['"TIMESTAMP"','"RECORD"']
            for i,name in enumerate(self.ArrayDefs['Table'][arrID]['variableMap']):
                if i>3:
                    row.append(f'"{name}"')
            TOA5_string[arrID] = TOA5_string[arrID] + ','.join(row) + '\n'
            row = ['"TS"','"RN"']
            for i,name in enumerate(self.ArrayDefs['Table'][arrID]['variableMap'].values()):
                if i>3:
                    row.append(f'"{name['unit']}"')
            TOA5_string[arrID] = TOA5_string[arrID] + ','.join(row) + '\n'
            row = ['","','"SMP"']
            for i,name in enumerate(self.ArrayDefs['Table'][arrID]['variableMap'].values()):
                if i>3:
                    row.append(f'"{name['variableDescription']}"')
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
            sourceFile = self.DAT_file.split('.')[0] + f'_ArrayID{name}_{datetime.datetime.now().strftime('%Y_%m_%d_%H%M')}.dat'
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
            