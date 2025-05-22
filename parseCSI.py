try:
    # relative import for use as submodules
    from .helperFunctions.updateDict import updateDict
    from .baseMethods import * 
except:
    # absolute import for use as standalone
    from helperFunctions.updateDict import updateDict
    from baseMethods import * 
import datetime
import struct
import os

@dataclass(kw_only=True)
class asciiHeader(genericLoggerFile):
    fileObject: object = field(default=None,repr=False)
    header: list = field(default_factory=lambda:[],repr=False)
    fileType: str = None
    StationName: str = None
    LoggerModel: str = None
    SerialNo: str = None
    program: str = None
    frequency: str = None
    Table: str = None
    variableMap: dict = field(default_factory=lambda:{})
    byteMap: str = None

    def __post_init__(self):
        header = self.parseLine(self.fileObject.readline())
        self.fileType = header[0]
        self.Type=header[0]
        self.StationName=header[1]
        self.LoggerModel=header[2]
        self.SerialNo=header[3]
        if self.fileType == 'TOA5':
            self.Table = header[-1]
            self.variableMap = {column:{'unit':unit,
                                        'variableDescription':aggregation
                                        } for column,unit,aggregation in zip(
                                            self.parseLine(self.fileObject.readline()),
                                            self.parseLine(self.fileObject.readline()),
                                            self.parseLine(self.fileObject.readline()),
                                            )}
            f = os.path.split(self.fileObject.name)[-1]
            self.fileTimestamp = pd.to_datetime(datetime.datetime.strptime(re.search(r'([0-9]{4}\_[0-9]{2}\_[0-9]{2}\_[0-9]{4})', f.rsplit('.',1)[0]).group(0),'%Y_%m_%d_%H%M'))
        elif self.fileType == 'TOB3':
            self.fileTimestamp = pd.to_datetime(header[-1])
            header = self.parseLine(self.fileObject.readline())
            self.Table = header[0]
            self.frequency = f"{pd.to_timedelta(self.parseFreq(header[1])).total_seconds()}s"
            self.frameSize = int(header[2])
            self.val_stamp = int(header[4])        
            self.frameTime = pd.to_timedelta(self.parseFreq(header[5])).total_seconds()
            self.variableMap = updateDict({column:{'unit':unit,
                            'variableDescription':aggregation,
                            'dtype':dtype
                            } for column,unit,aggregation,dtype in zip(
                                self.parseLine(self.fileObject.readline()),
                                self.parseLine(self.fileObject.readline()),
                                self.parseLine(self.fileObject.readline()),
                                self.parseLine(self.fileObject.readline()),
                                )},
                                self.variableMap
            )
            dtype_map_struct = {"IEEE4B": "f","IEEE8B": "d","FP2": "H"}
            self.byteMap = ''.join([dtype_map_struct[var['dtype']] for var in self.variableMap.values()])
            self.DataFrame = pd.DataFrame(columns=list(self.variableMap.keys()))    
        else:
            return(f"filetype: {self.fileType} not supported")
        
    def parseLine(self,line):
        if type(line) == str:
            return(line.strip().replace('"','').split(','))
        else:
            return(line.decode('ascii').strip().replace('"','').split(','))

@dataclass(kw_only=True)
class parseTOA5(asciiHeader):
    # sourceFile: str
    header: list = field(default_factory=lambda:{})
    readData: bool = field(default=True,repr=False)
    Data: pd.DataFrame = field(default_factory=pd.DataFrame,repr=False)
    calcStats: list = field(default_factory=lambda:[],repr=False)

    def __post_init__(self):
        with open(self.sourceFile) as self.fileObject:
            super().__post_init__()
        self.DataFrame = pd.read_csv(self.sourceFile,header=None,skiprows=4)
        self.DataFrame.columns = list(self.variableMap.keys())
        self.DataFrame = self.DataFrame.set_index(pd.to_datetime(self.DataFrame[self.timestampName],format='ISO8601'))
        self.DataFrame = self.DataFrame.drop(columns=[self.timestampName])
        genericLoggerFile.__post_init__(self)


@dataclass(kw_only=True)
class parseTOB3(asciiHeader):
    writeBinary: bool = False
    header: list = field(default_factory=lambda:{})
    calcStats: list = field(default_factory=lambda:[],repr=False)

    def __post_init__(self):
        with open(self.sourceFile,'rb') as self.fileObject:
            super().__post_init__()
            self.readFrames()
            genericLoggerFile.__post_init__(self)
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
        if self.calcStats != []:
            Agg = {}
            for column in self.variableMap:
                Agg[column] = self.DataFrame[column].agg(self.calcStats)
            self.DataFrame = pd.DataFrame(
                index=[self.DataFrame.index[-1]],
                data = {f"{col}_{agg}":val 
                        for col in Agg.keys() 
                        for agg,val in Agg[col].items()})
