
from .defaults import * 
import datetime
import os



        
@dataclass(kw_only=True)
class asciiHeader(genericLoggerFile):
    fileObject: object = field(default=None,repr=False)
    header: list = field(default_factory=lambda:[],repr=False)
    StationName: str = None
    LoggerModel: str = None
    SerialNo: str = None
    program: str = None
    Table: str = None
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
                
        else:
            return(f"filetype: {self.fileType} not supported")
        
    def parseLine(self,line):
        return(line.strip().replace('"','').split(','))

    def parseFreq(self,text):
        #Parse a measurement frequency from a TOB3 string input to a format compatible with pandas datetime
        def split_digit(s):
            match = re.search(r"\d", s)
            if match:
                s = s[match.start():]
            return s 
        freqDict = {'MSEC':'ms','Usec':'us','Sec':'s','HR':'h','MIN':'min'}
        freq = split_digit(text)
        for key,value in freqDict.items():
            freq = re.sub(key.lower(), value, freq, flags=re.IGNORECASE)
        freq = freq.replace(' ','')
        return(freq)

@dataclass(kw_only=True)
class read(asciiHeader):
    sourceFile: str
    timestampName: str = field(default="TIMESTAMP",repr=False)
    header: list = field(default_factory=lambda:{})
    readData: bool = field(default=True,repr=False)
    Data: pd.DataFrame = field(default_factory=pd.DataFrame,repr=False)
    calcStats: list = field(default_factory=lambda:[],repr=False)

    def __post_init__(self):
        with open(self.sourceFile) as self.fileObject:
            super().__post_init__()
        self.Data = pd.read_csv(self.sourceFile,header=None,skiprows=4)
        self.Data.columns = list(self.variableMap.keys())
        self.Data = self.Data.set_index(pd.to_datetime(self.Data[self.timestampName],format='ISO8601'))
        self.Data = self.Data.drop(columns=[self.timestampName])
            