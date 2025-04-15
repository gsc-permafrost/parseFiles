# from . import defaults
from .baseMethods import * 

@dataclass(kw_only=True)
class genericCSV(genericLoggerFile):
    timestampName: str #= "Date Time, GMT+00:00"
    timestampFormat: str #= "%y/%m/%d %H:%M:%S"
    skiprows: int = 0
    statusCols: list = field(default_factory=lambda:['Host Connected', 'Stopped', 'End Of File'],repr=False)

    def __post_init__(self):
        rawFile = open(self.sourceFile,'r',encoding='utf-8-sig')
        T = []
        for i in range(self.skiprows):
            T .append(rawFile.readline().rstrip('\n'))

        self.DataFrame = pd.read_csv(rawFile)
        # ID the timestamp and parse to datetime index
        if type(self.timestampName) is str:
            A = [c==self.timestampName for c in self.DataFrame.columns]
            B = self.DataFrame.columns.str.contains(self.timestampName)
            cix = self.DataFrame.columns[[a if a else b for a,b in zip(A,B)]]
            self.DataFrame.index = pd.to_datetime(self.DataFrame['Date Time, GMT+00:00'],format=self.timestampFormat)
        elif type(self.timestampName) is list:
            cix = [False for i in range(len(self.DataFrame.columns))]
            for t in self.timestampName:
                cix = [True if cix[i] or self.DataFrame.columns[i] == t else False
                       for i in range(len(self.DataFrame.columns))]
            self.DataFrame.index = self.DataFrame[self.DataFrame.columns[cix]].apply(' '.join, axis=1)
            self.DataFrame.index = pd.to_datetime(self.DataFrame.index,format=self.timestampFormat)
            self.timestampName = ' '.join(self.DataFrame.columns[cix])
        self.fileTimestamp = pd.to_datetime(self.DataFrame.index[-1])
        
        super().__post_init__()
        self.applySafeNames()

@dataclass(kw_only=True)
class hoboCSV(genericCSV):
    timestampName: str = "Date Time"
    timestampFormat: str = "%y/%m/%d %H:%M:%S"
    skiprows: int = 1
    statusCols: list = field(default_factory=lambda:['Host_Connected', 'Stopped', 'End_Of_File'],repr=False)

    def __post_init__(self):
        super().__post_init__()        
        self.statusCols = self.DataFrame.columns[self.DataFrame.columns.str.contains('|'.join(self.statusCols))].values
        self.fileTimestamp = pd.to_datetime(self.DataFrame.index[(self.DataFrame[self.statusCols].isna()==False).any(axis=1).values].values[-1])