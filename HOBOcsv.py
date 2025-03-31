# from . import defaults
from .baseMethods import * 
import dateutil.parser as dateParse 

@dataclass(kw_only=True)
class read(genericLoggerFile):
    timestampName: str = "Date Time"
    yearfirst: bool = field(default=True,repr=False)
    statusCols: list = field(default_factory=lambda:['Host Connected', 'Stopped', 'End Of File'],repr=False)

    def __post_init__(self):
        rawFile = open(self.sourceFile,'r',encoding='utf-8-sig')
        T = rawFile.readline().rstrip('\n')
        if not T.startswith('"Plot Title: '):
            self.fileType = False
        self.DataFrame = pd.read_csv(rawFile)
        # ID the timestamp and parse to datetime index
        if type(self.timestampName) is str:
            self.DataFrame.index = self.DataFrame[
                self.DataFrame.columns[self.DataFrame.columns.str.contains(self.timestampName)].values
                ].apply(' '.join, axis=1).apply(dateParse.parse,yearfirst=self.yearfirst)
            self.timestampName = [self.DataFrame.columns[i] for i,c in enumerate(self.DataFrame.columns.str.contains(self.timestampName)) if c]
        elif type(self.timestampName) is list:
            cix = [False for i in range(len(self.DataFrame.columns))]
            for t in self.timestampName:
                cix = [True if cix[i] or self.DataFrame.columns[i] ==t else False
                       for i in range(len(self.DataFrame.columns))]
            self.DataFrame.index = self.DataFrame[self.DataFrame.columns[cix]].apply(' '.join, axis=1).apply(dateParse.parse,yearfirst=self.yearfirst)

        self.statusCols = self.DataFrame.columns[self.DataFrame.columns.str.contains('|'.join(self.statusCols))].values
        self.DataFrame[self.statusCols] = self.DataFrame[self.statusCols].ffill(limit=1)
        keep = pd.isna(self.DataFrame[self.statusCols]).all(axis=1)
        self.fileTimestamp = pd.to_datetime(self.DataFrame.index[(self.DataFrame[self.statusCols].isna()==False).any(axis=1).values].values[-1])
        self.DataFrame = self.DataFrame.loc[keep].copy()
        super().__post_init__()
        self.applySafeNames()
