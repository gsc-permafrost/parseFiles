# from . import defaults
from .defaults import * 
import dateutil.parser as dateParse 

@dataclass(kw_only=True)
class read(genericLoggerFile):
    timestampName: str = field(default="Date Time",repr=False)
    yearfirst: bool = field(default=True,repr=False)
    statusCols: list = field(default_factory=lambda:['Host Connected', 'Stopped', 'End Of File'],repr=False)

    def __post_init__(self):
        rawFile = open(self.sourceFile,'r',encoding='utf-8-sig')
        T = rawFile.readline().rstrip('\n')
        if not T.startswith('"Plot Title: '):
            self.fileType = False
        self.Data = pd.read_csv(rawFile)
        # ID the timestamp and parse to datetime index
        self.Data.index = self.Data[
            self.Data.columns[self.Data.columns.str.contains(self.timestampName)].values
            ].apply(' '.join, axis=1).apply(dateParse.parse,yearfirst=self.yearfirst)
        self.statusCols = self.Data.columns[self.Data.columns.str.contains('|'.join(self.statusCols))].values
        self.Data[self.statusCols] = self.Data[self.statusCols].ffill(limit=1)
        keep = pd.isna(self.Data[self.statusCols]).all(axis=1)
        self.fileTimestamp = pd.to_datetime(self.Data.index[(self.Data[self.statusCols].isna()==False).any(axis=1).values].values[-1])
        self.Data = self.Data.loc[keep].copy()
        super().__post_init__()
        self.applySafeNames()
