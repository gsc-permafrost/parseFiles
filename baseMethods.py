import re
import os
import sys
import yaml
import numpy as np
import pandas as pd
import configparser
from dataclasses import dataclass,field
try:
    # relative import for use as submodules
    from src.helperFunctions.asdict_repr import asdict_repr
    from src.helperFunctions.loadDict import loadDict
    from src.helperFunctions.updateDict import updateDict
    from src.helperFunctions.log import log
    from src.helperFunctions.parseFrequency import parseFrequency
except:
    # absolute import for use as standalone
    from helperFunctions.asdict_repr import asdict_repr
    from helperFunctions.loadDict import loadDict
    from helperFunctions.updateDict import updateDict
    from helperFunctions.log import log
    from helperFunctions.parseFrequency import parseFrequency
    
@dataclass(kw_only=True)
class _metadata:
    northOffset: float = None
    Latitude: float = None
    Longitude: float = None
    Elevation: float = None
    variableMap: dict = None

@dataclass(kw_only=True)
class _variableMap:
    # map non-standard dtype names 
    dtype_map_numpy = {"IEEE4B": "float32", "IEEE8B": "float64", "FP2": "float32"}
    # default fill for variableName
    fillChar = '_'
    # dataclass fields
    title: str = None
    variableName: str = None
    ignore: bool = None
    sensor: str = None
    units: str = None
    dtype: str = None
    dateRange: list = None
    variableDescription: str = None
    verbose: bool = field(default=False,repr=False)
    dropCols: list = field(default_factory=lambda:[],repr=False)

    def __post_init__(self):
        if self.title is not None:
            if self.variableName is None:
                self.variableName = re.sub('[^0-9a-zA-Z]+',self.fillChar,self.title)
            else:
                self.variableName = re.sub('[^0-9a-zA-Z]+',self.fillChar,self.variableName)
            if self.dtype is not None:
                if self.dtype in self.dtype_map_numpy:
                    self.dtype = np.dtype(self.dtype_map_numpy[self.dtype])
                else:
                    self.dtype = np.dtype(self.dtype)
                if not self.ignore:
                    self.ignore = not np.issubdtype(self.dtype,np.number)
            if self.dtype:
                self.dtype = self.dtype.str
            if self.variableName == self.fillChar*len(self.variableName):
                self.ignore = True
            if self.variableName != self.title and self.verbose:
                print(['Re-named: ',self.title,' to: ',self.variableName])
            if self.title in self.dropCols or self.variableName in self.dropCols:
                self.ignore = True
        
@dataclass(kw_only=True)
class genericLoggerFile(_metadata):
    # Important attributes to be associated with a generic logger file
    sourceFile: str = field(repr=False)
    timezone: str = 'UTC'
    fileType: str = None
    frequency: str = None
    timestampName: str = "TIMESTAMP"
    timestampUnits: str = 'POSIX time (seconds elapsed since 1970-01-01T00:00:00Z)'
    UTCoffset: float = 0
    fileTimestamp: str = field(default='%Y_%m_%d_%H%M',repr=False)
    variableMap: dict = field(default_factory=lambda:{})
    DataFrame: pd.DataFrame = field(default_factory=pd.DataFrame,repr=False)
    verbose: bool = field(default=False,repr=False)
    binZip: bool = field(default=False,repr=False)
    dropCols: list = field(default_factory=lambda:[],repr=False)

    def __post_init__(self):
        if type(self.variableMap) is str and os.path.isfile(self.variableMap):
            self.variableMap = loadDict(self.variableMap)
        pass

    def standardize(self):
        # Create the template column map, fill column dtype where not present 
        if self.fileType is None:
            self.fileType=self.__class__.__name__
        self.variableMap = updateDict(
            {key:{
                'dtype':self.DataFrame[key].dtype,'title':key}|self.variableMap[key] 
                if key in self.variableMap else 
                {'dtype':self.DataFrame[key].dtype,'title':key} 
                for key in self.DataFrame.columns},self.variableMap#,overwrite=overwrite
        )
        self.variableMap[self.timestampName]['dtype'] = 'float64'
        self.variableMap[self.timestampName]['units'] = self.timestampUnits
        self.variableMap = {var.variableName:asdict_repr(var) for var in map(lambda name: _variableMap(dropCols=self.dropCols,**self.variableMap[name]),self.variableMap.keys())}
        if self.frequency is None:
            if isinstance(self.DataFrame.index, pd.DatetimeIndex) and self.DataFrame.index.shape[0]>0:
                self.frequency=f"{np.quantile(self.DataFrame.index.diff().total_seconds().dropna().values,.25)}s"
        if self.fileTimestamp != self.__dataclass_fields__['fileTimestamp'].default:
            self.fileTimestamp = self.fileTimestamp.strftime(format=self.__dataclass_fields__['fileTimestamp'].default)
        if self.binZip:
            print('call')

    def applyvariableNames(self):
        self.safeMap = {val['title']:variableName for variableName,val in self.variableMap.items()}
        self.backMap = {variableName:title for title,variableName in self.safeMap.items()}
        self.DataFrame = self.DataFrame.rename(columns=self.safeMap)

@dataclass
class template(genericLoggerFile):
    sourceFile: str = field(default=os.path.join(os.path.dirname(os.path.abspath(__file__)),'templates','templateExampleData.csv'),repr=False)
    fileType: str = 'example csv'

    def __post_init__(self):
        self.DataFrame = pd.read_csv(self.sourceFile)
        self.DataFrame = self.DataFrame.set_index(pd.to_datetime(self.DataFrame[self.timestampName],format='ISO8601'))
        self.DataFrame = self.DataFrame.drop(columns=[self.timestampName])
        super().__post_init__()
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'templates','templateInputs.yml'),'w+') as f:
            yaml.safe_dump(asdict_repr(self),f,sort_keys=False)
          
@dataclass(kw_only=True)
class binBundle:
    variableMap:str = None
    DataFrame:pd.DataFrame = None
    filename:str = None
    Metadata: configparser.ConfigParser = field(default_factory=lambda:configparser.ConfigParser())
    outputPath: str = None
    dtype: str = 'float32'
    verbose: bool = True

    def __post_init__(self):
        self.templateMD = configparser.ConfigParser()
        self.templateMD.read(os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','ep_md_template.metadata'))
        self.templateMD={sec:{key:value for key,value in self.templateMD[sec].items()} for sec in self.templateMD.sections()}
        for key in self.templateMD:
            self.Metadata.add_section(key)

        # keep = [col for col,md in self.variableMap.items() if not md['ignore']]
        keep = self.find_f32()
        self.bundleOut = {
            f'{self.filename}.metadata':{col:self.variableMap[col] for col in keep},
            f'{self.filename}.tsf64':self.DataFrame.index.values.astype(np.float64)/10**9,
            f'{self.filename}.ecf32':self.DataFrame[keep].values.T.flatten().astype('float32')
        }
        fn = f'{self.filename}.metadata'
        f = os.path.join(self.outputPath,fn)
        with open(f,'w') as out:
            yaml.safe_dump(self.bundleOut[fn],out,sort_keys=False)
        fn = f'{self.filename}.tsf64'
        f = os.path.join(self.outputPath,fn)
        self.bundleOut[fn].tofile(f)
        fn = f'{self.filename}.ecf32'
        f = os.path.join(self.outputPath,fn)
        self.bundleOut[fn].tofile(f)

        
    def find_f32(self):
        log('Restricting to f32, remember to implement fix elsewhere in toolchain so this is not needed in the future',verbose=self.verbose)
        cols = []
        for c,m in self.variableMap.items():
            if m['dtype'] == '<f4' and not m['ignore']:
                cols.append(c)
        return(cols)