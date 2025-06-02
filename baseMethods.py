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
    from .helperFunctions.asdict_repr import asdict_repr
    from .helperFunctions.loadDict import loadDict
    from .helperFunctions.updateDict import updateDict
    from .helperFunctions.log import log
except:
    # absolute import for use as standalone
    from helperFunctions.asdict_repr import asdict_repr
    from helperFunctions.loadDict import loadDict
    from helperFunctions.updateDict import updateDict
    from helperFunctions.log import log
    
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
    dtype_map_numpy = {"IEEE4B": "float32","IEEE8B": "float64","FP2": "float16"}
    # default fill for safeName
    fillChar = '_'
    # dataclass fields
    originalName: str = None
    safeName: str = None
    ignore: bool = None
    sensor: str = None
    unit: str = None
    dtype: str = None
    variableDescription: str = None
    verbose: bool = field(default=False,repr=False)
    dropCols: list = field(default_factory=lambda:[],repr=False)

    def __post_init__(self):
        if self.originalName is not None:
            if self.safeName is None:
                self.safeName = re.sub('[^0-9a-zA-Z]+',self.fillChar,self.originalName)
            else:
                self.safeName = re.sub('[^0-9a-zA-Z]+',self.fillChar,self.safeName)
            if self.dtype is not None:
                if self.dtype in self.dtype_map_numpy:
                    self.dtype = np.dtype(self.dtype_map_numpy[self.dtype])
                else:
                    self.dtype = np.dtype(self.dtype)
                if not self.ignore:
                    self.ignore = not np.issubdtype(self.dtype,np.number)
            if self.dtype:
                self.dtype = self.dtype.str
            if self.safeName == self.fillChar*len(self.safeName):
                self.ignore = True
            if self.safeName != self.originalName and self.verbose:
                print(['Re-named: ',self.originalName,' to: ',self.safeName])
            if self.originalName in self.dropCols or self.safeName in self.dropCols:
                self.ignore = True
        
@dataclass(kw_only=True)
class genericLoggerFile(_metadata):
    # Important attributes to be associated with a generic logger file
    sourceFile: str = field(repr=False)
    timezone: str = 'UTC'
    fileType: str = None
    frequency: str = None
    timestampName: str = "TIMESTAMP"
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
                'dtype':self.DataFrame[key].dtype,'originalName':key}|self.variableMap[key] 
                if key in self.variableMap else 
                {'dtype':self.DataFrame[key].dtype,'originalName':key} 
                for key in self.DataFrame.columns},self.variableMap#,overwrite=overwrite
        )
        self.variableMap = {var.safeName:asdict_repr(var) for var in map(lambda name: _variableMap(dropCols=self.dropCols,**self.variableMap[name]),self.variableMap.keys())}
        if self.frequency is None:
            if isinstance(self.DataFrame.index, pd.DatetimeIndex) and self.DataFrame.index.shape[0]>0:
                self.frequency=f"{np.quantile(self.DataFrame.index.diff().total_seconds().dropna().values,.25)}s"
        if self.fileTimestamp != self.__dataclass_fields__['fileTimestamp'].default:
            self.fileTimestamp = self.fileTimestamp.strftime(format=self.__dataclass_fields__['fileTimestamp'].default)
        if self.binZip:
            print('call')

    def applySafeNames(self):
        self.safeMap = {val['originalName']:safeName for safeName,val in self.variableMap.items()}
        self.backMap = {safeName:originalName for originalName,safeName in self.safeMap.items()}
        self.DataFrame = self.DataFrame.rename(columns=self.safeMap)
        
    def parseFreq(self,text):
        #Parse a measurement frequency from a assorted string inputs to a format compatible with pandas datetime
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