import re
import numpy as np
import pandas as pd
from dataclasses import dataclass,field

def reprToDict(dc):
    # given a dataclass, dummp itemes where repr=true to a dictionary
    return({k:v for k,v in dc.__dict__.items() if k in dc.__dataclass_fields__ and dc.__dataclass_fields__[k].repr})

@dataclass(kw_only=True)
class columnMap:
    dtype_map_numpy = {"IEEE4B": "float32","IEEE8B": "float64","FP2": "float16"}
    fillChar = '_'
    originalName: str
    safeName: str = field(default=None,repr=False)
    ignore: bool = True
    unit: str = None
    dtype: str = None
    frequency: str = None
    variableDescription: str = None
    verbose: bool = field(default=False,repr=False)
    def __post_init__(self):
        if self.originalName is not None:
            if self.safeName is None:
                self.safeName = re.sub('[^0-9a-zA-Z]+',self.fillChar,self.originalName)
            else:
                self.safeName = self.safeName.rstrip('_')
            if self.dtype is not None:
                if self.dtype in self.dtype_map_numpy:
                    self.dtype = np.dtype(self.dtype_map_numpy[self.dtype])
                else:
                    self.dtype = np.dtype(self.dtype)
                self.ignore = not np.issubdtype(self.dtype,np.number)
            if self.dtype:
                self.dtype = self.dtype.str
            if self.safeName == self.fillChar*len(self.safeName):
                self.ignore = True
            if self.safeName != self.originalName and self.verbose:
                print(['Re-named: ',self.originalName,' to: ',self.safeName])
        
@dataclass(kw_only=True)
class genericLoggerFile:
    # Important attributes to be associated with a generic logger file
    sourceFile: str = field(repr=False)
    timezone: str = None
    fileType: str = None
    frequency: str = None
    fileTimestamp: str = field(default='%Y_%m_%d_%H%M',repr=False)
    variableMap: dict = field(default_factory=lambda:{})
    Data: pd.DataFrame = field(default_factory=pd.DataFrame,repr=False)
    verbose: bool = field(default=True,repr=False)

    def __post_init__(self):
        # Create the template column map, fill column dtype where not present 
        self.variableMap = {key:{'dtype':self.Data[key].dtype,'originalName':key}|self.variableMap[key] 
                                if key in self.variableMap 
                                else {'dtype':self.Data[key].dtype,'originalName':key} 
                                for key in self.Data.columns}
        self.variableMap = {var.safeName:reprToDict(var) for var in map(lambda name: columnMap(**self.variableMap[name]),self.variableMap.keys())}

        self.fileTimestamp = self.fileTimestamp.strftime(self.__dataclass_fields__['fileTimestamp'].default)


    def applySafeNames(self):
        self.safeMap = {val['originalName']:safeName for safeName,val in self.variableMap.items()}
        self.backMap = {safeName:originalName for originalName,safeName in self.safeMap.items()}
        self.Data = self.Data.rename(columns=self.safeMap)