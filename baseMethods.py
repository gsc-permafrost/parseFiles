import re
import os
import yaml
import numpy as np
import pandas as pd
import zipfile
import configparser
from dataclasses import dataclass,field

def reprToDict(dc):
    # given a dataclass, dummp itemes where repr=true to a dictionary
    return({k:v for k,v in dc.__dict__.items() if k in dc.__dataclass_fields__ and dc.__dataclass_fields__[k].repr})

def updateDict(base,new,overwrite=False,verbose=False):
    if base == new: return(base)
    # more comprehensive way to update items in a nested dict
    for key,value in new.items():
        if type(base) is dict and key not in base.keys():
            if verbose: print('setting: ',key,' = ',base,'\n to: ',key,' = ',value)
            base[key]=value
        elif type(value) is dict and type(base[key]) is dict:
            base[key] = updateDict(base[key],value,overwrite,verbose)
        elif overwrite == True and base[key]!= value:
            if verbose: print('setting: ',key,' = ',base[key],'\n to: ',key,' = ',value)
            base[key] = value
        elif overwrite == 'append' and type(base[key]) is list:
            if type(base[key][0]) is not list and type(value) is list:
                base[key] = [base[key]]
            if verbose: print('adding: ',value,'\n to: ',key,' = ',base[key])
            base[key].append(value)
        elif overwrite == 'append' and type(base[key]) is not list:
            base[key] = [base[key]]
            if verbose: print('adding: ',value,'\n to: ',key,' = ',base[key])
            base[key].append(value)
        elif base[key] is None and value is not None:
            if verbose: print('setting: ',key,' = ',base[key],'\n to: ',key,' = ',value)
            base[key] = value
        elif base[key] != value:
            if verbose: print(f'overwrite = {overwrite} will not update matching keys: ',base[key],value)
    return(base) 

@dataclass(kw_only=True)
class columnMap:
    dtype_map_numpy = {"IEEE4B": "float32","IEEE8B": "float64","FP2": "float16"}
    fillChar = '_'
    originalName: str
    safeName: str = field(default=None,repr=False)
    ignore: bool = None
    instrument: str = None
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
                if not self.ignore:
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
    timezone: str = 'UTC'
    fileType: str = None
    frequency: str = None
    timestampName: str = "TIMESTAMP"
    fileTimestamp: str = field(default='%Y_%m_%d_%H%M',repr=False)
    variableMap: dict = field(default_factory=lambda:{})
    DataFrame: pd.DataFrame = field(default_factory=pd.DataFrame,repr=False)
    verbose: bool = field(default=False,repr=False)
    binZip: bool = field(default=False,repr=False)

    def __post_init__(self):
        # Create the template column map, fill column dtype where not present 
    
        self.variableMap = {key:{'dtype':self.DataFrame[key].dtype,'originalName':key}|self.variableMap[key] 
                                if key in self.variableMap 
                                else {'dtype':self.DataFrame[key].dtype,'originalName':key} 
                                for key in self.DataFrame.columns}
        self.variableMap = {var.safeName:reprToDict(var) for var in map(lambda name: columnMap(**self.variableMap[name]),self.variableMap.keys())}
        if self.frequency is None:
            self.frequency=f"{np.quantile(self.DataFrame.index.diff().total_seconds().dropna().values,.25)}s"
        self.fileTimestamp = self.fileTimestamp.strftime(format=self.__dataclass_fields__['fileTimestamp'].default)
        if self.binZip:
            print('call')

    def applySafeNames(self):
        self.safeMap = {val['originalName']:safeName for safeName,val in self.variableMap.items()}
        self.backMap = {safeName:originalName for originalName,safeName in self.safeMap.items()}
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
            yaml.safe_dump(reprToDict(self),f,sort_keys=False)

            
@dataclass(kw_only=True)
class binBundle:
    variableMap:str = None
    DataFrame:pd.DataFrame = None
    filename:str = None
    Metadata: configparser.ConfigParser = field(default_factory=lambda:configparser.ConfigParser())
    # Metadata: dict = field(default=os.path.join(os.path.dirname(os.path.abspath(__file__)),'parseFiles','config_files','ep_md_template.metadata'))
    dtype: str = 'float32'

    def __post_init__(self):
        self.templateMD = configparser.ConfigParser()
        self.templateMD.read(os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','ep_md_template.metadata'))
        self.templateMD={sec:{key:value for key,value in self.templateMD[sec].items()} for sec in self.templateMD.sections()}
        for key in self.templateMD:
            self.Metadata.add_section(key)

        keep = [col for col,md in self.variableMap.items() if not md['ignore']]
        self.binZipOut = {
            f'{self.filename}.metadata':{col:self.variableMap[col] for col in keep},
            f'{self.filename}.POSIX_timestamp':self.DataFrame.index.values.astype(np.float64)/10**9,
            f'{self.filename}.float32_array':self.DataFrame[keep].values.T.flatten().astype('float32')
        }