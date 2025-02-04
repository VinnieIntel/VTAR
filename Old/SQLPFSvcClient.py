"""
License : Copyright (c) Intel Corporation 2023
Product: Intel.TD.ATA.SQLPathFinder : Service Client
Authors : vishwas.nataraj@intel.com;SQLPathfinder_support@intel.com
History: 
1.0.0.0 : vanatara : Initial Version
2.0.0.0 : vanatara : Updated to support new payload formats
2.1.0.0 : vanatara : Updated to support robocopy output from shared folder
2.2.0.0 : vanatara : Enable Async execution
3.0.0.0 : vanatara : add Sync service execution support + enhancements + removed EOL'ed 'SQLPFSvcClient1' 
3.0.0.0a: vanatara : add SH properties 'SHJobName' & 'SHEntryID' into service input to help tracking and troubleshooting
3.0.0.0b: vanatara : update 'ZipFile' to ignore path of file being archived + removed. Check input/output files/list for relative path  
3.0.0.0c: vanatara : update 'required_input_files_list_csv' to support absolute path
3.0.0.0d: vanatara : Added CreateInGroupDataFile() to support creation of In-Group file
3.0.0.0e: vanatara : Bugfix handling absolute/relative paths for required input files. Removed generate_pems() as it is no longer used
3.0.0.0f: vanatara : Bugfix in GetFileDLM() and CreateInGroupDataFile()
3.0.0.0g: vanatara : add SPF_CL_Args param to support Command-line (CL_) args and Record_SPF() to log SPF usage
3.0.0.0h: vanatara : bugfix Async:validateExecutionResponse() error handling. Updated service console output file to be retained on service error
3.0.0.0i: vanatara : Added 'delete_svc_console_output' param for Async API to control delete(True)/retain(False) of service console output file. 
3.0.0.0j: vanatara : Removed 'delete_svc_console_output' param added 'copy_svc_console_flag' to control copying(True) of service execution console output file default = True and add the same parameter to Sync flow. Updated Sync flow to enable fetching console output and log files
3.0.0.0k: vanatara : bug fix validateExecutionResponse() corrected to use right flag variable
3.0.1.0 : vanatara : pandas removed deprecated read_csv() parameters error_bad_lines, warn_bad_lines & set on_bad_lines = 'skip'
3.0.1.1 : jmclarke : Added read_csv argument date_format for Pandas 2.0 or higher
3.0.1.2 : vanatara : Fix SVC console output printing to console.
3.0.1.3 : vanatara : Update console statements. Support Pandas V2.2+ removed deprecated parameters in pd.read_csv() 'keep_date_col', 'delim_whitespace', 'verbose'.
3.0.1.4 : vanatara : Updated console statements. Add new parameter logFileName
3.0.1.5 : vanatara : Updated _record_spf() arguments type hinting to be backward compatible with earlier version of Python 3.9
"""
__version__ = "3.0.1.5"
import os, sys, copy, socket, shutil
import datetime, time
from datetime import datetime, timedelta
import json
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Union
import base64
import pathlib
from pathlib import Path
import subprocess
import zlib, zipfile
import random
import pandas
import re
import codecs
import validators

import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(category=InsecureRequestWarning)
import requests
from requests.exceptions import HTTPError
from urllib.parse import urlparse, urlsplit

class ScriptHost(object):
    """
    Methods from ScriptHost.va
    """

    __CLASS_UserMain="{768E4AF3-1136-48B8-B957-BD798CA9F3F9}"
    __CLASS_PingMain="{DFB25043-5028-4051-A078-853EB2A6532E}"

    __server = None
    __entryID = None
    __isSHEntry = None
    __jobName = None
    __scriptFile = None
    __NearestNASAnalysis = None
    __tempFldr = None
    __jobDir = None
    __userArea = None
    __queueAttributes = None
    __logFile = None
    __logFileNamePart = None
    __spfLogFile = None
    __initialized = False

    def __init__(self):
        
        #get required env variables
        if not ScriptHost.__initialized:
            ScriptHost.__server = os.getenv("SHServer")

            ScriptHost.__entryID = os.getenv("SHEntry")
            ScriptHost.__NearestNASAnalysis = os.getenv("NearestNASAnalysis")
            

            #continue to get other env variables
            ScriptHost.__jobName = os.getenv("SHJob")
            ScriptHost.__scriptFile = os.getenv("SHScript")
            ScriptHost.__tempFldr = os.getenv("temp")
            ScriptHost.__jobDir = os.getenv("SHJobDir")
            ScriptHost.__userArea = os.getenv("SHUserArea")
            ScriptHost.__queueAttributes = os.getenv("SHQueueAttributes")
            ScriptHost.__logFile = os.getenv("SHLog")
            if ScriptHost.__logFile is not None :                
                ScriptHost.__logFileNamePart = os.path.splitext(os.path.split(ScriptHost.__logFile)[1])[0]
                ScriptHost.__spfLogFile = os.path.join(ScriptHost.__jobDir, "{0}.spf{1}".format(*os.path.splitext(ScriptHost.__logFile)))

            #print("SH Initialized")
            ScriptHost.__initialized = True

    @property
    def SHisSHEntry(self):
        #set prop SHisSHEntry 
        if ScriptHost.__isSHEntry is None :
            if self.SHserver is not None and self.SHentryID.isdigit():
                    ScriptHost.__isSHEntry = True
            else:
                ScriptHost.__isSHEntry = False

        return ScriptHost.__isSHEntry

    @property
    def SHserver(self):
        return ScriptHost.__server

    @property
    def SHentryID(self)->str:
        return ScriptHost.__entryID  # type: ignore

    @property
    def SHNearestNASAnalysis(self):
        return ScriptHost.__NearestNASAnalysis

    @property
    def SHtempFldr(self):
        return ScriptHost.__tempFldr

    @property
    def SHscriptFile(self):
        return ScriptHost.__scriptFile
        
    @property
    def SHjobName(self):
        return ScriptHost.__jobName

    @property
    def SHJobDir(self):
        return ScriptHost.__jobDir

    @property
    def SHUserArea(self):
        return ScriptHost.__userArea

    @property
    def SHQueueAttributes(self):
        return ScriptHost.__queueAttributes

    @property
    def SHLog(self):
        return ScriptHost.__logFile

    @property
    def SHSPFLog(self):
        return ScriptHost.__spfLogFile

    @property
    def SHLogNamePart(self):
        return ScriptHost.__logFileNamePart

    @property
    def SHalreadyRunning(self):
        errMsg = " not implemented"
        raise NotImplementedError(errMsg)

    @property
    def SHisLastJobEntry(self):
        errMsg = " not implemented"
        raise NotImplementedError(errMsg)

    #status : dev done, UT pending
    def SHPathForCB(self, release) :
        """
        ' get CB path on SH
        """
        #locals
        myPathForCB = None
        aReg = None
        aKey = None
        cbVersionsIni = ""

        try :
            try :
                try : 
                    aReg = ConnectRegistry(None, HKEY_LOCAL_MACHINE)
                    aKey = OpenKey(aReg, r"Software\ACTools\ScriptHost\CrystalBall")
                except Exception as err:
                    errMsg = "Error '{0}' occurred while looking for Crystal Ball in registry.".format(err.strerror)
                    raise Exception(errMsg)

                try :
                    pathKey = "BaseInstallPath"
                    cbVersionsIni = ExpandEnvironmentStrings(QueryValueEx(aKey, pathKey))
                except Exception as err:
                    errMsg = "Error '{0}' occurred while looking for Key '{1}' in registry.".format(err.strerror, pathKey)
                    raise Exception(errMsg)
            except Exception as err:
                pass #just pass...it errors out in SH...looks like it is an env variable

            if len(cbVersionsIni) > 0 :
                cbVersionsIni = os.path.join(cbVersionsIni, "CBVersions.ini")
            else :
                myPathForCB = os.getenv("CB")
                return myPathForCB
            
            INI_Config_Obj = ConfigParser.ConfigParser()
            INI_Config_Obj.read(cbVersionsIni)

            cbVersion = INI_Config_Obj.get("Releases", release)

            if cbVersion is None :
                cbVersion = release

            myPathForCB = os.getenv(cbVersion)
            if myPathForCB is None:
                myPathForCB = os.getenv("CB")

            return myPathForCB
        except Exception as err:
            raise
#END : class ScriptHost

class SQLPFSvcClient(ScriptHost):
    """client handler for SQLPF Service"""
    #region config/constants
    __gRetryMaxCount = 3
    __gSleepBetweenRetrySeconds = 10
    __gSPFSWSHDir = r"d:\sqlpathfinder\software"
    __gSPFLib = '\\\\atdfile3.ch.intel.com\\atd-web\\PathFinding\\SQLPathFinder\\Software\\'

    _gSQLPFSvcEnvLookup:Dict = {
                                'SQLPFAAS_PROD' : 'https://goto.intel.com/SQLPFaaS_PROD_V2'
                              , 'SQLPFAAS_PROD_CH_N1' : 'https://goto.intel.com/SQLPFaaS_PROD_CH_N1_V2'
                              , 'SQLPFAAS_PROD_CH_N2' : 'https://goto.intel.com/SQLPFaaS_PROD_CH_N2_V2'
                              , 'SQLPFSVC_DEVCH' : 'https://goto.intel.com/SQLPFaaSDEVCH_V2'
                              , 'SQLPFSVC_DEVCH2' : 'https://dtdvdwspfwb1.amr.corp.intel.com/SQLPFaaSGatewaySvcWAVN/api/Execute'
                              , 'SQLPFaaSDEVCH_atdvdwspfwb2' : 'https://goto.intel.com/SQLPFaaSDEVCH_atdvdwspfwb2'
                              , 'SQLPFSVC_DEVBA' : "https://localhost:44374//api/execute"
                            }


    #endregion config/constants

    #region properties from SPF GLobals
    @property
    def G_DT_FORMAT_DD_MON_YYYY_HH24_MM_SS(self)-> str:
        return "%d-%b-%Y %H:%M:%S" #'09-Oct-2015 18:32:02'

    @property
    def DatetimeNow(self)-> str:
        myNow = str(datetime.now().strftime(self.G_DT_FORMAT_DD_MON_YYYY_HH24_MM_SS))  # type: ignore
        return myNow

    __gisSPFonSH = None #False #"N"
    @property
    def gisSPFonSH(self)-> bool:
        """
        ' get: 'Is SPF Installed on SH
        """
        if self.__gisSPFonSH is None :
            tmp1 = os.path.join(self.gSPFSWSHDir, "R_Version.ini")
            self.logger.debug(f"gisSPFonSH: tmp1 : {tmp1}")
            self.logger.debug(f"gisSPFonSH: os.path.exists : {os.path.exists(tmp1)}")
            if os.path.exists(tmp1) is True :
                self.__gisSPFonSH = True # SPF SW is Installed on SH
            else :
                self.__gisSPFonSH = False # SPF SW is NOT Installed on SH
        self.logger.debug(f"get: gisSPFonSH: {self.__gisSPFonSH}")
        return self.__gisSPFonSH

    __gTempDir = None  #SH Local Dir
    @property
    def gTempDir(self)-> str:
        """
        #get: SH Local Dir
        """
        if self.__gTempDir is None :
            if self.SHisSHEntry is True :
                if self.gisSPFonSH is True  :
                    self.__gTempDir = self.gSPFSWSHDir
                else :
                    self.__gTempDir = self.SHtempFldr
            else :
                self.__gTempDir = os.getenv("temp")

        self.logger.debug(f"get - gTempDir : {self.__gTempDir}")
        return self.__gTempDir  # type: ignore
    
    __gSPFExe = None
    @property
    def gSPFExe(self) -> str:
        """
        ' get: global EXE path
        """
        self.logger.debug(f"get: gSPFExe: {self.__gSPFExe}")
        return self.__gSPFExe  # type: ignore
    @gSPFExe.setter
    def gSPFExe(self, value: str):
        """
        set : gSPFExe -- value is passed from calling client
        """
        if self.__gSPFExe is None:
            if value in ["", None]:
                raise ValueError(f"Invalid value for SPFExe : '{value}'. Provide the location of SQLPathfinder installation or SPFBin location")
            else: #SQLPathfinder install location is provided
                #check if path exists
                _tmp_SPFExe_pathObj = Path(value)

                if _tmp_SPFExe_pathObj.exists() is True and _tmp_SPFExe_pathObj.is_dir() is True:
                    self.__gSPFExe = str(_tmp_SPFExe_pathObj.resolve())
                else:
                    raise FileNotFoundError(f"Not found : SQLPathfinder installation or SPFBin location : {str(_tmp_SPFExe_pathObj)}")
            self.logger.debug(f"set: gSPFExe: {self.__gSPFExe}")
    #endregion properties from SPF GLobals

    #region properties
    _SvcExecInput_template = None
    @property
    def SvcExecInput_template(self)-> dict:
        """
        get : SvcExecInput_template -> Dict 
        """
        if self._SvcExecInput_template is None:
            self._SvcExecInput_template = {"Input":{"SPFLogLevel" : self.SPFLogLevel
                                                 , "SPFInstance" : f"{self.SPFInstance}"
                                                 , "CMDARGS" : self.SQLPF_CMD_Args_List_JSONString
                                                 , "ClientApp" : self.ClientAppName
                                                 , "ComputerName" : socket.getfqdn()
                                                 , "ClientUserIDSID" : self.user_idsid
                                                 , "ClientAPIVersion" : __version__
                                                 , "hpc_svc_memory_limit_GB" : 32
                                                 , "hpc_svc_execution_time_limit_minutes" : 480
                                                 , "ClientInvokeStartTimeStamp" : self.DatetimeNow
                                                 }
                                          } 

        return copy.deepcopy(self._SvcExecInput_template)

    @property
    def gSQLPFSvcEnvLookup(self)-> Dict:
        """
        get : A lookup dict SQLPFaaS Gateway service URLs
        """
        return self._gSQLPFSvcEnvLookup

    _SQLPFSvcURL = None
    @property
    def SQLPFSvcURL(self)-> str:
        """
        get : SQLPFSvcURL - SQLPF Service URL
        """
        if self._SQLPFSvcURL is None:
            try:
                tmp_SQLPFSvcURL = self.gSQLPFSvcEnvLookup[self.SQLPFSvcEnv]
            except Exception as err:
                # self.logger.warn(f"Env '{self.SQLPFSvcEnv}' not found...updating to a goto url")
                tmp_SQLPFSvcURL = f"HTTPS://GOTO.INTEL.COM/{self.SQLPFSvcEnv}"
                
            self.logger.debug(f"get : tmp_SQLPFSvcURL : {tmp_SQLPFSvcURL}")
            if tmp_SQLPFSvcURL.upper().startswith('HTTPS://GOTO.INTEL.COM') is True:
                self.logger.debug(f"Transform GOTO URL : {tmp_SQLPFSvcURL}")
                try:
                    r = requests.get(tmp_SQLPFSvcURL, verify = False) #, verify=self.generate_pems())
                    tmp_SQLPFSvcURL = r.url 
                    if (tmp_SQLPFSvcURL.lower().find("error") > 0):
                        raise Exception(f"Service Endpoint not found for : '{self.SQLPFSvcEnv}'")
                except HTTPError as httpErr:
                    self.logger.exception(F"GOTO Error {httpErr.response.status_code}")
                    raise Exception(f"Service Endpoint not found for : '{self.SQLPFSvcEnv}'")
                except requests.exceptions.SSLError as sslErr:
                    raise
            # if tmp_SQLPFSvcURL.upper().endswith("/EXECUTE") is False:
            #     tmp_SQLPFSvcURL = f"{tmp_SQLPFSvcURL}/Execute" 
            self._SQLPFSvcURL = tmp_SQLPFSvcURL 
        self.logger.debug(f"get : SQLPFSvcURL : {self._SQLPFSvcURL}")
        return self._SQLPFSvcURL
    @SQLPFSvcURL.setter
    def SQLPFSvcURL(self, value: str):
        """
        set : SQLPFSvcURL - SQLPF Service URL
        """
        if self._SQLPFSvcURL is None:
            if value not in ["", None]:
                self._SQLPFSvcURL = value
                self.logger.debug(f"set : SQLPFSvcURL : {self._SQLPFSvcURL}")
    
    #region logSvc properties
    _SPF_LogSvc_Env: str = "SQLPF_LogSvc"
    @property
    def SPF_LogSvc_Env(self)->str:
        """
        get : SPF_LogSvc_Env
        """
        self.logger.debug(f"get : SPF_LogSvc_Env : {self._SPF_LogSvc_Env}")
        return self._SPF_LogSvc_Env
    
    @SPF_LogSvc_Env.setter
    def SPF_LogSvc_Env(self, value:str):
        """
        set: 
        """
        self.logger.debug(f" value : {value}")
        if type(value) is not str:
            raise Exception(f"Invalid value type for SPF_LogSvc_Env : '{value}'. Expecting a str")
        if value.strip() != "":
            self._SPF_LogSvc_Env = value if os.getenv("SPF_LOGSVC_ENV") is None else os.getenv("SPF_LOGSVC_ENV")
        elif os.getenv("SPF_LOGSVC_ENV") is not None:
            self._SPF_LogSvc_Env = os.getenv("SPF_LOGSVC_ENV")

        self.logger.debug(f"set : SPF_LogSvc_Env : {self._SPF_LogSvc_Env}")

    @property
    def SQLPF_LogSvcURL(self)-> str:
        """
        get : 
        """
        try:
            tmp_SQLPF_LogSvcURL = f"HTTPS://GOTO.INTEL.COM/{self.SPF_LogSvc_Env}"
            r = requests.get(tmp_SQLPF_LogSvcURL, verify = False) #, verify=self.generate_pems())
            tmp_SQLPF_LogSvcURL = r.url 
            if (tmp_SQLPF_LogSvcURL.lower().find("error") > 0):
                raise Exception(f"Service Endpoint not found for : '{self.SPF_LogSvc_Env}'")
            if tmp_SQLPF_LogSvcURL.lower()[-4:] == ".svc":
                tmp_SQLPF_LogSvcURL = f"{tmp_SQLPF_LogSvcURL}/InsertSQLPFLogData"
            self.logger.debug(f"get : SQLPF_LogSvcURL : {tmp_SQLPF_LogSvcURL}")
            return tmp_SQLPF_LogSvcURL
        except HTTPError as httpErr:
            self.logger.exception(F"GOTO Error {httpErr.response.status_code}")
            raise Exception(f"Service Endpoint not found for : '{self.SQLPFLogSvcEnv}'")
        except Exception as err:
            raise

    @property
    def LogEE(self)->str:
        """
        get : LogEE
        """
        "ipykernel_launcher.py"
        if self.logCallingScript.lower() == "spfsql3.py":
            return "Py3_ASYNC_API"
        elif self.logCallingScript.lower() == "ipykernel_launcher.py":
            return "Jup_ASYNC_API"
        else:
            return "ASYNC_API"
    #endregion logSvc properties

    #region SQLPF_CMD_Args_List
    @property 
    def SQLPF_CMD_Args_List_JSONString(self)-> str:
        """
        get : SQLPF_CMD_Args_List_JSONString return a JSON string of SQLPF_CMD_Args_List
        """
        __tmp = json.dumps(self.SQLPF_CMD_Args_List)
        self.logger.debug(f"get : SQLPF_CMD_Args_List_JSONString : {__tmp}")
        return __tmp
    _SQLPF_CMD_Args_List = None
    @property
    def SQLPF_CMD_Args_List(self)-> List:
        """
        get : SQLPF CMD Arguments - input to SvcPyEE. e.g., "CMDARGS":"[\"/SPFINSTANCE=10061\", \"/iREPORTS_VERSION=NEXT\", \"/SPFLOGLEVEL=DEBUG\"]"
        """
        #concat both CMD_Args_List & CL_Args_List to get the final CMD_Args_List
        __tmp = self._SQLPF_CMD_Args_List + self.SPF_CL_Args_List
        self.logger.debug(f"get : SQLPF_CMD_Args_List : {__tmp}")
        return __tmp  # type: ignore
    
    @SQLPF_CMD_Args_List.setter
    def SQLPF_CMD_Args_List(self, value: List):
        """
        set : SQLPF_CMD_Args_List
        """
        self.logger.debug(f"set : SQLPF_CMD_Args_List : value : {value}")
        if self._SQLPF_CMD_Args_List is None:
            if type(value) is not list:
                raise Exception("SQLPF_CMD_Args_List expects a list object")
            if value in [[]]:
                value = [f'/SPFINSTANCE={self.SPFInstance}', f'/SPFLOGLEVEL={self.SPFLogLevel}'] 
            
            #process CMD_Args_list for CL_ params, remove and add to SPF_CL_Args
            _idx_of_CL_items_to_remove = []
            for idx, item in enumerate(value):
                try:
                    if item.lower().startswith('/cl_') is True:
                        key, val = item.strip("/").split("=")
                        self.SPF_CL_Args [key] = val # add to CL_args -- new values overwrite existing values
                        _idx_of_CL_items_to_remove.append(idx) #keep track of idx for CL_ arguments
                except Exception as err:
                    errMSg = f"Error while parsing SQLPF_CMD_Args_List item '{item}' as a CL argument"
                    raise Exception(errMSg)
            self.logger.debug(f"_idx_of_CL_items_to_remove : {_idx_of_CL_items_to_remove}")
            
            #remove the items from CMD_Args...will be populated in CDM_Args during the 'get' call
            for idx in _idx_of_CL_items_to_remove:
                value.pop(idx)
            self.logger.debug(f"After CL_ cleanup : value : {value}")
            self._SQLPF_CMD_Args_List = value
            self.logger.debug(f"set : SQLPF_CMD_Args_List : {self._SQLPF_CMD_Args_List}")
    
    @property
    def SPF_CL_Args_List(self) -> list:
        """
        get : SPF_CL_Args_List --> construct a list based on SPF_CL_Args dictionary
        """
        __tmp = [f"/{key}={val}" for key, val in self._SPF_CL_Args.items()]    
        self.logger.debug(f"get : SPF_CL_Args_List : {__tmp}")
        return __tmp
    
    _SPF_CL_Args: dict = {}
    @property
    def SPF_CL_Args(self) -> dict:
        """
        get : SPF_CL_Args
        """
        self.logger.debug(f"get : SPF_CL_Args : {self._SPF_CL_Args}")
        return self._SPF_CL_Args
    
    @SPF_CL_Args.setter
    def SPF_CL_Args(self, value:dict):
        """
        set : SPF_CL_Args
        """
        if type(value) is not dict:
            raise Exception(f"Invalid value for 'SPF_CL_Args' : '{value}'. Please provide a dictionary e.g., {{'CL_VAR_NAME' : 'VAR_VALUE'}}")
        
        if len(value) == 0:
            return #no items to process
        
        #validate key names start with 'CL_'
        for key in value.keys():
            if key.lower().startswith('cl_') is False:
                raise Exception(f"Invalid key name '{key}'. SPF_CL_Args dictionary key should start with 'CL_")
        if len(self._SPF_CL_Args) == 0:
            self._SPF_CL_Args = value
        else:
            self._SPF_CL_Args = self._SPF_CL_Args | value #new values overwrite old values
        
        self.logger.debug(f"set : SPF_CL_Args : {self._SPF_CL_Args}")
    #endregion SQLPF_CMD_Args_List

    _required_input_files_list_csv = None
    _required_input_files_list_csv_orig = None
    @property
    def required_input_files_list_csv(self)-> Path:
        """
        get : required_input_files_list_csv : Path object
        """
        self.logger.debug(f"get : required_input_files_list_csv : {self._required_input_files_list_csv}")
        return self._required_input_files_list_csv  # type: ignore
    @required_input_files_list_csv.setter
    def required_input_files_list_csv(self, value: str or list) -> None:
        """
        set : required_input_files_list_csv : Path object
        """
        if self._required_input_files_list_csv is None:
            self.logger.debug(f"required_input_files_list_csv : value : {value}")
            if value not in ["", None, []]:
                self.__required_input_files_list_csv_orig = value
                if type(value) is list:
                    value = self.CreateTempFileFromInputFilesList(value, "required_input_files_list_csv")
                if Path(value).exists() is False:
                    raise FileNotFoundError(f"File not found - 'File with Required Input Files' : {value}")
                self._required_input_files_list_csv = Path(value)
        self.logger.debug(f"set : required_input_files_list_csv : {value}")        
    
    _required_input_files_list_csv_archive_name = None    
    @property
    def required_input_files_list_csv_archive_name(self)-> str:
        """
        get : required_input_files_list_csv_archive_name
        """
        if self._required_input_files_list_csv_archive_name is None and self.required_input_files_list_csv is not None:
            self._required_input_files_list_csv_archive_name = f"{self.required_input_files_list_csv.name}.zip"
        self.logger.debug(f"get : required_input_files_list_csv_archive_name : {self._required_input_files_list_csv_archive_name}")
        return self._required_input_files_list_csv_archive_name  # type: ignore

    _required_output_files_folder_list_csv = None
    @property
    def required_output_files_folder_list_csv(self) -> Path:
        """
        get : required_output_files_folder_list_csv : Path object
        """
        return self._required_output_files_folder_list_csv  # type: ignore
    @required_output_files_folder_list_csv.setter
    def required_output_files_folder_list_csv(self, value: str or list) -> None:
        """
        set : required_output_files_folder_list_csv
        """
        if self._required_output_files_folder_list_csv is None:
            if value not in ["", None, []]:
                if type(value) is list:
                    value = self.CreateTempFileFromInputFilesList(value, "required_output_files_folder_list_csv")
                if Path(value).exists() is False:
                    raise FileNotFoundError(f"File not found - 'File with Required Output Files/Folders' : {value}")
                self._required_output_files_folder_list_csv = Path(value)
    
    _required_output_files_folder_list_csv_archive_name = None
    @property
    def required_output_files_folder_list_csv_archive_name(self) -> str:
        """
        get : required_output_files_folder_list_csv_archive_name
        """
        if self._required_output_files_folder_list_csv_archive_name is None and self.required_output_files_folder_list_csv is not None:
            self._required_output_files_folder_list_csv_archive_name = f"{self.required_output_files_folder_list_csv.name}.zip"
        self.logger.debug(f"get : _required_output_files_folder_list_csv_archive_name : {self._required_output_files_folder_list_csv_archive_name}")
        return self._required_output_files_folder_list_csv_archive_name  # type: ignore

    _optional_output_files_folder_list_csv = None
    @property
    def optional_output_files_folder_list_csv(self) -> Path:
        """
        get : optional_output_files_folder_list_csv
        """
        return self._optional_output_files_folder_list_csv  # type: ignore
    @optional_output_files_folder_list_csv.setter
    def optional_output_files_folder_list_csv(self, value: str or list) -> None:
        """
        set : optional_output_files_folder_list_csv
        """
        if self._optional_output_files_folder_list_csv is None:
            if value not in ["", None, []]:
                if type(value) is list:
                    value = self.CreateTempFileFromInputFilesList(value, "optional_output_files_folder_list_csv")
                if Path(value).exists() is False:
                    raise FileNotFoundError(f"File not found - 'File with Optional Output Files/Folders' : {value}")
                self._optional_output_files_folder_list_csv = Path(value)
    
    _optional_output_files_folder_list_csv_archive_name = None
    @property
    def optional_output_files_folder_list_csv_archive_name(self) -> str:
        """
        get : optional_output_files_folder_list_csv_archive_name
        """
        if self._optional_output_files_folder_list_csv_archive_name is None and self.optional_output_files_folder_list_csv is not None:
            self._optional_output_files_folder_list_csv_archive_name = f"{self.optional_output_files_folder_list_csv.name}.zip"
        self.logger.debug(f"get : optional_output_files_folder_list_csv_archive_name : {self._optional_output_files_folder_list_csv_archive_name}")
        return self._optional_output_files_folder_list_csv_archive_name  # type: ignore
    
    _SPFSQL_File = None
    @property
    def SPFSQL_File(self)-> Path:
        """
        get : SPFSQL_File
        """
        return self._SPFSQL_File  # type: ignore
    @SPFSQL_File.setter
    def SPFSQL_File(self, value: str):
        """
        set : SPFSQL_File
        """
        if self._SPFSQL_File is None:
            if value not in ["", None]:
                if Path(value).exists() is False:
                    raise FileNotFoundError(f"File not found - 'SPFSQL file : {value}")
                self._SPFSQL_File = Path(value)
    
    _SPFSQL_File_Archive_Name = None
    @property
    def SPFSQL_File_Archive_Name(self):
        """
        get : SPFSQL_File_Archive_Name
        """
        if self._SPFSQL_File_Archive_Name is None:
            self.__SPFSQL_File_Archive_Name = f"{self.SPFSQL_File.name}.zip"
            self.FilesToCleanUp.append(self.__SPFSQL_File_Archive_Name)
        self.logger.debug(f"get : SPFSQL_File_Archive_Name : {self._SPFSQL_File_Archive_Name}")
        return self.__SPFSQL_File_Archive_Name
    
    _SPFInstance:str = ""
    @property
    def SPFInstance(self)-> str:
        """
        get : SPFInstance
        """
        if (self._SPFInstance in ["", None]):
            self._SPFInstance = f"{int(32767 * random.random() + 1)}"
        self.logger.debug(f"get : SPFInstance : {self._SPFInstance}")
        return self._SPFInstance 
    @SPFInstance.setter
    def SPFInstance(self, value: str):
        """
        set : SPFInstance
        """
        if (self._SPFInstance in [None, ""]):
            if (value not in [None, ""] and value.isdigit() == True):
                self._SPFInstance = f"{value}"
            else:
                self._SPFInstance = f"{int(32767 * random.random() + 1)}"
            self.logger.debug(f"set : SPFInstance : {self._SPFInstance}")

    _hpc_svc_mem_limit_GB: int = None  # type: ignore
    @property
    def hpc_svc_mem_limit_GB(self)->int:
        """
        get : hpc_svc_mem_limit_GB
        """
        if (self._hpc_svc_mem_limit_GB is None):
            self._hpc_svc_mem_limit_GB = 32 # default value
        self.logger.debug(f"get : hpc_svc_mem_limit_GB : {self._hpc_svc_mem_limit_GB}")
        return self._hpc_svc_mem_limit_GB
    @hpc_svc_mem_limit_GB.setter
    def hpc_svc_mem_limit_GB(self, value: int):
        """
        set : hpc_svc_mem_limit_GB
        """
        _tmp = 8 #defualt min
        try:
            _tmp = int(value)
            if _tmp > 128:
                self.logger.debug(f"hpc_svc_mem_limit_GB value > 128 GB set to defualt max 128 GB")
                _tmp = 128
        except Exception as err:
            _tmp = 32 #defulat value
        self._hpc_svc_mem_limit_GB = _tmp
        self.logger.debug(f"set : hpc_svc_mem_limit_GB : {self._hpc_svc_mem_limit_GB}")

    _hpc_svc_timeout_hours: int = None  # type: ignore
    @property
    def hpc_svc_timeout_hours(self)->int:
        """
        get : hpc_svc_timeout_hours
        """
        if (self._hpc_svc_timeout_hours is None):
            self._hpc_svc_timeout_hours = 8 # default
        self.logger.debug(f"get : hpc_svc_timeout_hours : {self._hpc_svc_timeout_hours}")
        return self._hpc_svc_timeout_hours
    @hpc_svc_timeout_hours.setter
    def hpc_svc_timeout_hours(self, value:Union[str,int]):
        """
        set : hpc_svc_timeout_hours
        """
        _tmp = 32 # default min
        try:
            _tmp = int(value)
            if _tmp > 48:
                self.logger.debug(f"hpc_svc_timeout_hours value > 48 hours set to defualt max 48 hours")
                _tmp = 48
        except Exception as err:
            _tmp = 8 # default min value
        self._hpc_svc_timeout_hours = _tmp
        self.logger.debug(f"set : hpc_svc_timeout_hours : {self._hpc_svc_timeout_hours}")

    _GetDataRestAPI_TIMEOUTMINUTES = None
    @property
    def GetDataRestAPI_TIMEOUTMINUTES(self)->int:
        """
        get : GetDataRestAPI_TIMEOUTMINUTES. based on hpc_svc_timeout_hours
        """
        if (self._GetDataRestAPI_TIMEOUTMINUTES is None):
            self._GetDataRestAPI_TIMEOUTMINUTES = self.hpc_svc_timeout_hours * 60
        self.logger.debug(f"get : GetDataRestAPI_TIMEOUTMINUTES : {self._GetDataRestAPI_TIMEOUTMINUTES}")
        return self._GetDataRestAPI_TIMEOUTMINUTES

    #region svc_output_fetch_mode
    _ServiceOutput_FetchMode:str = None   # type: ignore
    _ServiceOutput_FetchMode_valid_values = ["ROBOCOPY-SHARE", "ROBOCOPY", "HTTP-SHARE", "HTTP"]
    _ServiceOutput_FetchMode_default = _ServiceOutput_FetchMode_valid_values[0]
    @property
    def ServiceOutput_FetchMode(self)->str:
        """
        get : ServiceOutput_FetchMode
        """
        if (self._ServiceOutput_FetchMode in ["", None] == True):
            self._ServiceOutput_FetchMode = self._ServiceOutput_FetchMode_default
        self.logger.debug(f"get : ServiceOutput_FetchMode : {self._ServiceOutput_FetchMode}")
        return self._ServiceOutput_FetchMode
    @ServiceOutput_FetchMode.setter
    def ServiceOutput_FetchMode(self, value:str):
        """
        set : ServiceOutput_FetchMode
        """
        if (value in ["", None] == True):
            self._ServiceOutput_FetchMode = self._ServiceOutput_FetchMode_default
            self.logger.debug(f"set : ServiceOutput_FetchMode : '{value}' use default : {self._ServiceOutput_FetchMode}")
        elif(value.strip().upper() not in self._ServiceOutput_FetchMode_valid_values):
            self._ServiceOutput_FetchMode = self._ServiceOutput_FetchMode_default
            self.logger.debug(f"set : ServiceOutput_FetchMode : '{value}' not in valid list : use default : {self._ServiceOutput_FetchMode}")
            self._ServiceOutput_FetchMode
        else:
            self._ServiceOutput_FetchMode = value.strip().upper()
            self.logger.debug(f"set : ServiceOutput_FetchMode : {self._ServiceOutput_FetchMode}")
    #endregion svc_output_fetch_mode

    #region use_output_stage_share
    _use_output_stage_share = None
    @property
    def use_output_stage_share(self):
        """
        get : use_output_stage_share.
        """
        if self._use_output_stage_share is None:
            if (self.ServiceOutput_FetchMode.endswith("-SHARE") is True):
                self._use_output_stage_share = True
            else:
                self._use_output_stage_share = False
        self.logger.debug(f"get: use_output_stage_share : {self._use_output_stage_share}")
        return self._use_output_stage_share
    #endregion use_output_stage_share

    #endregion properties

    #region properties : Svc Response helpers
    _ACTIONEXECUTE_Input_JSON_FileName:str = None
    @property
    def ACTIONEXECUTE_Input_JSON_FileName(self)-> str:
        """
        get : ACTIONEXECUTE_Input_JSON_FileName
        """
        if self._ACTIONEXECUTE_Input_JSON_FileName is None:
            self._ACTIONEXECUTE_Input_JSON_FileName = f"ACTIONEXECUTE_Input_{self.SvcReqRespFileNamePart}.json"
            self.FilesToCleanUp.append(self.ACTIONEXECUTE_Input_JSON_FileName)
        return self._ACTIONEXECUTE_Input_JSON_FileName

    _ACTIONEXECUTE_Output_JSON_FileName: str = None
    @property
    def ACTIONEXECUTE_Output_JSON_FileName(self)-> str:
        """
        get : ACTIONEXECUTE_Output_JSON_FileName
        """
        if self._ACTIONEXECUTE_Output_JSON_FileName is None:
            self._ACTIONEXECUTE_Output_JSON_FileName = f"ACTIONEXECUTE_Output_{self.SvcReqRespFileNamePart}.json"
            self.FilesToCleanUp.append(self.ACTIONEXECUTE_Output_JSON_FileName)
        return self._ACTIONEXECUTE_Output_JSON_FileName

    __ACTIONEXECUTE_Response_FileContent = None
    @property
    def ACTIONEXECUTE_Response_FileContent(self)-> str:
        """
        get : ACTIONEXECUTE_Output_JSON_FileName
        """
        if self.__ACTIONEXECUTE_Response_FileContent is None:
            if Path(self.ACTIONEXECUTE_Output_JSON_FileName).exists() is False:
                errMsg = "No response from SQLPF Service"
                raise Exception(errMsg)
            with open(self.ACTIONEXECUTE_Output_JSON_FileName, "r") as rdr:
                svc_response_file_content = rdr.read()

            if svc_response_file_content.strip() in [""]:
                errMsg = "Invalid response from SQLPF Service"
                raise Exception(errMsg)
            self.__ACTIONEXECUTE_Response_FileContent = svc_response_file_content
        return self.__ACTIONEXECUTE_Response_FileContent

    __ACTIONEXECUTE_Response_JSON = None
    @property
    def ACTIONEXECUTE_Response_JSON(self)-> dict:
        """
        get : ACTIONEXECUTE_Response_JSON
        """
        if self.__ACTIONEXECUTE_Response_JSON is None:
            self.__ACTIONEXECUTE_Response_JSON = json.loads(self.ACTIONEXECUTE_Response_FileContent)
        return self.__ACTIONEXECUTE_Response_JSON
    
    __ACTIONEXECUTE_Response_ExecuteResult = None
    @property
    def ACTIONEXECUTE_Response_ExecuteResult(self)-> dict:
        """
        get : ACTIONEXECUTE_Response_ExecuteResult
        """
        if self.__ACTIONEXECUTE_Response_ExecuteResult is None:
            try:
                self.__ACTIONEXECUTE_Response_ExecuteResult = json.loads(self.ACTIONEXECUTE_Response_JSON['ExecuteResult'])
            except Exception as err:
                self.__ACTIONEXECUTE_Response_ExecuteResult = self.ACTIONEXECUTE_Response_JSON
        return self.__ACTIONEXECUTE_Response_ExecuteResult
    
    __ACTIONEXECUTE_Response_RunStatus:int = None  # type: ignore
    @property
    def ACTIONEXECUTE_Response_RunStatus(self)-> int:
        """
        get : ACTIONEXECUTE_Response_RunStatus
        """
        if self.__ACTIONEXECUTE_Response_RunStatus is None:
            self.__ACTIONEXECUTE_Response_RunStatus = int(self.ACTIONEXECUTE_Response_ExecuteResult['RunStatus'])
        return self.__ACTIONEXECUTE_Response_RunStatus
    __ACTIONEXECUTE_Response_Exception = None
    @property
    def ACTIONEXECUTE_Response_Exception(self)-> str:
        """
        get : ACTIONEXECUTE_Response_Exception
        """
        if self.__ACTIONEXECUTE_Response_Exception is None:
            try:
                self.__ACTIONEXECUTE_Response_Exception = str(self.ACTIONEXECUTE_Response_ExecuteResult['Exception'])
            except Exception as err:
                self.__ACTIONEXECUTE_Response_Exception = ""
            return self.__ACTIONEXECUTE_Response_Exception
    __ACTIONEXECUTE_Response_ConsoleLogFile = None
    @property
    def ACTIONEXECUTE_Response_ConsoleLogFile(self)-> str:
        """
        get : ACTIONEXECUTE_Response_ConsoleLogFile
        """
        if self.__ACTIONEXECUTE_Response_ConsoleLogFile is None:
            self.__ACTIONEXECUTE_Response_ConsoleLogFile, sqlpf_svc_console_output_file_content = self.ACTIONEXECUTE_Response_ExecuteResult['ConsoleLogFile'][0]
            self.WriteBase64ContentToFile(self.__ACTIONEXECUTE_Response_ConsoleLogFile, sqlpf_svc_console_output_file_content)
            self.UnZipFile(self.__ACTIONEXECUTE_Response_ConsoleLogFile)
            self.DelIntermediateFile(self.__ACTIONEXECUTE_Response_ConsoleLogFile)
            self.__ACTIONEXECUTE_Response_ConsoleLogFile = self.__ACTIONEXECUTE_Response_ConsoleLogFile.strip(".zip")

            #region clean up
            svc_output_fetch_mode = None
            del svc_output_fetch_mode
            #endregion clean up
        return self.__ACTIONEXECUTE_Response_ConsoleLogFile
    #endregion properties : Svc Response helpers
    
    def __init__(self, SQLPFSvcEnv:str = "SQLPFAAS_PROD"
                     , SQLPFSvcURL:str = ""
                     , SQLPF_CMD_Args_List:list = []
                     , SPFSQL_File:str = ""
                     , SPFInstance:str = ""
                     , SPFLogLevel:str = "ERROR"
                     , required_input_files_list_csv:str or list = ""
                     , required_output_files_folder_list_csv:str or list = ""
                     , optional_output_files_folder_list_csv:str or list = ""
                     , display_svc_console_flag:bool = False
                     , copy_log_files_flag:bool = False # True --> fetch the query execution logs from HPC server
                     , continue_on_error:bool = False
                     , begin_hpc_svc_ver:str = "2"
                     , ClientAppName:str = __file__
                     , SPFExe:str ="."
                     , hpc_mem_limit_GB:int = 32
                     , hpc_svc_timeout_hours:int = 8
                     , user_idsid:str = ""
                     , svc_output_fetch_mode:str ="HTTP"
                     , loggerName:str = "SPFLib"
                     , logFileName:str = "SvcClient.log" # specify absolute path if location of log file needs to be created in a different location
                     , async_status_check_sleep_time_seconds:int =30
                     , SPF_CL_Args: dict={} ##command-line arguments e.g., {'CL_VAR_NAME' : 'VAR_VALUE'}. key should always begin with 'CL_'
                     , SPF_LogSvc_Env: str = "SQLPF_LogSvc" #SPF logger Svc Environment Prod: 'SQLPF_LogSvc', Dev : 'SQLPF_LogSvc_CH_Dev'
                     , copy_svc_console_flag: bool = False # True --> fetch the query execution console output from HPC server
                     ): 
        #region locals
        super().__init__()
        
        _time_stamp = str(datetime.now().strftime("%Y%m%d%H%M%S")) # type: ignore #G_DT_FORMAT_YYYYMMDDhhnnss : '20151009183105'
        #create the logger
        self._get_logger(loggerName = loggerName, logFileName = logFileName, logLevel = SPFLogLevel)
        
        self.gSPFSWSHDir = self.__gSPFSWSHDir
        self.FilesToCleanUp = [] #Track files that need to deleted post execution...if loglevel == 'DEBUG' files will not be deleted
        
        #region logSvc vars
        self.logCallingScript = Path(sys.argv[0]).name
        self.LogRec = [f"SvcClient_{__version__}", f"{self.logCallingScript}"] #build logging record
        #endregion logSvc vars

        #endregion locals
        self.logger.debug(f"SQLPFSvcEnv : {SQLPFSvcEnv}")
        self.logger.debug(f"SQLPFSvcURL : {SQLPFSvcURL}")
        self.logger.debug(f"SQLPF_CMD_Args_List : {SQLPF_CMD_Args_List}")
        self.logger.debug(f"SPFSQL_File : {SPFSQL_File}")
        self.logger.debug(f"SPFInstance : {SPFInstance}")
        self.logger.debug(f"SPFLogLevel : {SPFLogLevel}")
        self.logger.debug(f"required_input_files_list_csv : {required_input_files_list_csv}")
        self.logger.debug(f"required_output_files_folder_list_csv : {required_output_files_folder_list_csv}")
        self.logger.debug(f"optional_output_files_folder_list_csv : {optional_output_files_folder_list_csv}")
        self.logger.debug(f"display_svc_console_flag : {display_svc_console_flag}")
        self.logger.debug(f"copy_log_files_flag : {copy_log_files_flag}")
        self.logger.debug(f"continue_on_error : {continue_on_error}")
        self.logger.debug(f"begin_hpc_svc_ver : {begin_hpc_svc_ver}")
        self.logger.debug(f"ClientAppName : {ClientAppName}")
        self.logger.debug(f"SPFExe : {SPFExe}")
        self.logger.debug(f"hpc_mem_limit_GB : {hpc_mem_limit_GB}")
        self.logger.debug(f"hpc_svc_timeout_hours : {hpc_svc_timeout_hours}")
        self.logger.debug(f"user_idsid : {user_idsid}")
        # self.logger.debug(f"use_output_stage_share : {use_output_stage_share}")
        self.logger.debug(f"svc_output_fetch_mode : {svc_output_fetch_mode}")
        self.logger.debug(f"async_status_check_sleep_time_seconds : {async_status_check_sleep_time_seconds}")
        self.logger.debug(f"SPF_CL_Args : {SPF_CL_Args}")
        self.logger.debug(f"SPF_LogSvc_Env : {SPF_LogSvc_Env}")
        self.logger.debug(f"copy_svc_console_flag : {copy_svc_console_flag}")

        self.gSPFExe = SPFExe # set this property first...location of SQLPathfinder install or location of SPFBin folder
        self.SQLPFSvcEnv = SQLPFSvcEnv
        self.SQLPFSvcURL = SQLPFSvcURL
        self.SPFInstance = SPFInstance 
        self.SPFLogLevel = SPFLogLevel
        self.SQLPF_CMD_Args_List = SQLPF_CMD_Args_List #ensure self.SPFInstance + self.SPFLogLevel is set first
        self.SPFSQL_File = SPFSQL_File
        self.required_input_files_list_csv = required_input_files_list_csv
        self.required_output_files_folder_list_csv = required_output_files_folder_list_csv
        self.optional_output_files_folder_list_csv = optional_output_files_folder_list_csv
        self.display_svc_console_flag = display_svc_console_flag
        self.copy_log_files_flag = copy_log_files_flag
        self.continue_on_error = continue_on_error
        self.begin_hpc_svc_ver = begin_hpc_svc_ver
        self.ClientAppName = ClientAppName
        self.hpc_svc_mem_limit_GB = hpc_mem_limit_GB
        self.hpc_svc_timeout_hours = hpc_svc_timeout_hours
        self.user_idsid = user_idsid.lower()
        # self.use_output_stage_share = use_output_stage_share
        self.ServiceOutput_FetchMode = svc_output_fetch_mode.upper()
        self.async_status_check_sleep_time_seconds = async_status_check_sleep_time_seconds
        self.SPF_CL_Args = SPF_CL_Args
        self.SPF_LogSvc_Env = SPF_LogSvc_Env
        self.SvcReqRespFileNamePart = f"{self.SPFInstance}" #_{_time_stamp}"
        self.copy_svc_console_flag = copy_svc_console_flag
        self.ExecuteCycleDone = False
        self.displayConsMsg = True
        return 
    
    def Execute(self):
        """
        Entry point into Client handler
        """
        try:
            self.ConsoleWithTimeStamp(f"Starting SQLPathfinder HPC Service Client V{__version__}")
            self.LogRec.append('ex')
            self._invokeExecuteAsync(self.SQLPFSvcURL)
            self.ExecuteCycleDone = True
        except Exception as err:
            raise
        finally:
            self.DelListOfFiles(self.FilesToCleanUp, SkipDeleteInDebugMode=True)
            self.ConsoleDoneWithTimeStamp()
            # print("ASync Execute calling record_spf ")
            self._record_SPF()

    def _invokeExecuteAsync(self, web_api_url: str):
        """
        invoke aysnc execute
        """
        mySVCAction = "EXECUTESPFSQLASYNC" #"EXECUTESPFSQLASYNC"
        self.logger.info(f"start : {mySVCAction}")
        if self.required_input_files_list_csv is None:
            _required_input_files_archive = None
        else:
            _required_input_files_archive = self.getFileContent(self.create_archive_from_file_list(self.required_input_files_list_csv, self.required_input_files_list_csv_archive_name, checkRelativePathOfArchivingFile=True), AsBase64String=True)
            self.FilesToCleanUp.append(self.required_input_files_list_csv_archive_name)

        if self.required_output_files_folder_list_csv is None:
            _required_output_files_folder_list_csv_archive = None
        else:
            _required_output_files_folder_list_csv_archive = self.getFileContent(self.ZipFile(self.required_output_files_folder_list_csv, self.required_output_files_folder_list_csv_archive_name), AsBase64String=True)
            self.FilesToCleanUp.append(self.required_output_files_folder_list_csv_archive_name)

        if self.optional_output_files_folder_list_csv is None:
            _optional_output_files_folder_list_csv_archive = None
        else:
            _optional_output_files_folder_list_csv_archive = self.getFileContent(self.ZipFile(self.optional_output_files_folder_list_csv, self.optional_output_files_folder_list_csv_archive_name), AsBase64String=True)
            self.FilesToCleanUp.append(self.optional_output_files_folder_list_csv_archive_name)

        _spfsql_file_archive = self.getFileContent(self.ZipFile(self.SPFSQL_File, self.SPFSQL_File_Archive_Name, RetainRelativePathInArchive=False), AsBase64String=True)

        SvcInput_args1 = {'required_input_files_archive' : [self.required_input_files_list_csv_archive_name, _required_input_files_archive] # file with list of required input files + folders that are required for query execution at Svc 
                    , 'required_output_files_folder_list_csv_archive' : [self.required_output_files_folder_list_csv_archive_name, _required_output_files_folder_list_csv_archive] #file with list of required output files + folders that will be copied back to client
                    , 'optional_output_files_folder_list_csv_archive' : [self.optional_output_files_folder_list_csv_archive_name, _optional_output_files_folder_list_csv_archive] #file with list of optional output files + folders that will be copied back to client
                    , 'display_svc_console_flag' : self.display_svc_console_flag #default dont display to client console
                    , 'copy_log_files_flag' : self.copy_log_files_flag #default dont copy log files
                    , 'continue_on_error' : self.continue_on_error #default raise error 
                    , 'begin_hpc_svc_ver' : self.begin_hpc_svc_ver #default is V1
                    , 'spfsql_file' : [self.SPFSQL_File_Archive_Name, _spfsql_file_archive]
                    , 'use_output_stage_share' : self.use_output_stage_share
                    , 'hpc_svc_mem_limit_GB' : self.hpc_svc_mem_limit_GB # HPC svc peak memory limit 
                    # , 'hpc_svc_timeout_hours' : self.hpc_svc_timeout_hours #HPC svc timeout
                    , 'sh_job_name' : self.SHjobName # will have Scripthost job name if originating from SH
                    , 'sh_job_entryid' : self.SHentryID # will have Scripthost job EntryID if originating from SH
                    }

        SvcExecInput = self.SvcExecInput_template #this is copy of the template
        SvcExecInput['Input']['Action'] = f"{mySVCAction}"
        SvcExecInput['Input']['use_output_stage_share'] = f"{self.use_output_stage_share}"
        SvcExecInput['Input']['hpc_svc_execution_time_limit_minutes'] = self.hpc_svc_timeout_hours * 60 #HPC svc timeout in minutes
        SvcExecInput['Input']['hpc_svc_memory_limit_GB'] = self.hpc_svc_mem_limit_GB # HPC svc peak memory limit
        SvcExecInput['Input']['ARGS'] = f"{json.dumps(SvcInput_args1)}"
        #self.InvokeService(self.ACTIONEXECUTE_Input_JSON_FileName, SvcExecInput, self.ACTIONEXECUTE_Output_JSON_FileName, web_api_url, displayConsMsg=True) 
        
        staged_output_file = f"ACTIONEXECUTE_Output_{self.SvcReqRespFileNamePart}_staged.json"
        self.FilesToCleanUp.append(staged_output_file)
        self.InvokeService(self.ACTIONEXECUTE_Input_JSON_FileName, SvcExecInput, staged_output_file, web_api_url, displayConsMsg=self.displayConsMsg)
        self.fetchExecutionOutput(staged_output_file)

        self.processExecutionOutput()
        self.validateExecutionResponse()

        self.logger.info(f"done : {mySVCAction}")
    #END: def __invokeExecute

    def fetchExecutionOutput(self, staged_execute_response_file):
        """
        wait for execution to complete and get the execution output 
        """
        #fetchHandlerMapDict={ServiceOutput_FetchMode : (Handler_Method, Response_JSON_Key_Name)}
        fetchHandlerMapDict = {"ROBOCOPY-SHARE" : ("GetFile_Using_Robocopy", 'staged_output_file_path')
                              ,"ROBOCOPY" : ("GetFile_Using_Robocopy", 'staged_output_file_path_wd')
                              ,"HTTP-SHARE" : ("GetFile_Using_Webcopy", 'staged_output_file_httppath')
                              ,"HTTP" : ("GetFile_Using_Webcopy", 'staged_output_file_httppath_wd')
                              }
        try:
            svc_response_file_content = None
            with open(staged_execute_response_file, "r") as rdr:
                svc_response_file_content = json.load(rdr)
            
            self.logger.debug(f"svc_response_file_content : \n{json.dumps(svc_response_file_content, indent=2)}")
            Response_JSON = svc_response_file_content["ExecuteResult"]
            runStatus = Response_JSON["RunStatus"]

            handler_name, key_name = fetchHandlerMapDict[self.ServiceOutput_FetchMode.upper()]
            self.logger.debug(f"handler_name : {handler_name}, key_name : {key_name}")

            staged_output_path = Response_JSON[key_name]
            status_check_url = Response_JSON['status_check_url']
            _use_output_stage_share = Response_JSON['use_output_stage_share']
            _sid = Response_JSON['Session_ID']

            self.ConsoleWithTimeStamp(f"  Execution started in HPC environment with SID : {_sid}")            
            #check status of execution 
            status_completed = self.MonitorExecuteStatusCompleted(status_url=status_check_url
                        , sleep_time_seconds_between_retry=self.async_status_check_sleep_time_seconds)
            
            fetchHandler = getattr(self, handler_name)

            self.logger.debug(f"fetchHandler : {fetchHandler}")
            try:
                #invoke fetch handler
                fetchHandler(staged_output_path, self.ACTIONEXECUTE_Output_JSON_FileName)
            except FileNotFoundError as fileNotFoundErr:
                if self.ServiceOutput_FetchMode.upper() != "HTTP":
                    #try downloading directly from node where query executed over HTTP
                    handler_name, key_name = fetchHandlerMapDict["HTTP"]
                    fetchHandler = getattr(self, handler_name)
                    staged_output_path = Response_JSON[key_name] #get the URL to download the file from
                    self.logger.debug(f"fetchHandler : {fetchHandler}")
                    #invoke fetch handler
                    fetchHandler(staged_output_path, self.ACTIONEXECUTE_Output_JSON_FileName)
                else:
                    raise
        except Exception as err:
            self.logger.error(f"Error while fetching execution output : {err}")
            raise

    def GetFile_Using_Robocopy(self, _source_file_path:str, destination_file_name:str):
        """
        Copy the final service output using RoboCopy
        """
        self.LogRec.append('rcopy')
        svc_output_file_found = False
        retryCount = 0
        self.logger.debug(f"_source_file_path : {_source_file_path}")
        if (_source_file_path in ["", None]):
                raise Exception(f"Invalid File path : '{_source_file_path}'")
        source_file_path = Path(_source_file_path)

        while (svc_output_file_found is False and retryCount <= self.__gRetryMaxCount):
            retryCount = retryCount + 1
            try:
                if source_file_path.exists() is True:
                    self.ConsoleWithTimeStamp(f"   Found output file @ '{source_file_path}'...start copying")
                    self.SPFRoboCopy(source_file_path.name, str(source_file_path.parent), ".", 2, 10, "Y", "", "Y")
                    shutil.copy(source_file_path.name, destination_file_name)
                    # self.DelAFile(source_file_path)
                    svc_output_file_found = True
                else:
                    errMsg = f"Service output not found :'{source_file_path}'"
                    raise FileNotFoundError(errMsg)
            except FileNotFoundError as FileNotFoundErr:
                #check for retry
                if retryCount < self.__gRetryMaxCount:
                    errMsg = f"{FileNotFoundErr}... retrying '{retryCount}' of '{self.__gRetryMaxCount}' retries."
                    self.ConsoleWithTimeStamp(errMsg)
                    retryCount = retryCount + 1
                    time.sleep(self.__gSleepBetweenRetrySeconds)
                else:
                    errMsg = f"{FileNotFoundErr}... even after '{retryCount}' retries. Exiting"
                    raise FileNotFoundError(errMsg)
        #END: while (svc_output_file_found is False and retryCount <= self.__gRetryMaxCount):
        # self.Console()
        # self.ConsoleDoneWithTimeStamp()
        return svc_output_file_found
    #END : def GetFile_Using_Robocopy

    def GetFile_Using_Webcopy(self, _source_file_url:str, destination_file_name:str, processZipContent=False):
        """
        Copy the final service output using WebCopy -- over http
        """
        self.LogRec.append('wcopy')
        from requests_kerberos import HTTPKerberosAuth #ensure this package is installed 

        svc_output_file_found = False
        myChunkSize = 1024 * 1024
        retryCount = 0
        
        try:
            self.logger.debug(f"_source_file_url : {_source_file_url}")
            if (_source_file_url in ["", None]):
                raise Exception(f"Invalid URL : '{_source_file_url}'")
            getResponse = requests.request("GET", _source_file_url, verify=False, stream=True) #, auth=HTTPKerberosAuth())
            getResponse.raise_for_status()
            
            resp_content_type_is_zip = True if getResponse.headers['Content-Type'] == 'application/x-zip-compressed' else False

            self.logger.debug(f"getResponse.status_code : {getResponse.status_code}")
            if [resp_content_type_is_zip, processZipContent] == [True, True]:
                #zipped content...append output file name with '.zip'
                destination_file_name = f"{destination_file_name}.zip"

            with open(destination_file_name, "wb") as contentWrtr:
                # contentWrtr.write(response.content.decode())
                for chunk in getResponse.raw.stream(myChunkSize, decode_content=False):  # type: ignore
                    if chunk:
                        contentWrtr.write(chunk)

            if [resp_content_type_is_zip, processZipContent] == [True, True]:
                #zipped content...unzip & delete the .zip file
                self.UnZipFile(destination_file_name)
                self.DelAFile(destination_file_name)

        except HTTPError as httpErr:
            self.logger.exception(f"httpErr.response.status_code : {httpErr.response.status_code}", httpErr)
            raise
        except Exception as err:
            self.logger.exception(f"Error in GetFile_Using_Webcopy", err)
            raise
        # self.Console()
        # self.ConsoleDoneWithTimeStamp()
        return svc_output_file_found
    #END : def GetFile_Using_Webcopy

    def processExecutionOutput(self):
        """
        process execution output file
        """
        #extract file archives sent by service in response
        for ArchItemKey in ['OutputFiles', 'SPFLogFilesArchive']:
            for ArchiveItem in self.ACTIONEXECUTE_Response_ExecuteResult[ArchItemKey]:
                ArchiveItem_name, ArchiveItem_Content = ArchiveItem
                self.WriteBase64ContentToFile(ArchiveItem_name, ArchiveItem_Content)
                self.UnZipFile(ArchiveItem_name)
                self.DelIntermediateFile(ArchiveItem_name)
                ArchiveItem_name, ArchiveItem_Content = (None, None) # dispose temp vars
    #END :  def processExecutionOutput

    def validateExecutionResponse(self):
        """
        validate the execution response 
        """
        #region nested function
        def get_svc_console_output():
            return f"\n\n{80*'='}\nSQLPFaaS-HPC Console output : Begin\n{80*'='}\n\n{self.getFileContent(self.ACTIONEXECUTE_Response_ConsoleLogFile).decode()}\n\n{80*'='}\nSQLPFaaS-HPC Console output : End\n{80*'='}\n\n"
        #endregion nested function

        if self.ACTIONEXECUTE_Response_RunStatus != 0:
            errMsg = ("SQLPF Service error occured..."
                      f"\n  {self.ACTIONEXECUTE_Response_Exception}")
            if self.display_svc_console_flag is False:
                errMsg = (f"{errMsg}\n"
                          f"   for details, see log file : '{str(Path(self.ACTIONEXECUTE_Response_ConsoleLogFile).resolve())}'")
            else:
                errMsg = (f"{errMsg}\n{get_svc_console_output()}")
            raise Exception(errMsg)
            
        if self.copy_svc_console_flag is True:
            if self.display_svc_console_flag is False: 
                consoleMsg = (f"Service Console output saved to file : '{str(Path(self.ACTIONEXECUTE_Response_ConsoleLogFile).resolve())}'")
            else:
                consoleMsg = get_svc_console_output()
            self.Console(consoleMsg)
        else:
            # Service console output file will be deleted if there was no error on service and SPFLoglevel != 'DEBUG'
            self.FilesToCleanUp.append(self.ACTIONEXECUTE_Response_ConsoleLogFile) 
    #END :  def validateExecutionResponse

    def MonitorExecuteStatusCompleted(self, status_url, sleep_time_seconds_between_retry:int=30)-> bool:
        """
        Monitor for execution status
        """
        from requests_kerberos import HTTPKerberosAuth #ensure this package is installed 

        status_is_completed = False
        total_waited_time_in_secs = 0
        sleep_time_in_secs = sleep_time_seconds_between_retry
        timeout_in_secs = self.hpc_svc_timeout_hours * 3600
        self.ConsoleWithTimeStamp(f"  Checking execution status...please wait (Timeout : {self.hpc_svc_timeout_hours} hours), retrying every : {sleep_time_seconds_between_retry} seconds. Elapsed time : '{timedelta(seconds=total_waited_time_in_secs)}'")
        while (status_is_completed == False):
            try:
                statusCheckResponse = requests.request("GET", status_url, verify=False, auth=HTTPKerberosAuth())
                statusCheckResponse.raise_for_status()
                statusText:str = statusCheckResponse.text
                statusCode:int = statusCheckResponse.status_code
                self.logger.debug(f"statusCode : {statusCode}; statusText : {statusText}")
                if (statusCode == 200): #Completed
                    status_is_completed = True
                    self.Console("")
                    self.ConsoleWithTimeStamp(f"  ... Status: '{statusText}' ...Elapsed time : '{timedelta(seconds=total_waited_time_in_secs)}'. Getting output.")
                elif(statusCode == 202): #Inprogress
                    status_is_completed = False

                    if (total_waited_time_in_secs > timeout_in_secs):
                        errMsg = f"Error : Service Timeout '{self.hpc_svc_timeout_hours} hours' occured."
                        raise Exception(errMsg)
                    self.ConsoleWithTimeStamp(f"  ... Status: '{statusText}'...Elapsed time : '{timedelta(seconds=total_waited_time_in_secs)}'", overwrite = not self.SHisSHEntry)    
                    total_waited_time_in_secs = total_waited_time_in_secs + sleep_time_in_secs
                    time.sleep(sleep_time_in_secs)
                else:
                    errMsg = f"Error : Unkown status {statusCode}: {statusText} recieved."
                    raise Exception(errMsg)
            except HTTPError as httpErr:
                self.logger.exception(httpErr.response.status_code)
                raise
            except Exception as err:
                self.logger.exception(err)
                raise
        self.logger.debug(f"MonitorExecuteStatusCompleted : status_is_completed = {status_is_completed}")
        return status_is_completed
    #END : def MonitorExecuteStatusCompleted

    def InvokeService(self, json_input_file_name: str=None, myPayload: str="", web_api_outputFile: str=None, web_api_url: str=None, displayConsMsg=False, returnResponseObj=False, reqTimeout=None)-> None:
        """
        use py requests + kerberos
        """
        retryHTTPCodes = [503 #The service is unavailable. -- could be due to network or Svc might be in start up process
                         ]

        retryMaxcount = 3
        retrySleepInSeconds = 60
        retryCount = 0
        InvokeSvcDone = False
        try:
            from requests_kerberos import HTTPKerberosAuth #ensure this package is installed 
        except ImportError as importErr:
            self.logger.exception(f"unable to import 'HTTPKerberosAuth' from 'requests_kerberos'. Error: {importErr}")
            raise 

        try:
            self.logger.debug(f"web_api_url : {web_api_url}")
            self.logger.debug(f"len(myPayload) : {len(myPayload)}")
            self.logger.debug(f"web_api_outputFile : {web_api_outputFile}")
            # print(f"displayConsMsg : {displayConsMsg}; self.SPFLogLevel : {self.SPFLogLevel}")
            # if displayConsMsg is True or self.SPFLogLevel == "DEBUG":
            #     consMsg = (f"RestAPI URL : {web_api_url}"
            #              f"\nOutput File : {web_api_outputFile}")
            #     self.ConsoleWithCons80(consMsg, logThis=False)
            payload = json.dumps(myPayload, separators=(',',':'))
            self.logger.debug("payload : " + payload)
            headers = {
            'Content-Type': 'application/json'
            ,'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'
            }
            # try:
            #     from http.client import HTTPConnection
            # except importErr:
            #     from httplib import HTTPConnection
            # logging.basicConfig(level=logging.DEBUG)
            # HTTPConnection.debuglevel = 1
            # requests_log = logging.getLogger("urllib3")
            # requests_log.setLevel(logging.DEBUG)
            # requests_log.propagate = False
            while (InvokeSvcDone is False and retryCount <= retryMaxcount):
                retryCount = retryCount + 1
                try:
                    response = requests.request("POST", web_api_url, headers=headers, data = payload, auth=HTTPKerberosAuth(), verify=False, stream=True, timeout=reqTimeout)#timeout=(300, self.GetDataRestAPI_TIMEOUTMINUTES * 60))
                    self.logger.debug(response.headers)

                    if returnResponseObj is True:
                        #client is requesting to get the raw response object. Client is expected to take care of handling the response
                        return response

                    #continue to write the response output to file
                    response.raise_for_status()
                    resp_content_len = int(response.headers['content-length'])
                    resp_content_type = response.headers['Content-Type'] 

                    self.logger.debug(f"response.status_code : {response.status_code}; resp_content_len : {resp_content_len}; resp_content_type : {resp_content_type}")
                    if (resp_content_type == 'application/x-zip-compressed'):
                        #zipped content...append output file name with '.zip'
                        web_api_outputFile = f"{web_api_outputFile}.zip"

                    self.logger.debug(f"writing response to file : {web_api_outputFile}")
                    with open(web_api_outputFile, "wb") as contentWrtr:
                        # contentWrtr.write(response.content.decode())
                        for chunk in response.raw.stream(1024, decode_content=False):
                            if chunk:
                                contentWrtr.write(chunk)

                    self.logger.debug(f"completed writing response to file : {web_api_outputFile}")
                    
                    if (resp_content_type == 'application/x-zip-compressed'):
                        #zipped content...unzip & delete the .zip file
                        self.UnZipFile(web_api_outputFile)
                        self.DelAFile(web_api_outputFile)

                    self.logger.debug("Execution task accepted")
                    InvokeSvcDone = True
                # except requests.exceptions.ConnectionError as reqConnError:
                #     InvokeSvcDone = True
                #     self.ConsoleWithTimeStamp(reqConnError.args)
                except requests.exceptions.ReadTimeout as readTimeoutErr:
                    InvokeSvcDone = True
                    errMsg = "Timeout error has occured"
                    raise Exception(errMsg)
                except HTTPError as httpErr:
                    self.logger.exception(httpErr)
                    #errMsg = f"HTTPError : {httpErr.response.status_code} : {} : {httpErr.response.text}"
                    httpStatusCode = httpErr.response.status_code
                    if (httpStatusCode in retryHTTPCodes):
                        self.ConsoleWithTimeStamp(httpErr)
                        if (retryCount <= retryMaxcount):
                            errMsg = f"   ...Retrying {retryCount} of {retryMaxcount} with wait between retry : '{retrySleepInSeconds}' seconds"
                            self.ConsoleWithTimeStamp(errMsg)
                            time.sleep(retrySleepInSeconds)
                        else:
                            InvokeSvcDone = True
                            errMsg = f"   ...Maximum retry of {retryCount} completed. Exiting"
                            self.ConsoleWithTimeStamp(errMsg)
                            raise Exception(errMsg)
                    else:
                        InvokeSvcDone = True
                        errMsg = f"HTTPError : {httpErr}"
                        if int(httpErr.response.headers['content-length'] ) > 0:
                            resp_content = httpErr.response.content.decode()
                            try:
                                if httpErr.response.headers['Content-Type'].startswith("application/json"):
                                    # JSON 
                                    _tmp = json.loads(resp_content)
                                    errMsg = f"{errMsg}\n{json.dumps(_tmp, indent=2)}"
                                    del _tmp
                                else: 
                                    errMsg = f"{errMsg}\n{resp_content}"
                                del resp_content
                            except Exception as err:
                                raise Exception(errMsg)
                        #END : if int(httpErr.response.headers['content-length'] ) > 0:
                        raise Exception(errMsg)
                    #END: if (httpStatusCode in retryHTTPCodes and retryCount < retryMaxcount):
                except Exception as err:
                    InvokeSvcDone = True
                    self.logger.exception(err)
                    raise
            #END : while (InvokeSvcDone is False and retryCount <= retryMaxcount):
        except Exception as err:
            self.logger.exception(err)
            raise
    #END: InvokeServicePyReq
       
    def getFileContent(self, FileToReadContent: str, AsBase64String: bool=False, CompressFileContent: bool = False)-> str:
        """
        return the contents of file & optionally as a base64 string
        """
        sFile_content_as_base64Str = ""
        self.logger.debug(f"FileToReadContent : {FileToReadContent}")
        self.logger.debug(f"AsBase64String : {AsBase64String}")
        if FileToReadContent in ["", None]:
            raise Exception("Invalid value '{FileToReadContent}', expecting a file path")
        if Path(FileToReadContent).exists() is False:
            raise FileNotFoundError(f"File not found : '{str(Path(FileToReadContent).resolve())}'") 
        File_Content = ""
        with open(FileToReadContent, 'rb') as FileRdr:
            File_Content = FileRdr.read()
            if CompressFileContent is True:
                File_Content = self.zipString(File_Content)
            if AsBase64String is True:
                File_Content = base64.b64encode(File_Content).decode()
            
        return File_Content
    #END : def getFileContent

    #END : def getSvcConsoleOutput
    def WriteBase64ContentToFile(self, targetFileName: str, base64Content: str):
        """
        """
        with open(Path(targetFileName).name, "wb") as conWrtr:
                conWrtr.write(base64.b64decode(base64Content))
    #END : def WriteBase64ContentToFile

    def UnZipFile(self, ArchiveFileName: str):
        """
        """
        self.logger.debug(f"ArchiveFileName : {ArchiveFileName}")
        ArchiveFileName_PathObj = Path(ArchiveFileName)
        if ArchiveFileName_PathObj.exists() is False:
            raise FileNotFoundError(f"File not found : '{str(ArchiveFileName_PathObj.resolve())}'")
        if ArchiveFileName_PathObj.is_file() is False:
            raise ValueError(f"Path provided is not a file : '{str(ArchiveFileName_PathObj.resolve())}'")
        #Archive exists and is a file...continue with unzip
        with zipfile.ZipFile(Path(ArchiveFileName_PathObj).name, "r") as zRdr:
            zRdr.extractall()
    #END : def UnZipFile

    def ZipFile(self, FileToZip: Path, ArchiveName: str = None, RetainRelativePathInArchive=True) -> Path:
        """
        create zip file of 'FileToZip' using the 'ArchiveName'
        Input:
        1. FileToZip: Path : Path Object of source file to zip
        2. ArchiveName : target archive file name. If None...then use 'FileToZip.zip' 
        """
        #region locals
        #endregion locals
        self.logger.debug(f"FileToZip : {FileToZip}")
        self.logger.debug(f"ArchiveName : {ArchiveName}")
        if FileToZip.exists() is False:
            errMsg = f"File not found : {FileToZip}"
            raise FileNotFoundError(errMsg)

        if ArchiveName in ["", None]:
            ArchiveName = f"{FileToZip.name}.zip"
            self.logger.debug(f"updated ArchiveName : {ArchiveName}")

        with zipfile.ZipFile(ArchiveName, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True, compresslevel=9) as zWrtr:            
            if RetainRelativePathInArchive is True:
                # below args : (FileName_used_inside_archive, Path_of_file_add_to_archive) -- this is required to maintain the folder structure
                zWrtr.write(FileToZip, FileToZip.relative_to(Path(".")))
            else:
                # folder structure is not maintained
                zWrtr.write(FileToZip, FileToZip.name)
        # if self.SPFLogLevel == "DEBUG":
        #     with zipfile.ZipFile(ArchiveName, "r") as zRdr:
        #         zRdr.printdir()
        return ArchiveName
    #END : def ZipFile

    def zipString(self, inputStringToCompress) : 
        """
        ' Helper function compress a string
        ' Note: on error : return input string as is 
        """
        try :
            self.logger.info(f"len(inputStringToCompress) : {len(inputStringToCompress)}")
            #self.logger.debug(f"inputStringToCompress : {inputStringToCompress}")
            if type(inputStringToCompress) is str:
                inputStringToCompress = inputStringToCompress.encode()
            compressedOutputString = base64.standard_b64encode(zlib.compress(inputStringToCompress,2)) #updated to encode to base64string -- support gzipstream from .net
            del inputStringToCompress
            self.logger.info(f"len(compressedOutputString) : {len(compressedOutputString)}")
            #self.logger.debug(f"compressedOutputString : {compressedOutputString}".)
            return compressedOutputString
        except Exception as err:
            self.logger.warn(f"Error in zipString : {err}")
            return inputStringToCompress #if error return back input string
    #END : def zipString

    def create_archive_from_file_list(self, FileWith_FilesFolders_toArchive: str, ArchiveName: str = None, OptionalList: bool=False, checkRelativePathOfArchivingFile: bool=True)-> Path:
        """
        create a zip from file containing list of files & folders to be archived
        Input:
        1. FileWith_FilesFolders_toArchive: str : File where each row is a file or folder that needs to be archived
        2. ArchiveName: str : target archive file name
        3. OptionalList: bool : If True --> files/folders in List are optional...dont raise error if not found
        4. checkRelativePathOfArchivingFile : bool : True -- Error if absolute path is specified for file being archived and is not relative to current work directory
        Output
        1. ArchiveName: Path : return the path object of the Archive file. 
        """
        #region locals
        #endregion locals
        self.logger.debug(f"FileWith_FilesFolders_toArchive : {FileWith_FilesFolders_toArchive}")
        self.logger.debug(f"ArchiveName : {ArchiveName}")

        FileWith_FilesFolders_toArchive_pathObj = Path(FileWith_FilesFolders_toArchive)
        if FileWith_FilesFolders_toArchive_pathObj.exists() is False:
            errMsg = f"File not found : {FileWith_FilesFolders_toArchive}"
            raise FileNotFoundError(errMsg)
        if ArchiveName in ["", None]:
            ArchiveName = f"{FileWith_FilesFolders_toArchive_pathObj.name}.zip"
            self.logger.debug(f"updated ArchiveName : {ArchiveName}")

        ArchiveName_PathObj = Path(ArchiveName)
        if ArchiveName_PathObj.exists() is True:
            try:
                os.remove(ArchiveName)
                self.logger.debug(f"Deleted existing archive file : {str(ArchiveName_PathObj.resolve())}")
            except Exception as err:
                errMsg = f"Error while delete existing archive file : {str(ArchiveName_PathObj.resolve())}. {err}"
                self.logger.exception(errMsg)
                raise Exception(errMsg)

        listOfFilesFolders = []
        with open(FileWith_FilesFolders_toArchive_pathObj, "r") as rdr:
            for line in rdr:
                listOfFilesFolders.append(line.strip())
        self.logger.debug(f"\nFiles & Folders To Archive : {listOfFilesFolders}\n")

        list_of_files_to_archive = [] # (file_path_object, file_relative_path)

        for ffItem in listOfFilesFolders:
            self.logger.debug(f"Archiving Item : {ffItem}")
            _ffItem_PathObj = Path(ffItem)
            if _ffItem_PathObj.exists() is False:
                if OptionalList is True:
                    continue # item not found and is optional...dont raise error go next item
                errMsg = f"File Not found : {str(_ffItem_PathObj.resolve())}"
                raise FileNotFoundError(errMsg)

            if _ffItem_PathObj.is_dir() is True:
                self.logger.debug(f"Archiving Item is a folder, getting all files")
                _files_in_folder = _ffItem_PathObj.rglob("*")
                for fileItem in _files_in_folder:
                    self.logger.debug(f"Item inside folder : {fileItem}")
                    list_of_files_to_archive.append([fileItem, fileItem])
            elif _ffItem_PathObj.is_file() is True:
                self.logger.debug(f"Archiving Item is a file")
                if checkRelativePathOfArchivingFile is True:
                    # check if _ffItem_PathObj is relative current WD...if not then error
                    if _ffItem_PathObj.absolute().is_relative_to(Path.cwd()) is False:
                        errMsg = f"File '{_ffItem_PathObj.absolute()}' path should be a relative path to '{Path.cwd()}'"
                        raise Exception(errMsg)
                    list_of_files_to_archive.append([_ffItem_PathObj, _ffItem_PathObj.absolute().relative_to(Path.cwd())])
                else:
                    if _ffItem_PathObj.is_absolute() is False:
                        list_of_files_to_archive.append([_ffItem_PathObj, _ffItem_PathObj.absolute().relative_to(Path.cwd())])
                    else:
                        list_of_files_to_archive.append([_ffItem_PathObj, _ffItem_PathObj.name])
                
        self.logger.debug(f"list_of_files_to_archive : {list_of_files_to_archive}")
        for fileiTEM_To_Archive in list_of_files_to_archive:
            self.logger.debug(f"Add to archive : {fileiTEM_To_Archive}")
            with zipfile.ZipFile(ArchiveName, "a", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zWrtr:
                # below args : (FileName_used_inside_archive, Path_of_file_add_to_archive)
                zWrtr.write(fileiTEM_To_Archive[0], fileiTEM_To_Archive[1])

        if self.SPFLogLevel == "DEBUG":
            with zipfile.ZipFile(ArchiveName, "r") as zRdr:
                zRdr.printdir()
        return ArchiveName_PathObj
    #END : create_archive_from_file_list()

    def DelIntermediateFile(self, IntFile):
        """
        Delete intermediate/temporary files. 
        If loglevel == DEBUG : intermediate file will not be deleted
        """
        #region local variables
        #endregion local variables

        self.logger.debug(f"To delete Intermediate File : {IntFile}")
        if self.SPFLogLevel == "DEBUG":
            return
        self.DelAFile(IntFile)
    #END : def DelIntermediateFile

    def DelAFile(self, FileToDelete:Union[str, Path], RaiseErrIfNotFound: bool = False, DisplaytoConsole: bool = False):
        """
        delete the file 'FileToDelete'
        Input
        1. 
        2. 
        3. DisplaytoConsole: bool = False : If True Display to console
        """
        self.logger.debug(f"To delete AFile : {FileToDelete}; RaiseErrIfNotFound : {RaiseErrIfNotFound}")
        FileToDelete_pathObj:Path
        if type(FileToDelete) is str:
            FileToDelete_pathObj = Path(FileToDelete)
        else:
            FileToDelete_pathObj = FileToDelete
        if FileToDelete_pathObj.exists() is True:
            try:
                FileToDelete_pathObj.unlink() #same as os.remove()
                if DisplaytoConsole is True:
                    self.Console(f"Deleted File : {FileToDelete_pathObj.resolve()}")
            except Exception as err:
                errMsg = f"Error while deleting file : {str(FileToDelete_pathObj.resolve())}. {err}"
                raise Exception(errMsg)
        elif RaiseErrIfNotFound is True:
            errMsg = f"File not found : {str(FileToDelete_pathObj.resolve(strict=False))}"
            raise FileNotFoundError(errMsg)
    #END : def DelAFile

    def DelListOfFiles(self, ListOfFileToDelete: List, RaiseErrIfNotFound: bool = False, HaltDeleteOnError: bool = False, DisplaytoConsole: bool = False, SkipDeleteInDebugMode=False):
        """
        Delete all files provided in list
        Input:
        1. ListOfFileToDelete: List : List of File names to be deleted
        2. RaiseErrIfNotFound: bool = False : If True raise error if File not found
        3. HaltDeleteOnError: bool = False : If False --> Continue with Delete of rest of the files in list. If True --> Halt delete of files in the list if any error occurs
        4. DisplaytoConsole: bool = False : If True Display to console
        5. SkipDeleteInDebugMode: bool = False : If True ...Running in DEBUG mode, skip the delete
        """
        self.logger.debug(f"ListOfFileToDelete : {ListOfFileToDelete}")
        self.logger.debug(f"RaiseErrIfNotFound : {RaiseErrIfNotFound}")
        self.logger.debug(f"HaltDeleteOnError : {HaltDeleteOnError}")
        self.logger.debug(f"DisplaytoConsole : {DisplaytoConsole}")
        for FileItem in ListOfFileToDelete:
            try:
                if DisplaytoConsole is True:
                    Msg = f"Delete List of Files : Files Count : {len(ListOfFileToDelete)}"
                    _t = "\n".join([os.path.abspath(fileItem) for fileItem in ListOfFileToDelete])
                    self.Console(f"{Msg}\n{_t}")
                if SkipDeleteInDebugMode is True:
                    if self.SPFLogLevel == "DEBUG":
                        #in debug mode...skip delete
                        continue
                self.DelAFile(FileItem)
            except Exception as err:
                self.logger.exception(err)
                if HaltDeleteOnError is True:
                    raise # error occured while deleting file...raise error
                else:
                    pass # ignore error and continue with deleting
    #END : def DelListOfFiles

    def Console(self, logString="\n", logThis=True, overwrite:bool = False):
        #print to console
        print(logString) if overwrite is False else print(logString.strip(), end="\r")
        #print(logString.decode(encoding=locale.getdefaultlocale()[1]))
        if logThis is True: 
            self.logger.debug(f"{logString}")
        try:
            sys.stdout.flush()
        except Exception as err:
            pass #ignore
    #END : def Console

    def Cons80(self):
        """
        '=====
        'Write 80 of =
        '=====
        """
        #"================================================================================"
        self.Console(80*"=")
    #END : def Cons80

    def ConsStar80(self):
        """
        '=====
        'Write 80 of *
        '=====
        """
        #"********************************************************************************"
        self.Console(80*"*")
    #END : def ConsStar80
    
    def ConsoleWithTimeStamp(self, logString="\n", overwrite:bool = False):
        """
        print '<logstring> ... '<datetimestamp>\n'
        """
        self.Console(f"{logString} ... {self.DatetimeNow}\n", overwrite=overwrite, logThis=not overwrite)
    #END :  def ConsoleWithTimeStamp

    def ConsoleDoneWithTimeStamp(self):
        """
        print '  Done...'<datetimestamp>\n'
        """
        self.Console("  Done... {0}\n".format(self.DatetimeNow))
    #END : def ConsoleDoneWithTimeStamp

    def ConsoleDoneWithoutTimeStamp(self):
        """
        print '   Done ...'
        """
        self.Console("   Done ...")
    #END : def ConsoleDoneWithoutTimeStamp
       
    def ConsoleWithCons80(self, logString="\n", logThis=True):
        """
        print cons80 around message
        """
        self.Cons80()
        self.Console(logString, logThis)
        self.Cons80()
    #END :  def ConsoleWithCons80

    def ExtractArchive(self, ArchiveItem: List):
        """
        extract the archived items from payload
        Input:
        1. ArchiveItem: List : [ArhiveName, ArchiveContentBase64String]
        """
        self.logger.debug(ArchiveItem[0])
        archive_file_name, archive_file_zipped_content = ArchiveItem
        self.logger.debug(f"archive_file_name : '{archive_file_name}'; len(archive_file_zipped_content) : {len(archive_file_zipped_content)}")
        self.Extract_File_from_base64_string(archive_file_name, archive_file_zipped_content)
        return archive_file_name       
    #END : def ExtractArchive

    def SPFRoboCopy(self, MyFile, MySrc, MyDest, MyRetry, MyWait, MyOpen, MyArg, MyErrOp, MyPassExitCodes=[]) :
        """
        '====================================================
        'Robocopy File
        '
        'Args:
        '====
        'MyFile : File to copy
        'MySrc  ; Source Dir
        'MyDest : Dest Dir
        'MyRetry: # retries
        'MyWait : Wait time between retries
        'MyOpen : Y to not copy if src file open
        'MyArg  : Addnal Robocopy args. E.g., /MOV
        'MyErrOp: Y=Abort if Src File ~found
        'MyPassExitCodes : List of exit codes that shall be treated as Pass exit codes E.g., : [13, 14, 15]
        '
        'GLOBALS:
        '-------
        'gLocalDir: Local Folder
        'gMyAbort :Job Abort global
        '====================================================
        """

        self.ConsoleWithTimeStamp("Starting Robocopy Applet, v1.6") #VA30.86
        #locals
        SPFRoboCopyStatus = False # assume robocopy failed
        MyData = None
        lMyArrRC = None
        i = None
        Tmp = None
        MyModStr = None
        TmpD = None
        MyDModStr = None
        SPFRoboCopyExitCode = {16 : "***Robocopy Fatal Error. No files copied***",
                                15 : "Robocopy OKCOPY + FAIL + MISMATCHES + XTRA",
                                14 : "Robocopy FAIL + MISMATCHES + XTRA",
                                13 : "Robocopy OKCOPY + FAIL + MISMATCHES",
                                12 : "Robocopy FAIL + MISMATCHES",
                                11 : "Robocopy OKCOPY + FAIL + XTRA",
                                10 : "Robocopy FAIL + XTRA",
                                9 : "Robocopy OKCOPY + FAIL",
                                8 : "Robocopy error. Some files/folders could not be copied",
                                7 : "Robocopy OKCOPY + MISMATCHES + XTRA",
                                6 : "Robocopy MISMATCHES + XTRA",
                                5 : "Robocopy OKCOPY + MISMATCHES",
                                4 : "Robocopy Mismatched files and folders detected",
                                3 : "Robocopy OKCOPY + XTRA",
                                2 : "Robocopy Extra files/folders detected",
                                1 : "Robocopy Successful (1)",
                                0 : "Robocopy Successful (0) No files need be copied"}

        SPFRoboCopy_pass_ExitCode = [0, 1, 2, 3]
        SPFRoboCopy_err_ExitCode = [item for item in SPFRoboCopyExitCode.keys() if item not in SPFRoboCopy_pass_ExitCode]
        runOutput = ""

        self.logger.debug("MyFile: '{0}'".format(MyFile))
        self.logger.debug("MySrc: '{0}'".format(MySrc))
        self.logger.debug("MyDest: '{0}'".format(MyDest))
        self.logger.debug("MyRetry: '{0}'".format(MyRetry))
        self.logger.debug("MyWait: '{0}'".format(MyWait))
        self.logger.debug("MyOpen: '{0}'".format(MyOpen))
        self.logger.debug("MyArg: '{0}'".format(MyArg))
        self.logger.debug("MyErrOp: '{0}'".format(MyErrOp))
        self.logger.debug("MyPassExitCodes: '{0}'".format(MyPassExitCodes))
        MySrc = MySrc.strip().rstrip("\\") if MySrc is not None else ""
        MyDest = MyDest.strip().rstrip("\\") if MyDest is not None else ""
        MyWait = 30 if MyWait is None else MyWait
        MyFile = MyFile.strip() if MyFile is not None else ""
        MyRetry = 100 if MyRetry is None else MyRetry
        MyOpen = MyOpen.strip().upper() if MyOpen is not None else ""
        MyArg = MyArg.strip().upper() if MyArg is not None else ""
        MyErrOp = MyErrOp.strip().upper() if MyErrOp is not None else ""

        if type(MyPassExitCodes) is not list:
            errMsg = "MyPassExitCodes should be a list of Exit Codes(numbers) E.g., [13, 14, 15] "
            raise Exception(errMsg)
        else:
            SPFRoboCopy_pass_ExitCode = SPFRoboCopy_pass_ExitCode + MyPassExitCodes
            self.logger.debug("updated : SPFRoboCopy_pass_ExitCode: '{0}'".format(SPFRoboCopy_pass_ExitCode))


        if len(MyFile) == 0 :
            self.Console("  No Files specified to copy. Exiting ...")
        elif len(MySrc) == 0 :
            self.Console("  No Source Path specified. Exiting ...")
        elif len(MyDest) == 0 :
            self.Console("  No Destination Path specified. Exiting ...")
        #elif os.path.isdir(MySrc) == False :
        #'===v30.59  ===========  
        elif self.SHisSHEntry is False and  os.path.isdir(MySrc) is False : #' Folder ~exist for Interactive runs as SH may have network issues which robocopy will retry for
            self.Console("  Source path not found ({0}). Exiting ...".format(MySrc)) 
            self.gMyAbort = True
        else : 
            if MyOpen != "Y" : MyOpen = "N"
            if MyErrOp != "Y" : MyErrOp = "N"
            if MySrc in ["\\", ".\\"] : MySrc = "."
            if MyDest in ["\\", ".\\"] : MyDest = "."

            #print MyRetry.isdigit()
            try :
                MyRetry = int(MyRetry)
                MyWait = int(MyWait)
            except Exception as err: 
                self.logger.exception("{0}".format(err.args[0]))
                    
            if type(MyRetry) != int:
                self.Console("  Retry Argument must be numeric ({0}). Exiting ...".format(MyRetry))
            elif type(MyWait) != int:
                self.Console("  Wait Argument must be numeric ({0}). Exiting ...".format(MyWait))
            elif MySrc == MyDest :
                self.Console("  Source and Destination folders are the same ({0}). Exiting ...{0}".format(MySrc))
            else : #proceed
                if MyFile.find("*") != -1 or MyFile.find(",") != -1 :
                    if MyOpen == "Y" or MyErrOp == "Y" :
                        self.Console("Sorry but neither the Open test nor the Abort if source file not found test can be done if wildcards are used or multiple files are to be copied")
                        MyOpen = "N"

                if MyOpen == "Y" or MyErrOp == "Y" :
                    Tmp = TmpD = MyFile
                    
                    if MySrc != "." :
                        Tmp = os.path.join(MySrc, Tmp) #MySrc + "\\" + Tmp
                    if MyDest != "." :
                        TmpD = os.path.join(MyDest, TmpD) #MyDest + "\\" + TmpD
                                       
                    if Path(Tmp).exists() is False :
                        #self.Console("  Source File does not exist : {0}".format(Tmp))
                        errMsg = "  Source File does not exist : {0}".format(Tmp)
                        if MyErrOp == "Y" :
                            errMsg = "{0}\n  RoboCopy will exit with an error...".format(errMsg)
                            raise Exception(errMsg)
                        else :
                            self.Console("  Copy will be skipped ...")
                            return #exit sub
                    #end : if + elif : file_Exists_Retry == False :

                #End : if MyOpen == "Y" or MyErrOp == "Y" :
                
                lMyArrRC = MyFile.replace('"','').strip().split(",")
                del MyFile #no longer need this 

                self.logger.debug("Before clean up MyFilesArry -lMyArrRC : {0}".format(lMyArrRC))
                #remove empty items
                #'space is dlm. " = !!!
                lMyArrRC = ['"{0}"'.format(item.strip()) for item in lMyArrRC if item not in ['', None]]

                self.logger.debug("After clean up MyFilesArry - lMyArrRC :{0}".format(lMyArrRC))

                if len(lMyArrRC) == 0 :
                    self.Console("  No Files specified to copy. Exiting ...")
                else :
                    #run command
                    myCMDToExecute = "ROBOCOPY"

                    myCMDArgs = ['"{0}"'.format(MySrc), 
                                 '"{0}"'.format(MyDest)] + lMyArrRC + ['/R:{0}'.format(MyRetry),
                                 '/W:{0}'.format(MyWait)] 
                                                            #'/W:{0}'.format(MyWait),
                                                            #"/NP",
                                                            #"/IS"]

                    if MyArg is not None and len(MyArg.strip()) != 0 :
                        if type(MyArg) is str: # types.StringType :
                            MyArg = MyArg.split(' ')
                        myCMDArgs = myCMDArgs + MyArg

                    self.logger.debug(myCMDToExecute)
                    self.logger.debug(myCMDArgs)
                    runVal, runExitCode = False, -1
                    _retry_on_errCodes = [8]
                    _retry_attempts = 3
                    _current_attempt = 0
                    _retry_done = False
                    while (_retry_done == False):
                        _current_attempt = _current_attempt + 1
                        try:
                            SubProcArgumentsStr = " ".join([myCMDToExecute] + myCMDArgs)
                            # runExitCode = subprocess.call(SubProcArgumentsStr, shell=True)
                            myProcess = subprocess.Popen(SubProcArgumentsStr, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, shell=True)
                            while True : 
                                #nextVal = myProcess.stdout.read(1)
                                try :
                                    nextVal, errVal = myProcess.communicate()
                                    nextVal, errVal = nextVal.decode(), errVal.decode()
                                    # self.Console(nextVal.replace("\r", ""))
                                    if errVal not in ["", None] :
                                        # self.Console(errVal.replace("\r", ""))
                                        runOutput = runOutput + errVal
                                    #store the output for logging
                                    runOutput = runOutput + nextVal

                                    if myProcess.poll() is not None:
                                        break
                                except ValueError as valErr :
                                    self.logger.exception(valErr)
                                except Exception as err1 :
                                    self.logger.exception(err1)
                                    raise
                            #END while True : 
                            self.logger.debug("subprocess.output : {0}".format(runOutput))
                            runExitCode = myProcess.returncode
                            self.logger.debug(f"runExitCode : {runExitCode}")
                            if runExitCode not in SPFRoboCopy_pass_ExitCode and runExitCode in SPFRoboCopy_err_ExitCode:
                                runVal = False
                            else:
                                runVal = True
                            
                        except Exception as err:
                            self.logger.exception("Error : {0}".format(err))
                            runVal = err.args[2]
                            runExitCode = err.args[1]
                            #raise
                        
                        _retry_done = runVal

                        if _retry_done == False and runExitCode in _retry_on_errCodes:
                            if _current_attempt < _retry_attempts:
                                self.Console(f"Retry robocopy for '{SPFRoboCopyExitCode[runExitCode]}' ExitCode : {runExitCode}...Retry {_current_attempt} of {_retry_attempts}")
                                time.sleep(5)
                            else:
                                _retry_done = True
                    #END : while (_retry_done == False)

                    self.Console("{0} - ExitCode : {1} ".format(SPFRoboCopyExitCode[runExitCode], runExitCode))
                    if runVal is False :
                        errMsg = "   RoboCopy considered unsuccessful..."
                        raise Exception(errMsg)
                    else :
                        SPFRoboCopyStatus = True
                self.ConsoleDoneWithoutTimeStamp()
            #end : else : #proceed
            return SPFRoboCopyStatus
    #END : def SPFRoboCopy

    def CheckInvokeServiceErrFile(self, invokeSvc_output_filename):
        """
        will be called if InvokeService fails...check if "{invokeSvc_output_filename}.err" exists ...read the contents and log error message from service
        """
        invokeSvc_response_err_file = Path(f"{invokeSvc_output_filename}.err")
        self.logger.debug(invokeSvc_response_err_file)
        invokeSvc_response_err_msg = ""
        if Path(invokeSvc_response_err_file).exists():
            with open(str(invokeSvc_response_err_file), "r") as errFileRdr:
                invokeSvc_response_err_msg = errFileRdr.read()
            self.logger.debug(f"InvokeService Error : {invokeSvc_response_err_msg}")
        return invokeSvc_response_err_msg
    #END: def CheckInvokeServiceErrFile

    def CreateTempFileFromInputFilesList(self, InputFilesList:list, TempFileNamePart:str)->str:
        """
        InputFilesList:list --> list object of input files ['file1.csv','file2.csv'] that need to parsed and written to temp file
        TempFileNamePart:str --> file name part to be used to create the file
        Return:str --> 'tmp_file_name' name of file into which InputFilesList items were written into. 
        """
        self.logger.debug(f"InputFilesList : {InputFilesList}; TempFileNamePart : {TempFileNamePart}")   
        retValue:str = None
        if type(InputFilesList) is list :
            _tmp_file_name = f"{TempFileNamePart}_{self.SPFInstance}_tmp.csv"
            
            with open(_tmp_file_name, "w") as wrtr:
                wrtr.write("\n".join(InputFilesList))    
            
            self.logger.debug(f"written to temp file : {_tmp_file_name}")
            self.FilesToCleanUp.append(_tmp_file_name) # add to clean-up list
            retValue = _tmp_file_name #return the file name
        else:
            raise Exception(f"InputFilesList '{type(InputFilesList)}' is not a list type")
        self.logger.debug(f"retValue : {retValue}")
        return retValue
    #END : def CreateTempFilewithCSVValue

    def GetFileDLM(self, myFile, mode="I"):
        """
        '============================================
        'Return the delimiter to use based on the
        'file extension. Use \t for .tab, "|" for .asc
        'and "," for .csv or otherwise
        '
        '(input) :
        '----
        '1.) myFile: File to check
        'Note: the va script method has myMode, which has been deprecated
        '2.) mode : indicator if file in input (I) or output (O)
        '(output):
        '-------
        '1.) Delimiter. E.g., \t, "," or "|" or ""
        '============================================
        """
        fileDLM = None
        if myFile is None:
            raise Exception("FileName: '{0}' is invalid".format(myFile))
        # add future delimiters to below dictionary
        fileDLMS = {('.tab', '.hive-tab','.hive-sequence') :   '\t'
                    ,('.asc')       :   '|'
                    ,('.txt', '.csv')       :   ',' if mode=="I" else "" #can be removed
                    ,('.plus')      :   '+' 
                    }
        fileName, fileExt = os.path.splitext(myFile)
        self.logger.info("FileName = {0}, FileExt = {1}, mode = {2}".format(fileName, fileExt, mode))

        fileExt = fileExt.lower()
        for key1, val1 in fileDLMS.items():
            if (fileExt in key1): 
                fileDLM =  val1
                break
        if fileDLM is None:
            errMsg = "File extension not supported to have a Delimeter: {0}".format(myFile)
            raise Exception(errMsg)
        self.logger.info("For file: {0} Delimiter: {1}".format(myFile, fileDLM.encode("unicode-escape").decode()))
        return fileDLM
    #END : def GetFileDLM

    def CreateInGroupDataFile(self, InGroupData: Dict, InGroupDataFileName:str, DeleteInGroupDataFile: bool = True):
        """
        Create a file 'InGroupDataFileName' from 'InGroupData' and add to 'required_input_files_list_csv'
        'Input : 
        1. InGroupData : Dict : dictionary that needs to be written to a file. 
            E.g., : Format {'Column_Name' : [row_items]}
                    Below example shows data for 2 columns & 2 rows of data
                    myInGroupData = {'columnName1' : ['row_c1_1', 'row_c1_2']
                                    ,'columnName2' : ['row_c2_1', 'row_c2_2']}
        2. InGroupDataFileName : str : File name the InGroupData will be written. e.g., 'InGroupData.csv'
        3. DeleteInGroupDataFile : bool : True - delete file specified in 'InGroupDataFileName' after execution. False - Don't delete the file
        """
        #region locals        
        FileDelimiter = None
        #endregion locals
        self.LogRec.append("ingrp")
        #region validate input
        if type(InGroupData) is not dict:
            raise Exception(f"Invalid value for IngroupData : '{type(InGroupData)}'. Expecting a dictionary object")
        if len(InGroupData) == 0:
            errMsg = f"Empty dictionary provided for InGroupData : '{InGroupData}'."
            self.logger.exception(errMsg)
            raise Exception(errMsg) 

        if type(InGroupDataFileName) is not str:
            self.logger.exception(errMsg)
            raise Exception(f"Invalid value for InGroupDataFileName : '{type(InGroupDataFileName)}'. Expecting a str")
        elif InGroupDataFileName == "":
            self.logger.exception(errMsg)
            raise Exception(f"Invalid file name provided in InGroupDataFileName : '{InGroupDataFileName}'. Expecting a valid file name")

        if DeleteInGroupDataFile is True:
            self.FilesToCleanUp.append(InGroupDataFileName)
        #endregion validate input


        #region write InGroupData into InGroupDataFileName
        try:
            FileDelimiter = self.GetFileDLM(InGroupDataFileName)
        except Exception as err:
            errMsg = f"Error while determining the delimiter for output file '{InGroupDataFileName}'. Please use .csv or .tab as file extension.\n{err}"
            self.logger.exception(errMsg, err)
            raise Exception(errMsg)
        
        df1 = None
        try:
            df1 = pandas.DataFrame.from_dict(InGroupData, dtype=str)   
            #TODO : should check for Inc=999 case? 
            # If looping is needed...then it is efficient to use SQLPF Run-Loop step...will avoid roundtrips to service
        except ValueError as valErr:
            errMsg = f"InGroupData should have same number of items in row data for all columns. {valErr}"
            self.logger.exception(errMsg, valErr)
            raise errMsg
        df1.to_csv(InGroupDataFileName, index=False, sep=FileDelimiter)
        df1 = None
        del df1
        #endregion write InGroupData into InGroupDataFileName
        
        #region add InGroupDataFileName to required_input_files_list_csv file
        try:
            if self.required_input_files_list_csv is None:
                self.required_input_files_list_csv = [InGroupDataFileName]
            else:
                if Path(self.required_input_files_list_csv).exists() is True:
                    df1 = pandas.read_csv(self.required_input_files_list_csv, sep=",", header=None, names=['col1'])
                else: 
                    df1 = pandas.DataFrame(Columns=['col1'])
                df2 = pandas.DataFrame.from_dict({'col1' : [InGroupDataFileName]})
                df1 = pandas.concat([df1, df2], ignore_index=True, sort=True)
                # df1 = df1.append({'col1' : InGroupDataFileName}, ignore_index=True)
                df1.drop_duplicates(inplace=True)
                df1.to_csv(self.required_input_files_list_csv, sep=",", index=False, header=False)
                self.logger.debug(f"Updated '{self.required_input_files_list_csv}' with InGroupDataFileName : '{InGroupDataFileName}'")
                df2 = None
                df1 = None
                del df2
                del df1
        except Exception as err:
            errMsg = f"Error while updating data for 'required_input_files_list_csv'. {err}"
            self.logger.exception(errMsg, err)
            raise Exception(errMsg)
        #endregion add InGroupDataFileName to required_input_files_list_csv file
    #END : def CreateInGroupDataFile

    def _record_SPF(self, logrec:Union[str, list] = None, logEE: str = None, req_headers: dict = None):
            """
            log to SPFLogSvc
            """
            try:
                # spfLogServiceURL = "http://DTDVPWSPFWBN1.amr.corp.intel.com/SQLPF_Log_Service/SQLPF_Log_Service.svc/InsertSQLPFLogData"
                spfLogServiceURL = self.SQLPF_LogSvcURL
                if req_headers is None:
                    headers = {'user-agent': 'SQLPFSvcClient/{0}'.format(__version__)}
                else:
                    headers = req_headers

                _username = os.getenv('username')
                # print(socket.gethostname())
                _logClient = os.getenv("computername") #socket.getfqdn().upper()
                if _logClient is None:
                    _logClient = os.getenv("HOSTNAME")
                if _username is None:
                    _username = os.getenv('USER')

                if logrec is None:
                    logrec = self.LogRec

                if type(logrec) is list:
                    logrec = "#".join(logrec)

                if logEE is None:
                    logEE = self.LogEE
                    
                dataInput = {'logIDSID' : _username.upper(), 
                            'logDomain' : os.getenv('userdomain'),
                            'logClient' : _logClient,
                            'logDateTime' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'userAgent' : headers['user-agent'],
                            'RMode' : 'I' if self.SHisSHEntry is False else 'B',
                            # 'Recd' : f"SQLPFSvcClient_{__version__}_{self.__class__.__name__}",
                            'Recd' : logrec,
                            'Instance' : self.SPFInstance}
                dataInput["EE"] = logEE
                self.logger.debug(f"Start logSvc : dataInput{dataInput}")
                requests.get(spfLogServiceURL, params={'dataInput' : json.dumps(dataInput)}, headers=headers, timeout=1)
                self.logger.debug("Done logSvc")
            except Exception as err:
                self.logger.error(err)
                pass
    #END : def Record_SPF
            
    def _get_logger(self, loggerName:str = "SvcQueryClient", logFileName:str = "", logLevel: str = "ERROR"):
        """
        """
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(process)d.%(thread)d - %(levelname)s - %(module)s.%(funcName)s - %(message)s")
        _loglevel = { "INFO" : logging.INFO, #20
                    "DEBUG" : logging.DEBUG, #10
                    "ERROR" : logging.ERROR #40
                    }
        try:
            logLevel = _loglevel[logLevel.upper()]
        except KeyError as keyErr:
            # value not found ...default to ERROR
            logLevel = logging.ERROR

        mylogger = logging.getLogger(loggerName)

        _has_handlers = mylogger.hasHandlers()
        if _has_handlers is False and logLevel == logging.DEBUG: #there are no logging handlers... create a new one...only in DEBUG mode
            mylogger.setLevel(logLevel)
            LoggerHandler = RotatingFileHandler(logFileName, maxBytes=52428800, backupCount=1000, delay=True, encoding='utf-8')
            LoggerHandler.setFormatter(formatter)
            mylogger.addHandler(LoggerHandler)
        self.logger = mylogger
        self.logger.propagate = False
    #END : def _get_logger
#END : class SQLPFSvcClient

class SQLPFSvcClientPyReq(SQLPFSvcClient):
    """
    override to use Py requests instead of DotNet. Retained for backward compatibility 
    """
#END : class SQLPFSvcClientPyReq

class SQLPFSvcClientSync(SQLPFSvcClient):
    """
    client for SQLPF service for synchronized execution only 
    """
    #region properties
    _SvcExecInput_template = None
    @property
    def SvcExecInput_template(self)-> dict:
        """
        get : SvcExecInput_template -> Dict 
        """
        if self._SvcExecInput_template is None:
            self._SvcExecInput_template = {"SyncInput":{"SPFLogLevel" : self.SPFLogLevel
                                                     , "CMDARGS" : self.SQLPF_CMD_Args_List_JSONString
                                                     , "ClientApp" : self.ClientAppName
                                                     , "ComputerName" : socket.getfqdn()
                                                     , "ClientUserIDSID" : self.user_idsid
                                                     , "ClientAPIVersion" : __version__
                                                     , "hpc_svc_memory_limit_GB" : 32
                                                     , "ClientInvokeStartTimeStamp" : self.DatetimeNow} #test bare minimum input ...should result in all defaults kicking in at Svc end
                                          } 

        return copy.deepcopy(self._SvcExecInput_template)
    #END : def SvcExecInput_template
    
    #region hpc_svc_timeout_minutes
    __hpc_svc_timeout_minutes: int = None
    __hpc_svc_timeout_minutes_default = 10
    @property
    def hpc_svc_timeout_minutes(self)-> int:
        """
        get : hpc_svc_timeout_minutes
        """
        self.logger.debug(f"get : hpc_svc_timeout_minutes : {self.__hpc_svc_timeout_minutes}")
        return self.__hpc_svc_timeout_minutes
    
    @hpc_svc_timeout_minutes.setter
    def hpc_svc_timeout_minutes(self, value: int):
        """
        set : hpc_svc_timeout_minutes
        """
        errMsg = f"Invalid value for 'hpc_svc_timeout_minutes' : '{value}'. Value should be an integer in the range of 1 to 10."

        if value in ["", None]:
            raise ValueError(errMsg)
        
        try:
            if type(value) is not int:
                value = int(value.strip())
        except Exception as err:
            raise ValueError(errMsg)
        
        if value not in range(1, 11, 1):
            raise ValueError(errMsg)
        
        self.__hpc_svc_timeout_minutes = value
        self.logger.debug(f"set : hpc_svc_timeout_minutes : {value}")
    #endregion hpc_svc_timeout_minutes
    #region required_output_file
    __required_output_file: str = None
    @property
    def required_output_file(self):
        """
        get : required_output_file
        """
        if self.__required_output_file in ["", None]:
            self.__required_output_file = self.GetLastCSV_OptionValue(self.SPFSQL_File)
        self.logger.debug(f"get : required_output_file : {self.__required_output_file}")
        return self.__required_output_file

    @required_output_file.setter
    def required_output_file(self, value:str):
        """
        set : required_output_file
        """
        self.logger.debug(f"set : required_output_file : {value}")
        self.__required_output_file = value
    #endregion required_output_file
    #region logSvc properties
    @property
    def LogEE(self)->str:
        """
        get : LogEE
        """
        if self.logCallingScript.lower() == "spfsql3.py":
            return "Py3_SYNC_API"
        elif self.logCallingScript.lower() == "ipykernel_launcher.py":
            return "Jup_SYNC_API"
        else:
            return "SYNC_API"
    #endregion logSvc properties

    #endregion properties

    def __init__(self, SPFSQL_File:str = ""
                     , SQLPFSvcEnv:str = "SQLPFaaS_PROD_Sync"
                     , SQLPFSvcURL:str = ""
                     , SQLPF_CMD_Args_List:list = []
                     , SPFInstance:str = ""
                     , SPFLogLevel:str = "ERROR"
                     , required_input_files_list_csv:str or list = ""
                     , begin_hpc_svc_ver:str = "2" 
                     , ClientAppName:str = __file__
                     , hpc_mem_limit_GB:int = 32 # memory limit on service end for this query execution
                     , user_idsid:str = "" # IDSID of the actual user. Note: this is just for logging/tracking...no security action prformed based on this value
                     , svc_output_fetch_mode:str ="HTTP" #default is get output over HTTP
                     , loggerName:str = "SPFLib" # pass name of the logger that is already created by the calling client... 
                     , logFileName:str = "SvcClient.log" # specify absolute path if location of log file needs to be created in a different location
                     , convert_output_to_json:bool = False # sync svc only : true -> convert final output to json format
                     , required_output_file: str = "" # sync svc only : 'file name' of final output file of spfsql execution that should be read and sent back in Svc response
                     , hpc_svc_timeout_minutes = 10 #HPC Sync svc timeout in minutes 
                     , SPF_CL_Args: dict={} #command-line arguments e.g., {'CL_VAR_NAME' : 'VAR_VALUE'}. key should always begin with 'CL_'
                     , SPF_LogSvc_Env : str = "SQLPF_LogSvc" #SPF logger Svc Environment Prod: 'SQLPF_LogSvc', Dev : 'SQLPF_LogSvc_CH_Dev'
                     , copy_svc_console_flag: bool = False # True --> fetch the query execution console output from HPC server
                     , copy_log_files_flag: bool = False # True --> fetch the query execution logs from HPC server
                    ) -> None:
        """
        Input:
        1.)  SPFSQL_File:str = "" : (Required) : SPFSQL query file (.spfsql) that needs to get executed on SQLPf HPC 
        2.)  SQLPFSvcEnv:str = "SQLPFaaS_PROD_Sync" : (Optional) : SQLPF HPC service environment name. Any one of the values in ['SQLPFaaS_PROD_Sync', SQLPFaaSPROD_N1_Sync]
        3.)  SQLPFSvcURL:str = "" : (Optional) : SQLPF HPC Service URL. (used only for testing. Use 'SQLPFSvcEnv' to specify the SQLPF HPC environment name)
        4.)  SQLPF_CMD_Args_List:list = [] : (Optional) : Command line arguments that will be used during .spfsql execution on the server. e.g., SQLPF 'CL_xx' arguments -> ["/CL_outFILE_NAME=test.tab", ""]
        5.)  SPFInstance:str = "" : (Optional) : Instance number for SQLPF execution e.g., "1234"
        6.)  SPFLogLevel:str = "INFO" : (Optional) : Loglevel to be used on SQLPF HPC service end
        7.)  required_input_files_list_csv:str or list = "" : (Optional) : A .csv file containing info on input files or a list of files e.g., ['file1.csv, 'file2.csv'] needed for execution of SPFSQL file on server. 
        8.)  begin_hpc_svc_ver:str = "2" : (Optional) : SQLPF HPC Service version (future use)
        9.)  ClientAppName:str = __file__ : (Optional) : Current script that is using this SQLPf HPC client script 
        10.) hpc_mem_limit_GB:int = 32 : (Optional) : memory limit on service end for this query execution
        11.) user_idsid:str = "" : (Optional) : IDSID of the actual user. Note: this is just for logging/tracking...no security action prformed based on this value
        12.) svc_output_fetch_mode:str ="HTTP" : (Optional) : default is get output over HTTP
        13.) loggerName:str = "SPFLib" : (Optional) : pass name of the logger that is already created by the calling client... 
        14.) convert_output_to_json:bool = False : (Optional) : sync svc only : true -> convert final output to json format
        15.) required_output_file: str = "" : (Optional) : sync svc only : 'file name' of final output file of spfsql execution that should be read and sent back in Svc response
        16.) hpc_svc_timeout_minutes = 10 : (Optional) : HPC Sync svc timeout in minutes 
        17.) SPF_CL_Args : dict : dictionary of command-line arguments e.g., {'CL_VAR_NAME' : 'VAR_VALUE'}. key should always begin with 'CL_'
        18.) SPF_LogSvc_Env : str : SPF logger service environment name e.g., Prod: 'SQLPF_LogSvc', Dev : 'SQLPF_LogSvc_CH_Dev'    
        19.) copy_svc_console_flag: bool = False # True --> fetch the query execution console output from HPC server
        20.) copy_log_files_flag: bool = False # True --> fetch the query execution logs from HPC server
        """
        #region locals
        # print(f"Sync loggerName  : {loggerName}")
        super().__init__(SQLPFSvcEnv=SQLPFSvcEnv
                     , SQLPFSvcURL=SQLPFSvcURL
                     , SQLPF_CMD_Args_List=SQLPF_CMD_Args_List
                     , SPFSQL_File=SPFSQL_File
                     , SPFLogLevel=SPFLogLevel
                     , required_input_files_list_csv=required_input_files_list_csv
                     , begin_hpc_svc_ver=begin_hpc_svc_ver
                     , ClientAppName=ClientAppName
                     , hpc_mem_limit_GB=hpc_mem_limit_GB
                     , user_idsid=user_idsid
                     , svc_output_fetch_mode=svc_output_fetch_mode
                     , loggerName=loggerName
                     , SPF_CL_Args=SPF_CL_Args
                     , SPF_LogSvc_Env=SPF_LogSvc_Env
                     , copy_svc_console_flag=copy_svc_console_flag
                     , copy_log_files_flag=copy_log_files_flag
                    )
        #endregion locals

        #region debug logging
        self.logger.debug(f"SQLPFSvcEnv : {SQLPFSvcEnv}")
        self.logger.debug(f"SQLPFSvcURL : {SQLPFSvcURL}")
        self.logger.debug(f"SQLPF_CMD_Args_List : {SQLPF_CMD_Args_List}")
        self.logger.debug(f"SPFSQL_File : {SPFSQL_File}")
        self.logger.debug(f"SPFInstance : {SPFInstance}")
        self.logger.debug(f"SPFLogLevel : {SPFLogLevel}")
        self.logger.debug(f"required_input_files_list_csv : {required_input_files_list_csv}")
        self.logger.debug(f"begin_hpc_svc_ver : {begin_hpc_svc_ver}")
        self.logger.debug(f"ClientAppName : {ClientAppName}")
        self.logger.debug(f"hpc_mem_limit_GB : {hpc_mem_limit_GB}")
        self.logger.debug(f"user_idsid : {user_idsid}")
        self.logger.debug(f"svc_output_fetch_mode : {svc_output_fetch_mode}")
        self.logger.debug(f"convert_output_to_json : {convert_output_to_json}")
        self.logger.debug(f"required_output_file : {required_output_file}")
        self.logger.debug(f"hpc_svc_timeout_minutes : {hpc_svc_timeout_minutes}")   
        self.logger.debug(f"SPF_CL_Args : {SPF_CL_Args}")     
        self.logger.debug(f"SPF_LogSvc_Env : {SPF_LogSvc_Env}")     
        self.logger.debug(f"copy_svc_console_flag : {copy_svc_console_flag}")     
        self.logger.debug(f"copy_log_files_flag : {copy_log_files_flag}")     
        #endregion debug logging

        #region - set class level variables
        self.required_output_file = required_output_file        
        self.hpc_svc_timeout_minutes = hpc_svc_timeout_minutes
        self.convert_output_to_json = convert_output_to_json
        #endregion - set class level variables
        self.ExecuteCycleDone = False
        self.GetDF = False
        return 

    #END: def __init__

    def Execute(self) -> str or Path:
        """
        Entry point into Client handler
        """
        try:
            self.ConsoleWithTimeStamp(f"Starting SQLPathfinder HPC Service Client V{__version__}")
            self.LogRec.append('ex')
            self.__invokeExecuteSync(self.SQLPFSvcURL)
            self.ExecuteCycleDone = True
            return self.required_output_file
        except Exception as err:
            raise
        finally:
            self.DelListOfFiles(self.FilesToCleanUp, SkipDeleteInDebugMode=True)
            self.ConsoleDoneWithTimeStamp()
            # print(f"Sync Execute self.GetDF : {self.GetDF}")
            if self.GetDF is False:
                # print("Sync Execute calling record_spf ")
                self._record_SPF()
            
    #END : def Execute

    def pd_read_parquet(self, engine='auto', columns=None, storage_options=None, use_nullable_dtypes=False):
        """
        read the final parquet output of query execution and return a pandas dataframe object
        this method exposes all the parametes supported by pandas.read_paruet...
        Note: supports reading only delimited files only
        """
        if self.ExecuteCycleDone is False:
            execute_output = self.Execute() # invoke service
        else:
            execute_output = self.required_output_file

        # self.ConsoleWithTimeStamp("Start building dataframe")
        _tmp_df = pandas.read_parquet(execute_output, engine=engine, columns=columns, storage_options=storage_options, use_nullable_dtypes=use_nullable_dtypes)
        # self.ConsoleWithTimeStamp("End building dataframe")
        # self.ConsoleDoneWithTimeStamp()
        return _tmp_df

    def pd_read_csv(self, header='infer', names=None, index_col=None, usecols=None, dtype=None, engine=None
                     , converters=None, true_values=None, false_values=None, skipinitialspace=False, skiprows=None, skipfooter=0
                     , nrows=None, na_values=None, keep_default_na=True, na_filter=True, verbose=False, skip_blank_lines=True
                     , parse_dates=None, infer_datetime_format=False, keep_date_col=False, date_format=None, dayfirst=False
                     , cache_dates=True, iterator=False, chunksize=None, compression='infer', thousands=None, decimal='.'
                     , lineterminator=None, quotechar='"', quoting=0, doublequote=True, escapechar=None, comment=None, encoding=None
                     , encoding_errors='strict', dialect=None, on_bad_lines='skip'
                     , delim_whitespace=False, low_memory=True, memory_map=False, float_precision=None, storage_options=None):
        """
        read the final CSV/TAB output of query execution and return a pandas dataframe object
        this method exposes all the parametes supported by pandas.read_csv...
        Note: supports reading only delimited files only
        """
        if self.ExecuteCycleDone is False:
            execute_output = self.Execute() # invoke service
        else:
            execute_output = self.required_output_file

        # self.ConsoleWithTimeStamp("Start building dataframe")
        
        sep = self.GetFileDLM(execute_output)
        if (pandas.__version__ + '  ')[:2] >= '2.':
            _tmp_df = pandas.read_csv(execute_output, sep=sep, header=header, names=names, index_col=index_col, usecols=usecols, dtype=dtype, engine=engine
                     , converters=converters, true_values=true_values, false_values=false_values, skipinitialspace=skipinitialspace, skiprows=skiprows, skipfooter=skipfooter
                     , nrows=nrows, na_values=na_values, keep_default_na=keep_default_na, na_filter=na_filter, skip_blank_lines=skip_blank_lines
                     , parse_dates=parse_dates, date_format=date_format, dayfirst=dayfirst
                     , cache_dates=cache_dates, iterator=iterator, chunksize=chunksize, compression=compression, thousands=thousands, decimal=decimal
                     , lineterminator=lineterminator, quotechar=quotechar, quoting=quoting, doublequote=doublequote, escapechar=escapechar, comment=comment, encoding=encoding
                     , encoding_errors=encoding_errors, dialect=dialect, on_bad_lines=on_bad_lines
                     , low_memory=low_memory, memory_map=memory_map, float_precision=float_precision, storage_options=storage_options)
        else:
            _tmp_df = pandas.read_csv(execute_output, sep=sep, header=header, names=names, index_col=index_col, usecols=usecols, dtype=dtype, engine=engine
                     , converters=converters, true_values=true_values, false_values=false_values, skipinitialspace=skipinitialspace, skiprows=skiprows, skipfooter=skipfooter
                     , nrows=nrows, na_values=na_values, keep_default_na=keep_default_na, na_filter=na_filter, verbose=verbose, skip_blank_lines=skip_blank_lines
                     , parse_dates=parse_dates, keep_date_col=keep_date_col, dayfirst=dayfirst
                     , cache_dates=cache_dates, iterator=iterator, chunksize=chunksize, compression=compression, thousands=thousands, decimal=decimal
                     , lineterminator=lineterminator, quotechar=quotechar, quoting=quoting, doublequote=doublequote, escapechar=escapechar, comment=comment, encoding=encoding
                     , encoding_errors=encoding_errors, dialect=dialect, on_bad_lines=on_bad_lines
                     , delim_whitespace=delim_whitespace, low_memory=low_memory, memory_map=memory_map, float_precision=float_precision, storage_options=storage_options)
        
        # self.ConsoleWithTimeStamp("End building dataframe")
        # self.ConsoleDoneWithTimeStamp()
        return _tmp_df

    def get_df(self, delete_svc_output_file=True):
        """
        return final output as a pandas dataframe
        Note: loads the entire service response output into a DF. Large files might cause issues
        """
        self.LogRec.append("df")
        self.GetDF = True
        # print(f"get_df self.GetDF : {self.GetDF}")
        try:
            if Path(self.required_output_file).suffix.lower() == ".parquet":
                _tmp_df = self.pd_read_parquet()
            else:
                _tmp_df = self.pd_read_csv()
            if delete_svc_output_file is True:
                self.FilesToCleanUp.append(self.required_output_file)
                self.DelListOfFiles(self.FilesToCleanUp, SkipDeleteInDebugMode=True)
            return _tmp_df
        except Exception as err:
            self.logger.error(err)
            raise
        finally:
            # print("Sync get_df calling record_spf ")
            self._record_SPF()
    #END : get_df

    def __invokeExecuteSync(self, web_api_url:str):
        """
        """
        mySVCAction = "EXECUTESPFSQLSYNC"
        self.logger.info(f"start : {mySVCAction}")

        if self.required_input_files_list_csv is None:
            _required_input_files_archive = None
        else:
            _required_input_files_archive = self.getFileContent(self.create_archive_from_file_list(self.required_input_files_list_csv, self.required_input_files_list_csv_archive_name, checkRelativePathOfArchivingFile=False), AsBase64String=True)
            self.FilesToCleanUp.append(self.required_input_files_list_csv_archive_name)

        _spfsql_file_archive = self.getFileContent(self.ZipFile(self.SPFSQL_File, self.SPFSQL_File_Archive_Name, RetainRelativePathInArchive=False), AsBase64String=True)

        #region prep payload
        SvcInput_args1 = {'spfsql_file' : [self.SPFSQL_File_Archive_Name, _spfsql_file_archive]
                        , 'required_input_files_archive' : [self.required_input_files_list_csv_archive_name, _required_input_files_archive] # file with list of required input files + folders that are required for query execution at Svc 
                        , 'begin_hpc_svc_ver' : self.begin_hpc_svc_ver #default is '2S'
                        # , 'hpc_svc_mem_limit_GB' : self.hpc_svc_mem_limit_GB # HPC svc peak memory limit 
                        # , 'hpc_svc_timeout_minutes' : self.hpc_svc_timeout_minutes #HPC Sync svc timeout in minutes
                        , 'convert_output_to_json' : self.convert_output_to_json #HPC Sync svc - flag to indicate that svc should convert the final output to json format -- TBD on service end
                        , 'required_output_file' : self.required_output_file 
                        , 'sh_job_name' : self.SHjobName # will have Scripthost job name if originating from SH
                        , 'sh_job_entryid' : self.SHentryID # will have Scripthost job EntryID if originating from SH
                        }
        SvcExecInput = self.SvcExecInput_template #this is copy of the template
        SvcExecInput['SyncInput']['Action'] = f"{mySVCAction}"
        SvcExecInput['SyncInput']['hpc_svc_execution_time_limit_minutes'] = self.hpc_svc_timeout_minutes #HPC Sync svc timeout in minutes
        SvcExecInput['SyncInput']['hpc_svc_memory_limit_GB'] = self.hpc_svc_mem_limit_GB # HPC svc peak memory limit
        # SvcExecInput['Input']['use_output_stage_share'] = f"{self.use_output_stage_share}"
        SvcExecInput['SyncInput']['ARGS'] = f"{json.dumps(SvcInput_args1)}"

        staged_output_file = self.ACTIONEXECUTE_Output_JSON_FileName
        #endregion prep payload

        timeout=(300, self.hpc_svc_timeout_minutes * 60)
        self.InvokeService(self.ACTIONEXECUTE_Input_JSON_FileName, SvcExecInput, staged_output_file, web_api_url, displayConsMsg=self.displayConsMsg, reqTimeout=timeout)
        
        self.fetchExecutionOutput(staged_output_file)
    #END: def __invokeExecuteSync

    def GetLastCSV_OptionValue(self, spfsqlFileName):
        """
        parse the .spfsql file 'spfsqlFileName' and get the last /CSV option value
        """
        spfsqlFileContent = None
        lastCSV = None
        rePtrn = "^/CSV\s?=\s?(?P<csv>.*)$"
        csvItems = []
        try:
            with open(spfsqlFileName, "r") as rdr:
                spfsqlFileContent = rdr.read()
            SPFTaskQueryItems = spfsqlFileContent.split("\n<---- New Query ---->")
            for item in SPFTaskQueryItems:
                item = item.strip()
                optItem = item.split("</OPTIONS>")
                findAll = re.findall(rePtrn, optItem[0], re.IGNORECASE|re.MULTILINE)
                if len(findAll) > 0:
                    lastCSV = findAll[-1]
                    csvItems.append(lastCSV)

            if len(csvItems) == 0:
                raise Exception(f"SPFSQL doesn't have any /CSV option and no required_ouptut_file specified")

            lastCSV = csvItems[-1]
            self.logger.debug(f"last /CSV option value : {lastCSV}")
            if re.match("<<<CL_.*>>>", lastCSV, re.IGNORECASE):
                _tmp_found_match = False
                for key, val in self.SPF_CL_Args.items():
                    _tmp_lastCSV = re.sub(f"<<<{key}>>>", val, lastCSV, re.IGNORECASE)

                    if _tmp_lastCSV != lastCSV:
                        _tmp_found_match = True
                        lastCSV = _tmp_lastCSV
                        del _tmp_lastCSV
                        break
                if _tmp_found_match is False:
                    errMsg = f"Command-line 'CL_' argument not found for '{lastCSV}'. Please check if you provided correct values in SPF_CL_ARgs parameter"
                    raise Exception(errMsg)
            if re.match("<<<.*>>>", lastCSV, re.IGNORECASE):
                raise Exception(f"Invalid file name provided for the last /CSV '{lastCSV}'. Macro Substitution is not supported for output file name")
            return lastCSV
        except Exception as err:
            errMsg = f"Error while parsing for the last /CSV option. Error : {err}"
            self.logger.exception(errMsg)
            raise
        finally:
            #clean-up
            spfsqlFileContent = None
            del csvItems 
            del spfsqlFileContent
    #END : def GetLastCSV_OptionValue

    def fetchExecutionOutput(self, staged_execute_response_file):
        """
        Override base method : get the execution output 
        """
        try:            
            
            if self.ACTIONEXECUTE_Response_RunStatus != 0:
                errMsg = f"Error while executing the query in SQLPF-HPC: {self.ACTIONEXECUTE_Response_Exception}\n   Please check the service execution console output/log files"
                raise Exception(errMsg)
            
            #execution has passed...continue to get the execution artifacts
            self.get_required_output_file()    
            
            if self.copy_svc_console_flag is True:
                self.get_svc_console_outputfile()

            if self.copy_log_files_flag is True:
                self.get_svc_log_files()

        except Exception as err:
            self.get_svc_console_outputfile()
            self.get_svc_log_files()
            self.logger.error(f"Error while fetching execution output : {err}")
            raise
    #END : def fetchExecutionOutput

    def get_required_output_file(self):
        """
        fetch the required_output_file
        """
        OutputFileURL = self.ACTIONEXECUTE_Response_ExecuteResult["OutputFileURL"]
        if validators.url(OutputFileURL) is True:
            self.GetFile_Using_Webcopy(OutputFileURL, self.required_output_file, processZipContent=True)
        else:
            errMsg = f"Error while getting required_output_file : '{self.required_output_file}' : {OutputFileURL}"
            self.logger.error(errMsg)
            raise Exception(errMsg)
        
    def get_svc_console_outputfile(self):
        """
        fetch the service execution console output
        """
        try:
            SQLPF_HPC_Execution_Console_Output_URL = self.ACTIONEXECUTE_Response_ExecuteResult["SQLPF_HPC_Execution_Console_Output"]
            self.GetFile_Using_Webcopy(SQLPF_HPC_Execution_Console_Output_URL, f"SQLPF_HPC_Execution_Console_Output_{self.SPFInstance}.log", processZipContent=True)
            if self.display_svc_console_flag is True:
                consoleMsg = (f"{self.getFileContent(self.ACTIONEXECUTE_Response_ConsoleLogFile).decode()}")
                self.Console(consoleMsg)
        except Exception as err:
            pass
    #END : def get_svc_console_outputfile

    def get_svc_log_files(self):
        """
        fetch the service execution log files
        """
        try:
            SPFLogFilesArchive_URL = self.ACTIONEXECUTE_Response_ExecuteResult["SPFLogFilesArchiveURL"]
            self.GetFile_Using_Webcopy(SPFLogFilesArchive_URL, f"SPFLogFilesArchive_{self.SPFInstance}.log", processZipContent=True)
        except Exception as err:
            pass
    #END : def get_svc_log_files
#END : class SQLPFSvcClientSync

if __name__ == "__main__":
    print("Executing in command line mode : Start\n")
    sync_dev_svc_url = r"https://dtdvdwspfwb1.amr.corp.intel.com/SQLPFaaSGatewaySvc2WASync/api/ExecuteSync"

    # _SQLPFSvcURL = r'https://DTDVDWSPFWB1.amr.corp.intel.com/gar_vanatara_cpfjdznt5adkpoztuavyv3h4_637822922143922929/SQLPFaaSGateway.svc/Execute'
    _SQLPFSvcURL = sync_dev_svc_url #"https://dtdvdwspfwb1.amr.corp.intel.com/SQLPFaaSGatewaySvcWAVN/api/execute"
    _Svc_SPFSQL_file = "imBigData_Async.spfsql" #"run_midas_test_svc_input.spfsql" #"Test_Query_MIDAS.spfsql" #"test_long_running_query.spfsql" #"test_Query_simple.spfsql" #r"test_Query.spfsql"
    _SPFInstance = "1002"
    _SPFLogLevel = "DEBUG"
    _SQLPF_CMD_Args_List = [f"/SPFINSTANCE={_SPFInstance}", "/iREPORTS_VERSION=NEXT", f"/SPFLOGLEVEL={_SPFLogLevel}", "/CL_outFILE_NAME="]
    _ClientAppName = "SQLPFSvcClient"
    #location of SQLPathfinder or SPFBin folder location
    _SPFExe = r"C:\Users\vanatara\My Programs\SQLPathFinder3"
    _SPFExe_PyReq = "." # if using the PyReq class then there is no dependency on SQLPathfinder
    #arg1 : SQLPF service environment name E.g., SQLPFSVC_DEVCH
    SQLPFSvcEnv = '' #'SQLPFaaS_DEV_CH2' #'SQLPFSVC_DEVCH_WA'
    #arg2 : file with list of required input files + folders that are required for query execution at Svc  
    required_input_files = r"required_input_file_for_query.csv"      
    #arg3 : file with list of required output files + folders that will be copied back to client
    required_output_files = "Test_Query_MIDAS_requried_output_files.csv" #r"required_output_files_folders.csv"     
    #arg4 : file with list of optional output files + folders that will be copied back to client
    optional_output_files = r"optional_output_files_folders.csv"     
    #arg5 : Flag (Y/N)[True/False] : If Y(True) print service console output to client console
    display_svc_console_flag = False 
    #arg6 : Flag (Y/N)[True/False] : If Y(True) copy service PyEE log files back to client 
    copy_log_files_flag = False      
    #arg7 : Flag (Y/N)[True/False] : If Y(True) continue if error reported from Svc         
    continue_on_error = False        
    #arg8 : Version # E.g., "1" for future use
    begin_hpc_svc_ver = "1"     
    #arg9 : hpc peak memory limit in GB --> min: 32GB max: 128GB
    hpc_mem_limit_GB = 32
    #arg10 : hpc timeout in hours --> min 8 hours max: 48 hours
    hpc_svc_timeout_hours = 8
    #arg11 : <user-domain>_<user_idsid> of the actual user a sys account is making the call
    user_idsid = "gar_vanatara"
    #arg12 : True/False : Default : False -- Flag to copy the output to a intermediate share and use robocopy on client end to copy the output from intermediate share to client machine
    use_output_stage_share = True
    #arg13 : HTTP/ROBOCOPY/ROBOCOPY-SHARE/HTTP-SHARE
    svc_output_fetch_mode = "ROBOCOPY-SHARE"

    _SQLPFSvcClient = SQLPFSvcClientSync(SQLPFSvcEnv = SQLPFSvcEnv
                                    , SQLPFSvcURL = _SQLPFSvcURL
                                    , SQLPF_CMD_Args_List = _SQLPF_CMD_Args_List
                                    , SPFSQL_File=_Svc_SPFSQL_file
                                    # , SPFInstance=_SPFInstance
                                    , SPFLogLevel=_SPFLogLevel
                                    # , required_input_files_list_csv = required_input_files
                                    # , required_output_files_folder_list_csv = required_output_files
                                    # , optional_output_files_folder_list_csv = optional_output_files
                                    # , display_svc_console_flag = display_svc_console_flag
                                    # , copy_log_files_flag = copy_log_files_flag
                                    # , continue_on_error = continue_on_error
                                    # , begin_hpc_svc_ver = begin_hpc_svc_ver
                                    # , ClientAppName=_ClientAppName
                                    # , SPFExe=_SPFExe
                                    # , hpc_mem_limit_GB=hpc_mem_limit_GB
                                    # , hpc_svc_timeout_hours=hpc_svc_timeout_hours
                                    # , user_idsid=user_idsid
                                    # , use_output_stage_share=use_output_stage_share
                                    # , convert_output_to_json = False 
                                    # , required_output_file = "out.tab"
                                    )

    # ArchiveName_PathObj = _SQLPFSvcClient.create_archive_from_file_list(required_input_files)
    # base64StringOutput = _SQLPFSvcClient.getFileContent(f"{str(ArchiveName_PathObj.resolve(strict=True))}", AsBase64String=True)
    _SQLPFSvcClient.Execute() # .__invokeExecute()

    # _SQLPFSvcClientPyReq = SQLPFSvcClientPyReq(SQLPFSvcEnv = SQLPFSvcEnv
    #                                 , SQLPFSvcURL = _SQLPFSvcURL
    #                                 , SQLPF_CMD_Args_List = _SQLPF_CMD_Args_List
    #                                 , SPFSQL_File=_Svc_SPFSQL_file
    #                                 , SPFInstance=_SPFInstance
    #                                 , SPFLogLevel=_SPFLogLevel
    #                                 , required_input_files_list_csv = required_input_files
    #                                 , required_output_files_folder_list_csv = required_output_files
    #                                 , optional_output_files_folder_list_csv = optional_output_files
    #                                 , display_svc_console_flag = display_svc_console_flag
    #                                 , copy_log_files_flag = copy_log_files_flag
    #                                 , continue_on_error = continue_on_error
    #                                 , begin_hpc_svc_ver = begin_hpc_svc_ver
    #                                 , ClientAppName=_ClientAppName
    #                                 , SPFExe=_SPFExe_PyReq
    #                                 , hpc_mem_limit_GB=hpc_mem_limit_GB
    #                                 , hpc_svc_timeout_hours=hpc_svc_timeout_hours
    #                                 , user_idsid=user_idsid
    #                                 , use_output_stage_share=use_output_stage_share)
    # _SQLPFSvcClientPyReq.Execute()
    print("\nExecuting in command line mode : End")