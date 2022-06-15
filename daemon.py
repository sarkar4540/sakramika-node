import json
import math
import sys
import traceback
import requests
import sqlite3

import time
from subprocess import Popen, PIPE

TASK_SYSTEM = 0
TASK_SERVICE = 1
TASK_WORKFLOW = 2
TASK_WEB = 3
TASK_UI = 4
TASK_SCRIPT = 5
TASK_DECISION = 6
TASK_MAP = 7
TASK_REDUCE = 8
TASK_FILTER = 9
TASK_TERMINAL = 10

STATE_QUEUED = 0
STATE_LOADED = 1
STATE_STARTED = 2
STATE_ENDED = 3
STATE_MARKED = 4
STATE_KILLED = -1
STATE_FAILED = -2

DATATYPE_NONE = -1
DATATYPE_INT = 0
DATATYPE_FLOAT = 1
DATATYPE_TEXT = 2
DATATYPE_STRUCTURE = 3


class Jallad:
    def __init__(self, db_name="datastore.db", registry_protocol="http:", registry_host="localhost", registry_port="5001"):
        self.db_name = db_name
        self.registry_protocol = registry_protocol
        self.registry_host = registry_host
        self.registry_port = registry_port
        self.db = sqlite3.connect(self.db_name)
        cur = self.db.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS Registry(Id INTEGER PRIMARY KEY AUTOINCREMENT, IpAddress TEXT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS Workflow(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, InputDataTypeId INT, OutputDataTypeId INT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS Node(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, IpAddress TEXT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS Service(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, NodeId INT REFERENCES Node(Id), WorkflowId INT REFERENCES Workflow(Id),NodeServiceId INT, UniformServiceId INT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS Task(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Type INT, InputDataTypeId INT, OutputDataTypeId INT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS TaskParam(Id INTEGER PRIMARY KEY AUTOINCREMENT, TaskId INT REFERENCES Task(Id), Title TEXT, Value TEXT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS TaskInstance(Id INTEGER PRIMARY KEY AUTOINCREMENT, WorkflowId INT REFERENCES Workflow(Id), TaskId INT REFERENCES Task(Id), ScreenX INT, ScreenY INT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS Edge(Id INTEGER PRIMARY KEY AUTOINCREMENT,  WorkflowId INT REFERENCES Workflow(Id), TaskInstanceId1 INT REFERENCES TaskInstance(Id), DataIndexId1 INT REFERENCES DataIndex(Id), TaskInstanceId2 INT REFERENCES TaskInstance(Id), DataIndexId2 INT REFERENCES DataIndex(Id));")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS DataIndex(Id INTEGER PRIMARY KEY AUTOINCREMENT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS DataIndexValue(Id INTEGER PRIMARY KEY AUTOINCREMENT, DataIndexId INT REFERENCES DataIndex(Id), Value INT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS Data(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, DataTypeId INT, Created TEXT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS UnitData(Id INTEGER PRIMARY KEY AUTOINCREMENT, DataId INT REFERENCES Data(Id), Value TEXT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS WorkflowExecution(Id INTEGER PRIMARY KEY AUTOINCREMENT,  WorkflowId INT REFERENCES Workflow(Id), EntryTime TEXT, InputDataId INT REFERENCES Data(Id), ExecutionState INT, StartTime TEXT, OutputDataId INT REFERENCES Data(Id), EndTime TEXT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS WorkflowExecutionParams(Id INTEGER PRIMARY KEY AUTOINCREMENT, WorkflowExecutionId INT REFERENCES WorkflowExecution(Id), Title TEXT, Value TEXT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS TaskInstanceExecution(Id INTEGER PRIMARY KEY AUTOINCREMENT, WorkflowExecutionId INT REFERENCES WorkflowExecution(Id), TaskInstanceId INT REFERENCES TaskInstance(Id), EntryTime TEXT, InputDataId INT REFERENCES Data(Id), ExecutionState INT, StartTime TEXT, OutputDataId INT REFERENCES Data(Id), EndTime TEXT);")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS TaskInstanceExecutionParams(Id INTEGER PRIMARY KEY AUTOINCREMENT, TaskInstanceExecutionId INT REFERENCES TaskInstanceExecution(Id), Title TEXT, Value TEXT);")
        self.db.commit()
        cur.close()

    def updateDataTypes(self):
        self.data_types = requests.get(
            self.registry_protocol+'//'+self.registry_host+':'+self.registry_port+"/datatype").json()

    def dataType(self, id: int):
        for dataType in self.data_types:
            if dataType['id'] == id:
                return dataType
        return {'id': 0, 'base': DATATYPE_NONE, 'length': 0}

    def data(self, id: int):
        cur = self.db.cursor()
        data = None
        for row in cur.execute("SELECT * FROM Data WHERE Id=?;", [id]):
            data = dict(zip(['id', 'title', 'dataTypeId', 'created'], row))
            cur2 = self.db.cursor()
            data['values'] = [row2[0] for row2 in cur2.execute(
                "SELECT Value FROM UnitData Where DataId=?;", [data['id']])]
            cur2.close()
        cur.close()
        return data

    def saveData(self, dataTypeId: int, values: list, title: str = 'Interprocess'):
        cur = self.db.cursor()
        cur.execute("INSERT INTO Data (Title, DataTypeId, Created) VALUES (?,?,?);",
                    [title, dataTypeId, time.time()])
        dataId = cur.lastrowid
        for unitdata in values:
            cur.execute("INSERT INTO UnitData (DataId,Value) VALUES (?,?);",
                        [dataId, unitdata])
        self.db.commit()
        cur.close()
        return dataId

    def workflow(self, id: int):
        cur = self.db.cursor()
        workflow = [dict(zip(['id', 'title', 'inputDataTypeId', 'outputDataTypeId'], row)) for row in cur.execute(
            "SELECT * FROM Workflow WHERE Id=(?);", (id))]
        cur.close()
        return workflow

    def edge(self, id: int):
        cur = self.db.cursor()
        edge = None
        for row in cur.execute("SELECT * FROM Edge WHERE Id=?;", [id]):
            edge = dict(
                zip(['id', 'workflowId', 'taskInstanceId1', 'dataIndexId1', 'taskInstanceId2', 'dataIndexId2'], row))
            cur2 = self.db.cursor()
            edge['dataIndex1'] = [row2[0] for row2 in cur2.execute(
                "SELECT Value FROM DataIndexValue WHERE DataIndexId=?;", [edge['dataIndexId1']])]
            edge['dataIndex2'] = [row2[0] for row2 in cur2.execute(
                "SELECT Value FROM DataIndexValue WHERE DataIndexId=?;", [edge['dataIndexId2']])]
            cur2.close()
        self.db.commit()
        return edge

    def task(self, id: int):
        cur = self.db.cursor()
        task = None
        for row in cur.execute("SELECT * FROM Task WHERE Id=?;", [id]):
            task = dict(
                zip(['id', 'title', 'type', 'inputDataTypeId', 'outputDataTypeId'], row))
            cur2 = self.db.cursor()
            for row2 in cur2.execute("SELECT Title,Value FROM TaskParam WHERE TaskId=?;", [task['id']]):
                task[row2[0]] = row2[1]
            cur2.close()
        cur.close()
        return task

    def taskInstance(self, id: int, taskId: int = None):
        cur = self.db.cursor()
        task = None
        for row in cur.execute("SELECT * FROM TaskInstance WHERE Id=?;", [id]):
            task = dict(
                zip(['id', 'workflowId', 'taskId', 'screenX', 'screenY'], row))
            cur2 = self.db.cursor()
            if task['taskId'] == 0:
                start = len([row2[0] for row2 in cur2.execute(
                    "SELECT Id FROM TaskInstance WHERE TaskId=0 AND Id=? AND Id=(SELECT Id FROM TaskInstance WHERE WorkflowId=? AND TaskId=0 ORDER BY Id ASC LIMIT 1);",
                    [task['id'], task['workflowId']])]) > 0
                for row2 in cur2.execute("SELECT InputDataTypeId,OutputDataTypeId FROM Workflow WHERE Id=?;", [task['workflowId']]):
                    task['title'] = "start" if start else "end"
                    task['type'] = TASK_TERMINAL
                    task['outputDataTypeId'] = row2[0] if start else 0
                    task['inputDataTypeId'] = 0 if start else row2[1]
            elif taskId is not None:
                task.update(self.task(taskId))
            else:
                task.update(self.task(task['taskId']))
            cur2.close()
        cur.close()
        return task

    def dataUsingDataIndex(self, data, dataIndex):
        values = []
        # print(":dataIndexed:data:"+str(data))
        if data is not None:
            leftDataTypes = [data['dataTypeId']]
            listOfIndices = []
            foundIndex = False
            j = 1
            while(len(leftDataTypes) > 0):
                currentDataTypeId = leftDataTypes.pop(0)
                if currentDataTypeId > 0:
                    currentDataType = self.dataType(currentDataTypeId)
                    currentIndex = listOfIndices.pop(
                        0) if len(listOfIndices) > 0 else []
                    if currentIndex == dataIndex:
                        foundIndex = True
                    if currentDataType['length'] > 0 and len(currentDataType['subDataTypes']) > 0:
                        for i in range(0, currentDataType['length']):
                            for g in range(0, len(currentDataType['subDataTypes'])):
                                leftDataTypes.insert(i*len(currentDataType['subDataTypes'])+g,
                                                     currentDataType['subDataTypes'][g]['subDataTypeId'])
                                subDataTypeIndex = list(currentIndex)
                                subDataTypeIndex.extend([i+1, g+1])
                                listOfIndices.insert(
                                    i*len(currentDataType['subDataTypes'])+g, subDataTypeIndex)
                    else:
                        if foundIndex:
                            #print(":dataIndexed:j:"+str(j))
                            print(":dataIndexed:currentIndex:"+str(currentIndex))
                            if currentIndex[:len(dataIndex)] == dataIndex:
                                print(j)
                                print(json.dumps(data))
                                print(data['values'][j-1])
                                values.append(data['values'][j-1])
                            else:
                                break
                        j = j+1
            # print(str(data['values'])+' '+str(dataIndex)+' :dataIndexed: '+str(values))
        return values

    def mergePartialIndexing(self, finalDataTypeId, edgesWithData):
        if finalDataTypeId == 0:
            return []
        values = []
        leftDataTypes = [finalDataTypeId]
        listOfIndices = []
        while(len(leftDataTypes) > 0):
            currentDataType = self.dataType(leftDataTypes.pop(0))
            currentIndex = listOfIndices.pop(
                0) if len(listOfIndices) > 0 else []
            for edgeWithData in edgesWithData:
                # print(str(currentIndex)+" "+str(edgeWithData['dataIndex2'])+" "+str(edgeWithData['data1']))
                if edgeWithData['dataIndex2'] == currentIndex:
                    values.extend(edgeWithData['data1'])
            if currentDataType['length'] > 0 and len(currentDataType['subDataTypes']) > 0:
                for i in range(0, currentDataType['length']):
                    for g in range(0, len(currentDataType['subDataTypes'])):
                        leftDataTypes.insert(i*len(currentDataType['subDataTypes'])+g,
                                             currentDataType['subDataTypes'][g]['subDataTypeId'])
                        subDataTypeIndex = list(currentIndex)
                        subDataTypeIndex.extend([i+1, g+1])
                        listOfIndices.insert(
                            i*len(currentDataType['subDataTypes'])+g, subDataTypeIndex)
                        # print(str(listOfIndices))
        #print(str(finalDataTypeId)+' :mergePartial: '+str(values))
        return values

    def queueNextTaskInstances(self):
        cur = self.db.cursor()
        queuedTaskInstances = [taskInstance for taskInstance in cur.execute(
            "SELECT TaskInstanceId,WorkflowExecutionId FROM TaskInstanceExecution WHERE ExecutionState=?;", [STATE_QUEUED])]
        nextTaskInstances = []
        for unmarkedTaskInstanceExecution in cur.execute(
                "SELECT TaskInstanceExecution.Id,TaskInstanceExecution.TaskInstanceId,TaskInstanceExecution.OutputDataId,WorkflowExecution.Id,WorkflowExecution.WorkflowId FROM TaskInstanceExecution JOIN WorkflowExecution ON (TaskInstanceExecution.WorkflowExecutionId=WorkflowExecution.Id) WHERE TaskInstanceExecution.ExecutionState=?", [STATE_ENDED]):
            cur2 = self.db.cursor()
            cur2.execute("UPDATE TaskInstanceExecution SET ExecutionState=? WHERE Id=?;", [
                STATE_MARKED, unmarkedTaskInstanceExecution[0]])
            # print("queueNext:unmarked:"+str(unmarkedTaskInstanceExecution))
            self.db.commit()
            negEdgeTaskInstance = None
            edgeTaskInstances = []
            for edgeTaskInstance in cur2.execute(
                    "SELECT Id FROM Edge WHERE WorkflowId=? AND TaskInstanceId1=?;", [unmarkedTaskInstanceExecution[4], unmarkedTaskInstanceExecution[1]]):
                edge = self.edge(edgeTaskInstance[0])
                taskInstanceExecution = [
                    edge['taskInstanceId2'], unmarkedTaskInstanceExecution[3]]
                isUnique = True
                for queuedTaskInstance in queuedTaskInstances:
                    if taskInstanceExecution == queuedTaskInstance:
                        isUnique = False
                        break
                if isUnique:
                    data = self.data(unmarkedTaskInstanceExecution[2])
                    hasNegValue = True
                    isNegEdge = len(
                        edge['dataIndex1']) == 2 and edge['dataIndex1'][0] == 0 and edge['dataIndex1'][1] == 0
                    if data is not None:
                        hasNegValue = len(
                            data['values']) == 1 and data['values'][0] == 0
                    if isNegEdge and hasNegValue:
                        negEdgeTaskInstance = taskInstanceExecution
                    elif taskInstanceExecution not in edgeTaskInstances:
                        edgeTaskInstances.append(taskInstanceExecution)
            if negEdgeTaskInstance is not None:
                nextTaskInstances.append(negEdgeTaskInstance)
                queuedTaskInstances.append(negEdgeTaskInstance)
            else:
                nextTaskInstances.extend(edgeTaskInstances)
                queuedTaskInstances.extend(edgeTaskInstances)
            cur2.close()
        # print("queueNext:next:"+str(nextTaskInstances))
        for nextTaskInstance in nextTaskInstances:
            cur.execute("INSERT INTO TaskInstanceExecution (WorkflowExecutionId,TaskInstanceId,EntryTime,ExecutionState) VALUES (?,?,?,?);", [
                        nextTaskInstance[1], nextTaskInstance[0], time.time(), STATE_QUEUED])
            # print("queueNext:added:"+str(cur.lastrowid))
        self.db.commit()
        cur.close()

    def loadQueuedTaskInstances(self):
        cur = self.db.cursor()
        for loadedTaskInstance in cur.execute(
                "SELECT TaskInstanceExecution.Id,TaskInstanceExecution.TaskInstanceId,WorkflowExecution.Id,Workflow.Id,Workflow.OutputDataTypeId,Workflow.Title FROM TaskInstanceExecution JOIN WorkflowExecution JOIN Workflow ON (TaskInstanceExecution.WorkflowExecutionId=WorkflowExecution.Id AND WorkflowExecution.WorkflowId=Workflow.Id) WHERE TaskInstanceExecution.ExecutionState=?;", [STATE_QUEUED]):
            cur2 = self.db.cursor()
            edgesWithData = []
            toBeLoaded = True
            dataTypeId = [loadedTaskInstance[4], loadedTaskInstance[5], 0]
            for taskInstance in cur2.execute("SELECT InputDataTypeId,Task.Title FROM TaskInstance JOIN Task ON (TaskInstance.TaskId=Task.Id) WHERE TaskInstance.Id=?;", [loadedTaskInstance[1]]):
                dataTypeId = taskInstance
            for edgeTaskInstance in cur2.execute(
                    "SELECT Id FROM Edge WHERE WorkflowId=? AND TaskInstanceId2=?;", [loadedTaskInstance[3], loadedTaskInstance[1]]):
                edge = self.edge(edgeTaskInstance[0])
                cur3 = self.db.cursor()
                dataId1 = None
                foundButPending = False
                for taskInstanceExecution in cur3.execute("SELECT Id,ExecutionState,OutputDataId FROM TaskInstanceExecution WHERE WorkflowExecutionId=? AND TaskInstanceId=? ORDER BY Id DESC LIMIT 1;", [loadedTaskInstance[2], edge['taskInstanceId1']]):
                    if taskInstanceExecution[1] >= STATE_ENDED:
                        dataId1 = taskInstanceExecution[2]
                    else:
                        foundButPending = True
                if foundButPending:
                    toBeLoaded = False
                    break
                elif dataId1 is not None:
                    edge['data1'] = self.dataUsingDataIndex(
                        self.data(dataId1), edge['dataIndex1'])
                    edgesWithData.append(edge)
                cur3.close()
            if toBeLoaded:
                data = self.mergePartialIndexing(dataTypeId[0], edgesWithData)
                dataId = self.saveData(dataTypeId[0], data, dataTypeId[1]+" Input" if len(
                    dataTypeId) == 2 else dataTypeId[1]+" Result")
                cur2.execute("UPDATE TaskInstanceExecution SET InputDataId=?,ExecutionState=? WHERE Id=?;", [
                             dataId, STATE_LOADED, loadedTaskInstance[0]])
                self.db.commit()
            cur2.close()
        cur.close()
        self.db.commit()

    def dataToText(self, data):
        text: str = ""
        leftDataTypes = [data['dataTypeId']]
        currentIndex = 0
        while(len(leftDataTypes) > 0):
            currentDataType = self.dataType(leftDataTypes.pop(0))
            if currentDataType['length'] > 0 and len(currentDataType['subDataTypes']) > 0:
                for i in range(0, currentDataType['length']):
                    subDataTypes = [subDataType['subDataTypeId']
                                    for subDataType in currentDataType['subDataTypes']]
                    leftDataTypes[0:0] = subDataTypes
                text = text+str(currentDataType['length'])+" "
            else:
                text = text+(str(data['values'][currentIndex]) if currentDataType['base'] == DATATYPE_FLOAT or currentDataType['base'] == DATATYPE_INT else
                             str(len(data['values'][currentIndex]))+" "+" ".join(
                                 [str(ord(character)) for character in data['values'][currentIndex]])
                             if currentDataType['base'] == DATATYPE_TEXT else "0")+" "
                currentIndex = currentIndex+1
        return text

    def textToData(self, text: str, dataTypeId: int):
        values = []
        input = text.split(" ")
        leftDataTypes = [dataTypeId]
        currentIndex = 0
        while(len(leftDataTypes) > 0):
            currentDataType = self.dataType(leftDataTypes.pop(0))
            if currentDataType['length'] > 0 and len(currentDataType['subDataTypes']) > 0:
                for i in range(0, currentDataType['length']):
                    subDataTypes = [subDataType['subDataTypeId']
                                    for subDataType in currentDataType['subDataTypes']]
                    leftDataTypes[0:0] = subDataTypes
                if not (int(input[currentIndex]) == currentDataType['length']):
                    return None
            else:
                if currentDataType['base'] == DATATYPE_FLOAT:
                    values.append(float(input[currentIndex]))
                elif currentDataType['base'] == DATATYPE_INT:
                    values.append(int(input[currentIndex]))
                elif currentDataType['base'] == DATATYPE_TEXT:
                    textLength = int(input[currentIndex])
                    text = "".join([chr(int(input[index]))
                                    for index in range(currentIndex+1, textLength)])
                    values.append(text)
            currentIndex = currentIndex+1
        return values

    def dataToObject(self, data):
        dataType = self.dataType(data['dataTypeId'])
        if dataType['base'] == DATATYPE_FLOAT:
            return float(data['values'][0]), 1
        elif dataType['base'] == DATATYPE_INT:
            return int(data['values'][0]), 1
        elif dataType['base'] == DATATYPE_TEXT:
            return str(data['values'][0]), 1
        elif dataType['base'] == DATATYPE_STRUCTURE:
            obj = []
            index = 0
            for i in range(0, dataType['length']):
                elem = dict()
                for subDataType in dataType['subDataTypes']:
                    subData = {
                        'dataTypeId': subDataType['subDataTypeId'], 'values': data['values'][index:]}
                    subObject, length = self.dataToObject(subData)
                    elem[subDataType['title']] = subObject
                    index = index+length
                obj.append(elem)
            return obj, index
        else:
            return 0, 0

    def objectToData(self, obj, dataTypeId):
        dataType = self.dataType(dataTypeId)
        print(":objToData:datatype:"+str(dataType))
        if dataType['base'] == DATATYPE_FLOAT or dataType['base'] == DATATYPE_INT or dataType['base'] == DATATYPE_TEXT:
            return [obj]
        elif dataType['base'] == DATATYPE_STRUCTURE:
            values = []
            for elem in obj:
                print(":objToData:elem:"+str(elem))
                for subDataType in dataType['subDataTypes']:
                    print(":objToData:subDataType:"+str(subDataType))
                    values.extend(self.objectToData(
                        elem[subDataType['title']], subDataType['subDataTypeId']))
                print(":objToData:values:"+str(values))
            return values
        else:
            return []

    def executeSystem(self, task, inputData):
        cur = self.db.cursor()
        inputText = self.dataToText(inputData)
        cur.execute("UPDATE TaskInstanceExecution SET ExecutionState=?,StartTime=? WHERE Id=?;", [
                    STATE_STARTED, time.time(), task['taskInstanceExecutionId']])
        self.db.commit()
        p = Popen(task['command'].split(" "),
                  stdout=PIPE, stdin=PIPE, stderr=PIPE, text=True)
        outputText = p.communicate(inputText)
        outputDataId = self.saveData(task['outputDataTypeId'], self.textToData(
            outputText, task['outputDataTypeId']), str(task['title'])+" Result")
        cur.execute("UPDATE TaskInstanceExecution SET OutputDataId=?,ExecutionState=?,EndTime=? WHERE Id=?;", [
                    outputDataId, STATE_ENDED, time.time(), task['taskInstanceExecutionId']])
        self.db.commit()

    def startService(self, task, inputData):
        cur = self.db.cursor()
        nodes = [dict(zip(['id', 'ipAddress', 'workflowId', 'nodeServiceId'], row)) for row in cur.execute(
            "SELECT Node.id,Node.IpAddress,Service.WorkflowId,Service.NodeServiceId FROM Node JOIN Service ON(Node.Id=Service.NodeId OR Node.Id=0) WHERE Service.UniformServiceId=?;", [task['uniformServiceId']])]
        minCountNode = None
        minCount = math.inf
        for node in nodes:
            count = requests.get(
                node['ipAddress']+"/queueCount").json()
            if minCount > count:
                minCount = count
                minCountNode = node
        res = requests.post(minCountNode['ipAddress']+"/service/"+minCountNode['nodeServiceId']+"/start", json={
                            'values': inputData['values'], 'callBack': '/taskInstanceExecution/'+str(task['taskInstanceExecutionId'])+'/end'
                            }).json()
        cur.execute("UPDATE TaskInstanceExecution SET ExecutionState=?, StartTime=? WHERE Id=?;", [
            STATE_STARTED, time.time(), task['taskInstanceExecutionId']])
        cur.execute("INSERT INTO TaskInstanceExecutionParams (Title,Value,TaskInstanceExecutionId) VALUES (?,?,?);", [
                    'ipAddress', minCountNode['ipAddress'], task['taskInstanceExecutionId']])
        cur.execute("INSERT INTO TaskInstanceExecutionParams (Title,Value,TaskInstanceExecutionId) VALUES (?,?,?);", [
                    'workflowExecutionId', res['workflowExecutionId'], task['taskInstanceExecutionId']])
        self.db.commit()

    def checkService(self, task):
        cur = self.db.cursor()
        nodeInfo = dict([row for row in cur.execute(
            "SELECT Title,Value FROM TaskInstanceExecutionParams WHERE TaskInstanceExecutionId=?;", [task['taskInstanceExecutionId']])])
        if "workflowExecutionId" in nodeInfo and "ipAddress" in nodeInfo:
            res = requests.get(
                nodeInfo['ipAddress']+"/service/execution/"+nodeInfo['workflowExecutionId']).json()
            if(res["executionState"] == STATE_ENDED or res["executionState"] == STATE_MARKED):
                outputDataId = self.saveData(
                    task['outputDataTypeId'], res['outputDataValues'], str(task['title'])+" Result")
                cur.execute("UPDATE TaskInstanceExecution SET OutputDataId=?,ExecutionState=?,EndTime=? WHERE Id=?;", [
                    outputDataId, STATE_ENDED, time.time(), task['taskInstanceExecutionId']])

    def loadWorkflow(self, task, inputData):
        cur = self.db.cursor()
        cur.execute("UPDATE TaskInstanceExecution SET ExecutionState=?,StartTime=? WHERE Id=?;", [
                    STATE_STARTED, time.time(), task['taskInstanceExecutionId']])
        cur.execute("INSERT INTO WorkflowExecution (WorkflowId,InputDataId,ExecutionState,EntryTime) VALUES (?,?,?,?);", [
            task['workflowId'], inputData['id'], STATE_LOADED, time.time()])
        workflowExecutionId = cur.lastrowid
        cur.execute("UPDATE TaskInstanceExecution SET ExecutionState=? WHERE Id=?;", [
            STATE_STARTED, task['taskInstanceExecutionId']])
        cur.execute("INSERT INTO WorkflowExecutionParams (Title,Value,WorkflowExecutionId) VALUES (?,?,?);", [
                    'taskInstanceExecutionId', task['taskInstanceExecutionId'], workflowExecutionId])
        self.db.commit()

    def executeWeb(self, task, inputData):
        cur = self.db.cursor()
        inputObj = self.dataToObject(inputData)
        cur.execute("UPDATE TaskInstanceExecution SET ExecutionState=?,StartTime=? WHERE Id=?;", [
                    STATE_STARTED, time.time(), task['taskInstanceExecutionId']])
        self.db.commit()
        outputObj = requests.request(url=task['url'], method=task['method'], data=(
            json.dumps(inputObj) if task['sendBody'] else None)).json()
        outputDataId = self.saveData(task['outputDataTypeId'], self.objectToData(
            outputObj, task['outputDataTypeId']), str(task['title'])+" Result")
        cur.execute("UPDATE TaskInstanceExecution SET OutputDataId=?,ExecutionState=?,EndTime=? WHERE Id=?;", [
                    outputDataId, STATE_ENDED, time.time(), task['taskInstanceExecutionId']])

    def executeScript(self, task, inputData):
        cur = self.db.cursor()
        cur.execute("UPDATE TaskInstanceExecution SET ExecutionState=?,StartTime=? WHERE Id=?;", [
                    STATE_STARTED, time.time(), task['taskInstanceExecutionId']])
        self.db.commit()
        locals = {"input": self.dataToObject(inputData)[0]}
        try:
            exec(task['code'], globals(), locals)
            print(locals)
            outputDataId = 0
            if "output" in locals:
                outputData=self.objectToData(
                    locals["output"], task['outputDataTypeId'])
                print(outputData)
                outputDataId = self.saveData(task['outputDataTypeId'], outputData, str(task['title'])+" Result")
                print(outputDataId)
            cur.execute("UPDATE TaskInstanceExecution SET OutputDataId=?,ExecutionState=?,EndTime=? WHERE Id=?;", [
                    outputDataId, STATE_ENDED, time.time(), task['taskInstanceExecutionId']])
        except Exception:
            print(locals)
            print(traceback.format_exc())
            cur.execute("UPDATE TaskInstanceExecution SET ExecutionState=?,EndTime=? WHERE WorkflowExecutionId=?;", [
                    STATE_FAILED, time.time(), task['workflowExecutionId']])
            cur.execute("UPDATE WorkflowExecution SET ExecutionState=?,EndTime=? WHERE Id=?;", [
                    STATE_FAILED, time.time(), task['workflowExecutionId']])
        self.db.commit()

    def endWorkflowExecution(self, workflowExecution, outputData):
        cur = self.db.cursor()
        cur.execute("UPDATE WorkflowExecution SET OutputDataId=?,ExecutionState=?,EndTime=? WHERE Id=?;", [
                    outputData['id'], STATE_ENDED, time.time(), workflowExecution['id']])
        print("workflow:end:"+str(outputData['values']))
        params = {}
        for param in cur.execute("SELECT Title,Value FROM WorkflowExecutionParams WHERE WorkflowExecutionId=?;", [workflowExecution['id']]):
            params[param[0]] = param[1]
        if 'callBack' in params and 'remoteAddr' in params:
            requests.post("http://"+params['remoteAddr']+params['callBack'], json={
                'values': outputData['values']}).json()
        if 'taskInstanceExecutionId' in params:
            cur.execute("UPDATE TaskInstanceExecution SET OutputDataId=?,ExecutionState=?,EndTime=? WHERE Id=?;", [
                outputData['id'], STATE_ENDED, time.time(), params['taskInstanceExecutionId']])

    def executeLoadedTaskInstances(self):
        cur = self.db.cursor()
        for loadedTaskInstance in cur.execute(
                "SELECT Id,TaskInstanceId,InputDataId,WorkflowExecutionId FROM TaskInstanceExecution WHERE ExecutionState=?;", [STATE_LOADED]):
            cur2 = self.db.cursor()
            cur2.execute("UPDATE TaskInstanceExecution SET StartTime=?,ExecutionState=? WHERE Id=?;", [
                time.time(), STATE_STARTED, loadedTaskInstance[0]])
            self.db.commit()
            task = self.taskInstance(loadedTaskInstance[1])
            if task['type'] == TASK_DECISION:
                task = self.taskInstance(
                    loadedTaskInstance[1], task('subTaskId'))
            task['taskInstanceExecutionId'] = loadedTaskInstance[0]
            task['workflowExecutionId'] = loadedTaskInstance[3]
            inputData = self.data(loadedTaskInstance[2])
            if task['type'] == TASK_SYSTEM:
                self.executeSystem(task, inputData)
            elif task['type'] == TASK_SERVICE:
                self.startService(task, inputData)
            elif task['type'] == TASK_WORKFLOW:
                self.loadWorkflow(task, inputData)
            elif task['type'] == TASK_WEB:
                self.executeWeb(task, inputData)
            elif task['type'] == TASK_SCRIPT:
                self.executeScript(task, inputData)
            # elif task['type'] == TASK_MAP:
            #     for i in inputData['dataTypeId']:

            # elif task['type'] == TASK_REDUCE:
            elif task['type'] == TASK_TERMINAL:
                self.endWorkflowExecution(self.workflowExecution(
                    loadedTaskInstance[3]), inputData)
                cur2.execute("UPDATE TaskInstanceExecution SET EndTime=?,ExecutionState=? WHERE Id=?;", [
                    time.time(), STATE_ENDED, loadedTaskInstance[0]])
            cur2.close()
        cur.close()
        self.db.commit()

    def startLoadedWorkflows(self):
        cur = self.db.cursor()
        for row in cur.execute("SELECT * FROM WorkflowExecution WHERE ExecutionState=? ORDER BY Id ASC LIMIT 1;", [STATE_LOADED]):
            cur2 = self.db.cursor()
            workflowExecution = dict(
                zip(['id', 'workflowId', 'entryTime', 'inputDataId', 'executionState', 'startTime', 'outputDataId', 'endTime'], row))
            cur2.execute("UPDATE WorkflowExecution SET ExecutionState=?,StartTime=? WHERE Id=? RETURNING WorkflowId;", [
                STATE_STARTED, time.time(), workflowExecution['id']])
            terminals = [row2[0] for row2 in cur2.execute(
                'SELECT Id FROM TaskInstance WHERE WorkflowId=? AND TaskId=0 ORDER BY Id ASC LIMIT 1;', [workflowExecution['workflowId']])]
            for terminal in terminals:
                cur2.execute('INSERT INTO TaskInstanceExecution (WorkflowExecutionId,TaskInstanceId,EntryTime,InputDataId,ExecutionState,StartTime,OutputDataId,EndTime) VALUES (?,?,?,?,?,?,?,?);', [
                    workflowExecution['id'], terminal, time.time(), 0, STATE_ENDED, time.time(), workflowExecution['inputDataId'], time.time()])
            cur2.close()
        cur.close()
        self.db.commit()

    def workflowExecution(self, workflowExecutionId: int):
        cur = self.db.cursor()
        workflowExecution = None
        for row in cur.execute("SELECT * FROM WorkflowExecution WHERE Id=?;", [workflowExecutionId]):
            workflowExecution = dict(
                zip(['id', 'workflowId', 'entryTime', 'inputDataId', 'executionState', 'startTime', 'outputDataId', 'endTime'], row))
            for row2 in cur.execute("SELECT Title,Value FROM WorkflowExecutionParams WHERE WorkflowExecutionId=?;", [workflowExecutionId]):
                workflowExecution[row2[0]] = row2[1]
        return workflowExecution


    is_executing = True

    def stop(self):
        """Stops the daemon"""
        self.is_executing = False

    def sleep(self):
        """Makes the process sleep"""
        sleep_duration = self.next_start_time+self.sleep_interval-time.time()
        if(sleep_duration > 0):
            time.sleep(sleep_duration)
        self.next_start_time = time.time()

    def start(self, sleep_interval=5):
        """Runs the daemon iteratively

        :param sleep_interval: maximum sleep duration between iterations
        """
        self.is_executing = True
        self.sleep_interval = sleep_interval
        self.next_start_time = time.time()
        self.updateDataTypes()
        while self.is_executing:
            self.startLoadedWorkflows()
            self.queueNextTaskInstances()
            self.loadQueuedTaskInstances()
            self.executeLoadedTaskInstances()
            self.sleep()


jallad = Jallad()
jallad.start(0.5)
