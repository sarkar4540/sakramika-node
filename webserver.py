from time import time
import requests
from flask import Flask, request
from flask_cors import CORS
import sqlite3
import json

app = Flask(__name__)
CORS(app)
db_name = "datastore.db"

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


def setup():
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Workflow(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, InputDataTypeId INT, OutputDataTypeId INT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Node(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, IpAddress TEXT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Service(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, NodeId INT, WorkflowId INT,NodeServiceId INT, UniformServiceId INT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Task(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, Type INT, InputDataTypeId INT, OutputDataTypeId INT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS TaskParam(Id INTEGER PRIMARY KEY AUTOINCREMENT, TaskId INT, Title TEXT, Value TEXT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS TaskInstance(Id INTEGER PRIMARY KEY AUTOINCREMENT, WorkflowId INT, TaskId INT, ScreenX INT, ScreenY INT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Edge(Id INTEGER PRIMARY KEY AUTOINCREMENT, WorkflowId INT, TaskInstanceId1 INT, DataIndexId1 INT, TaskInstanceId2 INT, DataIndexId2 INT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS DataIndex(Id INTEGER PRIMARY KEY AUTOINCREMENT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS DataIndexValue(Id INTEGER PRIMARY KEY AUTOINCREMENT, DataIndexId INT, Value INT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Data(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT, DataTypeId INT, Created TEXT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS UnitData(Id INTEGER PRIMARY KEY AUTOINCREMENT, DataId INT, Value TEXT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS WorkflowExecution(Id INTEGER PRIMARY KEY AUTOINCREMENT, WorkflowId INT, EntryTime TEXT, InputDataId INT, ExecutionState INT, StartTime TEXT, OutputDataId INT, EndTime TEXT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS WorkflowExecutionParams(Id INTEGER PRIMARY KEY AUTOINCREMENT, WorkflowExecutionId INT, Title TEXT, Value TEXT);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS TaskInstanceExecution(Id INTEGER PRIMARY KEY AUTOINCREMENT, WorkflowExecutionId INT, TaskInstanceId INT, EntryTime TEXT, InputDataId INT, ExecutionState INT, StartTime TEXT, OutputDataId INT, EndTime TEXT);")
    db.commit()
    db.close()


@app.route("/")
def hello_world():
    return "ok"


@app.route("/workflow")
def workflow():
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    workflows = [dict(zip(['id', 'title', 'inputDataTypeId', 'outputDataTypeId'], row))
                 for row in cur.execute("SELECT * FROM Workflow;")]
    db.commit()
    db.close()
    return json.dumps(workflows)


@app.route("/workflow/<id>", methods=["GET", "POST", "PATCH", "DELETE"])
def workflow_id(id):
    if request.method == "GET":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        workflow = [dict(zip(['id', 'title', 'inputDataTypeId', 'outputDataTypeId'], row)) for row in cur.execute(
            "SELECT * FROM Workflow WHERE Id=(?);", (id))]
        db.commit()
        db.close()
        return json.dumps(workflow)
    elif request.method == "POST":
        workflow = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("INSERT INTO Workflow (Title, InputDataTypeId, OutputDataTypeId) VALUES (?,?,?);",
                    [workflow['title'], workflow['inputDataTypeId'], workflow['outputDataTypeId']])
        workflow['id'] = cur.lastrowid
        cur.execute("INSERT INTO TaskInstance (WorkflowId, ScreenX, ScreenY, TaskId) VALUES (?,300,50,0);",
                    [workflow['id']])
        cur.execute("INSERT INTO TaskInstance (WorkflowId, ScreenX, ScreenY, TaskId) VALUES (?,300,650,0);",
                    [workflow['id']])
        db.commit()
        db.close()
        return json.dumps(workflow)
    elif request.method == "PATCH":
        workflow = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("UPDATE Workflow SET Title=?, InputDataTypeId=?, OutputDataTypeId=? WHERE Id=?;",
                    [workflow['title'], workflow['inputDataTypeId'], workflow['outputDataTypeId'], id])
        workflow['id'] = id
        db.commit()
        db.close()
        return json.dumps(workflow)
    elif request.method == "DELETE":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("DELETE FROM Workflow WHERE Id=?;",
                    [id])
        cur.execute("DELETE FROM TaskInstance WHERE WorkflowId=?;",
                    [id])
        workflow = {"id": id, "deleted": True}
        db.commit()
        db.close()
        return json.dumps(workflow)
    return "{error:'Invalid method'}"


@app.route("/node")
def node():
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    nodes = [{"id": 0, "title": "This Node", "services": [dict(zip(
        ['id', 'title', 'nodeId', 'workflowId', 'nodeServiceId', 'uniformServiceId'], row2)) for row2 in cur.execute("SELECT * FROM Service Where NodeId=?;", [0])]}]
    for row in cur.execute("SELECT * FROM Node;"):
        node = dict(zip(['id', 'title', 'ip'], row))
        cur2 = db.cursor()
        node['services'] = [dict(zip(['id', 'title', 'nodeId', 'workflowId', 'nodeServiceId', 'uniformServiceId'], row2))
                            for row2 in cur2.execute("SELECT * FROM Service Where NodeId=?;", [node['id']])]
        nodes.append(node)
    db.close()
    return json.dumps(nodes)


@app.route("/node/<id>", methods=["GET", "POST", "PATCH", "DELETE"])
def node_id(id):
    if request.method == "GET":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        node = {}
        for row in cur.execute("SELECT * FROM Node WHERE Id=?;", [id]):
            node = dict(zip(['id', 'title', 'ip'], row))
            cur2 = db.cursor()
            node['services'] = [dict(zip(['id', 'title', 'nodeId', 'workflowId', 'nodeServiceId', 'uniformServiceId'], row2))
                                for row2 in cur2.execute("SELECT * FROM Service Where NodeId=?;", [node['id']])]
        db.close()
        return json.dumps(node)
    elif request.method == "POST":
        node = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        node["services"] = requests.get(
            'http://'+node['ipAddress']+"/service").json()
        cur.execute("INSERT INTO Node (Title,IpAddress) VALUES (?,?);",
                    [node['title'], node['ipAddress']])
        node['id'] = cur.lastrowid
        for service in node["services"]:
            cur.execute("INSERT INTO Service (Title,NodeId,NodeServiceId,UniformServiceId) VALUES (?,?,?);",
                        [service['title'], node['id'], service['id'], service['uniformServiceId']])

        db.commit()
        db.close()
        return json.dumps(node)
    elif request.method == "PATCH":
        node = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("UPDATE Node SET Title=? WHERE Id=?;",
                    [node['title'], id])
        node['id'] = cur.lastrowid
        db.commit()
        db.close()
        return json.dumps(node)
    elif request.method == "DELETE":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("DELETE FROM Node WHERE Id=?;",
                    [id])
        cur.execute("DELETE FROM Service WHERE NodeId=?;",
                    [id])
        node = {"id": id, "deleted": True}
        db.commit()
        db.close()
        return json.dumps(node)
    return "{error:'Invalid method'}"


@app.route("/workflow/<id>/service", methods=["GET", "POST", "DELETE"])
def workflow_id_service(id):
    if request.method == "GET":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        workflow = [dict(zip(['id', 'title', 'nodeId', 'workflowId', 'nodeServiceId', 'uniformServiceId'], row))
                    for row in cur.execute("SELECT * FROM Service WHERE WorkflowId=(?);", (id))]
        db.commit()
        db.close()
        return json.dumps(workflow)
    elif request.method == "POST":
        service = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("INSERT INTO Service (Title,NodeId,WorkflowId,UniformServiceId) VALUES (?,?,?,?);",
                    [service['title'], 0, id, service['uniformServiceId']])
        service['id'] = cur.lastrowid
        service['workflowId'] = id
        db.commit()
        db.close()
        return json.dumps(service)
    elif request.method == "DELETE":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("DELETE FROM Service WHERE WorkflowId=?;",
                    [id])
        service = {"workflowId": id, "deleted": True}
        db.commit()
        db.close()
        return json.dumps(service)
    return "{error:'Invalid method'}"


@app.route("/service")
def service():
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    services = [dict(zip(['id', 'title', 'uniformServiceId'], row2)) for row2 in cur.execute(
        "SELECT Id,Title,UniformServiceId FROM Service WHERE NodeId=0;")]
    db.commit()
    db.close()
    return json.dumps(services)


@app.route("/service/queueCount")
def service_queuecount():
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    count = 0
    for row2 in cur.execute(
            "SELECT COUNT(Id) FROM WorkflowExecution WHERE ExecutionState>=? AND ExecutionState<=?;", [STATE_QUEUED, STATE_STARTED]):
        count = row2[0]
    db.commit()
    db.close()
    return json.dumps({'count': count})


@app.route("/service/<serviceId>/<nodeId>/<inputDataId>", methods=['POST'])
def service_start(serviceId, nodeId, inputDataId):
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    nodes = [dict(zip(['id', 'ipAddress', 'title', 'workflowId', 'nodeServiceId'], row)) for row in cur.execute(
        "SELECT Node.id,Node.IpAddress,Service.Title,Service.WorkflowId,Service.NodeServiceId FROM Node JOIN Service ON(Node.Id=Service.NodeId) WHERE Node.Id=? AND Service.Id=?;", [nodeId, serviceId])]
    if len(nodes) > 0:
        taskInstanceExecutionId = None
        for row in cur.execute("INSERT INTO TaskInstanceExecution (WorkflowExecutionId,TaskInstanceId,EntryTime,ExecutionState,InputDataId,StartTime) VALUES (?,?,?,?) RETURNING Id;", [
                0, 0, time(), STATE_STARTED, inputDataId, time()]):
            taskInstanceExecutionId = row[0]
        inputData = None
        for row in cur.execute("SELECT * FROM Data WHERE Id=?;", [inputDataId]):
            inputData = dict(
                zip(['id', 'title', 'dataTypeId', 'created'], row))
            cur2 = db.cursor()
            inputData['values'] = [row2[0] for row2 in cur2.execute(
                "SELECT Value FROM UnitData Where DataId=?;", [data['id']])]
            cur2.close()
        node = nodes[0]
        res = requests.post(node['ipAddress']+"/service/"+node['nodeServiceId']+"/start", json={
            'values': inputData['values'], 'callBack': '/taskInstanceExecution/'+str(taskInstanceExecutionId)+'/end'}).json()
        cur.execute("INSERT INTO TaskInstanceExecutionParams (Title,Value,TaskInstanceExecutionId) VALUES (?,?,?);", [
            'title', node['title'], taskInstanceExecutionId])
        cur.execute("INSERT INTO TaskInstanceExecutionParams (Title,Value,TaskInstanceExecutionId) VALUES (?,?,?);", [
            'ipAddress', node['ipAddress'], taskInstanceExecutionId])
        cur.execute("INSERT INTO TaskInstanceExecutionParams (Title,Value,TaskInstanceExecutionId) VALUES (?,?,?);", [
            'remoteWorkflowExecutionId', res['workflowExecutionId'], taskInstanceExecutionId])
        db.commit()
        node.update({ "remote": res, "taskInstanceExecutionId": taskInstanceExecutionId})
        return json.dumps(node)
    db.close()
    return json.dumps({"error": "nodeId not found"})


@app.route("/service/<id>/start", methods=['POST'])
def service_start_id(id):
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    data = request.get_json(force=True)
    headers_list = request.headers.getlist("X-Forwarded-For")
    data['remoteAddr'] = headers_list[0] if headers_list else request.remote_addr
    workflow = [row2 for row2 in cur.execute(
        "SELECT WorkflowId,InputDataTypeId,Service.Title FROM Service JOIN Workflow ON Service.WorkflowId=Workflow.Id WHERE Id=?;", [id])][0]
    cur.execute("INSERT INTO WorkflowExecution (WorkflowId,ExecutionState,EntryTime) VALUES (?,?,?);", [
                workflow[0], STATE_QUEUED, time()])
    workflowExecutionId = cur.lastrowid
    for key in data:
        if key not in ['values']:
            cur.execute("INSERT INTO WorkflowExecutionParams (WorkflowExecutionId,Title,Value) VALUES (?,?,?);", [
                        workflowExecutionId, key, data[key]])
    db.commit()
    cur.execute("INSERT INTO Data (Title, DataTypeId, Created) VALUES (?,?,?);",
                [workflow[2]+"#"+str(workflow[0])+" Input", workflow[1], time()])
    inputDataId = cur.lastrowid
    for unitdata in data['values']:
        cur.execute("INSERT INTO UnitData (DataId,Value) VALUES (?,?);",
                    [inputDataId, unitdata])
    cur.execute("UPDATE WorkflowExecution SET InputDataId=?,ExecutionState=? WHERE Id=?;", [
                inputDataId, STATE_LOADED, workflowExecutionId])
    db.commit()
    db.close()
    return json.dumps({'workflowExecutionId': workflowExecutionId, "title": workflow[2]})


@app.route("/taskInstanceExecution/<taskExecutionId>/end", methods=['POST'])
def service_callback(taskExecutionId):
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    data = request.get_json(force=True)
    tasks = [row for row in cur.execute(
        "SELECT Task.OutputDataTypeId, Task.Title FROM TaskInstanceExecution JOIN TaskInstance JOIN Task ON (TaskInstanceExecution.TaskInstanceId=TaskInstance.Id AND TaskInstance.TaskId = Task.Id) WHERE TaskInstanceExecution.Id=?;"[taskExecutionId])]
    if len(tasks) > 0:
        cur.execute("INSERT INTO Data (Title, DataTypeId, Created) VALUES (?,?,?);",
                    [tasks[1]+"#"+str(taskExecutionId)+" Result", tasks[0], time()])
        outputDataId = cur.lastrowid
        for unitdata in data['values']:
            cur.execute("INSERT INTO UnitData (DataId,Value) VALUES (?,?);",
                        [outputDataId, unitdata])
        cur.execute("UPDATE TaskInstanceExecution SET OutputDataId=?,ExecutionState=? WHERE Id=?;", [
                    outputDataId, STATE_ENDED, taskExecutionId])
    db.close()
    return json.dumps({"success": "True"})


@app.route("/service/execution")
def service_execution():
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    executions = []
    for row in cur.execute(
            "SELECT * FROM (SELECT Id,WorkflowId,ExecutionState,EntryTime,StartTime,EndTime,InputDataId,OutputDataId,1 FROM WorkflowExecution UNION SELECT Id,TaskInstanceId,ExecutionState,EntryTime,StartTime,EndTime,InputDataId,OutputDataId,2 FROM TaskInstanceExecution WHERE TaskInstanceId=0) ORDER BY EntryTime DESC;"):
        execution = dict(zip(['executionId', 'referenceId', 'executionState', 'entryTime',
                              'startTime', 'endTime', 'inputDataId', 'outputDataId', 'type'], row))
        cur2 = db.cursor()
        if execution['type'] == 1:
            execution['title'] = [row2[0] for row2 in cur2.execute(
                "SELECT Title FROM Workflow WHERE Id=? ORDER BY Id ASC;", [execution['referenceId']])][0]
            execution.update(dict([row2 for row2 in cur2.execute(
                "SELECT Title,Value FROM WorkflowExecutionParams WHERE WorkflowExecutionId=?;", [execution['referenceId']])]))
        elif execution['type'] == 2:
            execution.update(dict([row2 for row2 in cur2.execute(
                "SELECT Title,Value FROM TaskInstanceExecutionParams WHERE TaskInstanceExecutionId=?;", [execution['referenceId']])]))
        executions.append(execution)
    db.commit()
    db.close()
    return json.dumps(executions)


@app.route("/service/execution/<workflowExecutionId>")
def service_execution_id(workflowExecutionId):
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    execution = [dict(zip(['workflowExecutionId', 'executionState', 'entryTime', 'startTime', 'endTime'], row2)) for row2 in cur.execute(
        "SELECT Id,ExecutionState,EntryTime,StartTime,EndTime FROM WorkflowExecution WHERE Id=?;", [workflowExecutionId])]
    if execution['executionState'] == STATE_ENDED or execution['executionState'] == STATE_MARKED:
        outputDataId = [row2[0] for row2 in cur.execute(
            "SELECT OutputDataId FROM WorkflowExecution WHERE Id=?;", [workflowExecutionId])]
        execution['outputDataValues'] = [row2[0] for row2 in cur.execute(
            "SELECT Value FROM UnitData WHERE DataId=? ORDER BY Id ASC;", [outputDataId[0]])]
    db.commit()
    db.close()
    return json.dumps(execution)


@app.route("/service/execution/<workflowExecutionId>/kill")
def service_kill(workflowExecutionId):
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    cur.execute("UPDATE WorkflowExecution SET ExecutionState=? WHERE Id=?;", [
                STATE_KILLED, workflowExecutionId])
    cur.execute("UPDATE TaskInstanceExecution SET ExecutionState=? WHERE WorkFlowExecutionId=?;", [
                STATE_KILLED, workflowExecutionId])
    db.commit()
    db.close()
    return json.dumps({'workflowExecutionId': workflowExecutionId, 'executionState': STATE_KILLED})


@app.route("/task")
def task():
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    tasks = []
    for row in cur.execute("SELECT * FROM Task;"):
        task = dict(
            zip(['id', 'title', 'type', 'inputDataTypeId', 'outputDataTypeId'], row))
        cur2 = db.cursor()
        for row2 in cur2.execute("SELECT Title,Value FROM TaskParam WHERE TaskId=?;", [task['id']]):
            task[row2[0]] = row2[1]
        tasks.append(task)
        cur2.close()
    db.commit()
    db.close()
    return json.dumps(tasks)


@app.route("/task/<id>", methods=["GET", "POST", "DELETE"])
def task_id(id):
    if request.method == "GET":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        task = None
        for row in cur.execute("SELECT * FROM Task WHERE Id=?;", [id]):
            task = dict(
                zip(['id', 'title', 'type', 'inputDataTypeId', 'outputDataTypeId'], row))
            cur2 = db.cursor()
            for row2 in cur2.execute("SELECT Title,Value FROM TaskParam WHERE TaskId=?;", [task['id']]):
                task[row2[0]] = row2[1]
            cur2.close()
        db.commit()
        db.close()
        return json.dumps(task)
    elif request.method == "POST":
        task = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        if int(id) == 0:
            cur.execute("INSERT INTO Task (Title,Type,InputDataTypeId,OutputDataTypeId) VALUES (?,?,?,?);",
                        [task['title'], task['type'], task['inputDataTypeId'], task['outputDataTypeId']])
            task['id'] = cur.lastrowid
        else:
            cur.execute("UPDATE Task SET Title=?,Type=?,InputDataTypeId=?,OutputDataTypeId=? WHERE Id=?;",
                        [task['title'], task['type'], task['inputDataTypeId'], task['outputDataTypeId'], id])
            task['id'] = id
            cur.execute("DELETE FROM TaskParam WHERE TaskId=?;",
                        [id])
        for title, value in task.items():
            if not title in ['id', 'title', 'type', 'inputDataTypeId', 'outputDataTypeId']:
                cur.execute("INSERT INTO TaskParam (Title,Value,TaskId) VALUES (?,?,?);", [
                            title, value, task['id']])
        db.commit()
        db.close()
        return json.dumps(task)
    elif request.method == "DELETE":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("DELETE FROM Task WHERE Id=?;",
                    [id])
        cur.execute("DELETE FROM TaskParam WHERE TaskId=?;",
                    [id])
        task = {"id": id, "deleted": True}
        db.commit()
        db.close()
        return json.dumps(task)
    return "{error:'Invalid method'}"


@app.route("/workflow/<workflowId>/taskInstance")
def taskInstance(workflowId):
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    tasks = []
    start = True
    for row in cur.execute("SELECT * FROM TaskInstance WHERE WorkflowId=?;", [workflowId]):
        task = dict(
            zip(['id', 'workflowId', 'taskId', 'screenX', 'screenY'], row))
        cur2 = db.cursor()
        if task['taskId'] == 0:
            for row2 in cur2.execute("SELECT InputDataTypeId,OutputDataTypeId FROM Workflow WHERE Id=?;", [workflowId]):
                task['title'] = "start" if start else "end"
                task['type'] = TASK_TERMINAL
                task['outputDataTypeId'] = row2[0] if start else 0
                task['inputDataTypeId'] = 0 if start else row2[1]
            start = False
        else:
            for row2 in cur2.execute("SELECT * FROM Task WHERE Id=?;", [task['taskId']]):
                task.update(dict(
                    zip(['taskId', 'title', 'type', 'inputDataTypeId', 'outputDataTypeId'], row2)))
        tasks.append(task)
        cur2.close()
    db.commit()
    db.close()
    return json.dumps(tasks)


@app.route("/taskInstance/<id>", methods=["GET", "POST", "DELETE"])
def taskInstance_id(id):
    if request.method == "GET":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        task = None
        for row in cur.execute("SELECT * FROM TaskInstance WHERE Id=?;", [id]):
            task = dict(
                zip(['id', 'workflowId', 'taskId', 'screenX', 'screenY'], row))
            cur2 = db.cursor()
            if task['taskId'] == 0:
                start = len([row2[0] for row2 in cur2.execute(
                    "SELECT Id FROM TaskInstance WHERE TaskId=0 AND Id=? AND Id=(SELECT Id FROM TaskInstance WHERE WorkflowId=? AND TaskId=0 ORDER BY Id ASC LIMIT 1);",
                    [task['id'], task['workflowId']])]) > 0
                for row2 in cur2.execute("SELECT InputDataTypeId,OutputDataTypeId FROM Workflow WHERE Id=?;", [task['workflowId']]):
                    task['title'] = "start" if start else "end"
                    task['type'] = TASK_TERMINAL
                    task['outputDataTypeId'] = row2[0] if start else 0
                    task['inputDataTypeId'] = 0 if start else row2[1]
            else:
                for row2 in cur2.execute("SELECT * FROM Task WHERE Id=?;", [task['taskId']]):
                    task.update(dict(
                        zip(['taskId', 'title', 'type', 'inputDataTypeId', 'outputDataTypeId'], row2)))
            cur2.close()
        db.commit()
        db.close()
        return json.dumps(task)
    elif request.method == "POST":
        task = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        if int(id) == 0:
            cur.execute("INSERT INTO TaskInstance (WorkflowId,TaskId,ScreenX,ScreenY) VALUES (?,?,?,?);",
                        [task['workflowId'], task['taskId'], task['screenX'], task['screenY']])
            task['id'] = cur.lastrowid
        else:
            cur.execute("UPDATE TaskInstance SET ScreenX=?, ScreenY=? WHERE Id=?;",
                        [task['screenX'], task['screenY'], id])
            task['id'] = id
        db.commit()
        db.close()
        return json.dumps(task)
    elif request.method == "DELETE":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("DELETE FROM TaskInstance WHERE Id=?;",
                    [id])
        for row in cur.execute("DELETE FROM Edge WHERE TaskInstanceId1=? OR TaskInstanceId2=? RETURNING DataIndexId1,DataIndexId2;",
                               [id, id]):
            cur.execute("DELETE FROM DataIndex WHERE Id=? OR Id=?;", row)
            cur.execute(
                "DELETE FROM DataIndexValue WHERE DataIndexId=? OR DataIndexId=?;", row)
        task = {"id": id, "deleted": True}
        db.commit()
        db.close()
        return json.dumps(task)
    return "{error:'Invalid method'}"


@app.route("/workflow/<workflowId>/edge")
def edge(workflowId):
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    edges = []
    for row in cur.execute("SELECT * FROM Edge WHERE WorkflowId=?;", [workflowId]):
        edge = dict(
            zip(['id', 'workflowId', 'taskInstanceId1', 'dataIndexId1', 'taskInstanceId2', 'dataIndexId2'], row))
        cur2 = db.cursor()
        edge['dataIndex1'] = [row2[0] for row2 in cur2.execute(
            "SELECT Value FROM DataIndexValue WHERE DataIndexId=?;", [edge['dataIndexId1']])]
        edge['dataIndex2'] = [row2[0] for row2 in cur2.execute(
            "SELECT Value FROM DataIndexValue WHERE DataIndexId=?;", [edge['dataIndexId2']])]
        cur2.close()
        edges.append(edge)
    db.commit()
    db.close()
    return json.dumps(edges)


@app.route("/edge/<id>", methods=["GET", "POST", "DELETE"])
def edge_id(id):
    if request.method == "GET":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        edge = None
        for row in cur.execute("SELECT * FROM Edge WHERE Id=?;", [id]):
            edge = dict(
                zip(['id', 'workflowId', 'taskInstanceId1', 'dataIndexId1', 'taskInstanceId2', 'dataIndexId2'], row))
            cur2 = db.cursor()
            edge['dataIndex1'] = [row2[0] for row2 in cur2.execute(
                "SELECT Value FROM DataIndexValue WHERE DataIndexId=?;", [edge['dataIndexId1']])]
            edge['dataIndex2'] = [row2[0] for row2 in cur2.execute(
                "SELECT Value FROM DataIndexValue WHERE DataIndexId=?;", [edge['dataIndexId2']])]
            cur2.close()
        db.commit()
        db.close()
        return json.dumps(edge)
    elif request.method == "POST":
        edge = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        if int(id) == 0:
            cur.execute("INSERT INTO DataIndex (Id) VALUES (null);")
            edge['dataIndexId1'] = cur.lastrowid
            cur.execute("INSERT INTO DataIndex (Id) VALUES (null);")
            edge['dataIndexId2'] = cur.lastrowid
            cur.execute("INSERT INTO Edge (WorkflowId,TaskInstanceId1,DataIndexId1,TaskInstanceId2,DataIndexId2) VALUES (?,?,?,?,?);",
                        [edge['workflowId'], edge['taskInstanceId1'], edge['dataIndexId1'], edge['taskInstanceId2'], edge['dataIndexId2']])
            edge['id'] = cur.lastrowid
        else:
            cur.execute("DELETE FROM DataIndexValue WHERE DataIndexId=?;",
                        [edge['dataIndexId1']])
            cur.execute("DELETE FROM DataIndexValue WHERE DataIndexId=?;",
                        [edge['dataIndexId2']])
        for index in edge['dataIndex1']:
            cur.execute("INSERT INTO DataIndexValue (DataIndexId,Value) VALUES (?,?);", [
                        edge['dataIndexId1'], index])
        for index in edge['dataIndex2']:
            cur.execute("INSERT INTO DataIndexValue (DataIndexId,Value) VALUES (?,?);", [
                        edge['dataIndexId2'], index])
        db.commit()
        db.close()
        return json.dumps(edge)
    elif request.method == "DELETE":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        for row in cur.execute("SELECT * FROM Edge WHERE Id=?;", [id]):
            edge = dict(
                zip(['id', 'workflowId', 'taskInstanceId1', 'dataIndexId1', 'taskInstanceId2', 'dataIndexId2'], row))
            cur.execute("DELETE FROM Edge WHERE Id=?;",
                        [id])
            cur.execute("DELETE FROM DataIndex WHERE Id=? OR Id=?;", [
                        edge['dataIndexId1'], edge['dataIndexId2']])
            cur.execute("DELETE FROM DataIndexValue WHERE DataIndexId=? OR DataIndexId=?;", [
                        edge['dataIndexId1'], edge['dataIndexId2']])
        edge = {"id": id, "deleted": True}
        db.commit()
        db.close()
        return json.dumps(edge)
    return "{error:'Invalid method'}"


@app.route("/data")
def data():
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    data = []
    for row in cur.execute("SELECT * FROM Data ORDER BY Id DESC;"):
        eachdata = dict(zip(['id', 'title', 'dataTypeId', 'created'], row))
        cur2 = db.cursor()
        eachdata['values'] = [row2[0] for row2 in cur2.execute(
            "SELECT Value FROM UnitData Where DataId=?;", [eachdata['id']])]
        data.append(eachdata)
        cur2.close()
    db.commit()
    db.close()
    return json.dumps(data)


@app.route("/data/<id>", methods=["GET", "POST", "PATCH", "DELETE"])
def data_id(id):
    if request.method == "GET":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        data = None
        for row in cur.execute("SELECT * FROM Data Where Id=?;", [id]):
            eachdata = dict(zip(['id', 'title', 'dataTypeId', 'created'], row))
            cur2 = db.cursor()
            eachdata['values'] = [row2[0] for row2 in cur2.execute(
                "SELECT Value FROM UnitData Where DataId=?;", [eachdata['id']])]
            data.append(eachdata)
            cur2.close()
        db.commit()
        db.close()
        return json.dumps(data)
    elif request.method == "POST":
        data = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        _data = None
        for row in cur.execute("INSERT INTO Data (Title, DataTypeId, Created) VALUES (?,?,?) RETURNING *;",
                               [data['title'], data['dataTypeId'], time()]):
            _data = dict(zip(['id', 'title', 'dataTypeId', 'created'], row))
        for unitdata in data['values']:
            cur.execute("INSERT INTO UnitData (DataId,Value) VALUES (?,?);",
                        [data['id'], unitdata])
        _data['values'] = data['values']
        db.commit()
        db.close()
        return json.dumps(_data)
    elif request.method == "PATCH":
        data = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("UPDATE Data SET Title=?, DataTypeId=? WHERE Id=?;",
                    [data['title'], data['dataTypeId'], id])
        data['id'] = id
        db.commit()
        db.close()
        return json.dumps(data)
    elif request.method == "DELETE":
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("DELETE FROM Data WHERE Id=?;",
                    [id])
        cur.execute("DELETE FROM UnitData WHERE DataId=?;",
                    [id])
        workflow = {"id": id, "deleted": True}
        db.commit()
        db.close()
        return json.dumps(workflow)
    return "{error:'Invalid method'}"

@app.route("/workflow/<workflowId>/execute", methods=["POST"])
def workflow_execute(workflowId):
    if request.method == "POST":
        workflowExecution = request.get_json(force=True)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("INSERT INTO WorkflowExecution (WorkflowId, InputDataId, EntryTime, ExecutionState) VALUES (?,?,?,?);",
                    [workflowId, workflowExecution['dataId'], time(), STATE_LOADED])
        workflowExecution['id'] = cur.lastrowid
        db.commit()
        db.close()
        return json.dumps(workflowExecution)
    return "{error:'Invalid method'}"


setup()

if __name__ == "__main__":
    app.run()
