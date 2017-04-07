# coding=utf8
import boto3, os
from datetime import datetime


def getBotoSession(awsAccessKeyId, awsSecretAccessKey):
    return boto3.session.Session(
        aws_access_key_id=awsAccessKeyId,
        aws_secret_access_key=awsSecretAccessKey,
        region_name="eu-central-1"
    )


def lambda_handler(event, context):
    boto = getBotoSession(os.environ["awsAccessKeyId"], os.environ["awsSecretAccessKey"])
    dynamodb = boto.resource("dynamodb")
    linesTable = dynamodb.Table("hackathon-lines")

    def getLineData(lineId):
        response = linesTable.get_item(
            Key={
                "id": lineId,
            },
            AttributesToGet=["in", "out", "inDestination", "outDestination", "type"]
        )["Item"]
        return response

    def calculateNearestStation(latitude, longitude):
        return "Vlhká", ["8"]


    def getSoonestConnections(lineData, timestamp, limit=1):
        clientDateTime = datetime.fromtimestamp(timestamp)
        clientTimeVal = int(clientDateTime.strftime("%H")) * 60 + int(clientDateTime.strftime("%M"))
        
        def extractTimes(clientTimeVal, lineData):
            lineTimesList = {}
            for lineTime in lineData:
                lineTimeParts = lineTime.split(":")
                lineTimeVal = int(lineTimeParts[0]) * 60 + int(lineTimeParts[1])
                if (lineTimeVal > clientTimeVal):
                    lineTimesList[lineTimeVal] = lineTime
            return lineTimesList
        lineInTimesList = extractTimes(clientTimeVal, lineData["in"])
        if (len(lineInTimesList.keys()) < limit):
            addition = extractTimes(-1, lineData["in"])
            tmp = {}
            for time in addition:
                tmp[time + 24*60] = addition[time]
            lineInTimesList.update(tmp)
            
        lineInResult = [lineInTimesList[x] for x in sorted(lineInTimesList)][0:limit]
        
        lineOutTimesList = extractTimes(clientTimeVal, lineData["out"])
        if (len(lineOutTimesList.keys()) < limit):
            addition = extractTimes(-1, lineData["out"])
            tmp = {}
            for time in addition:
                tmp[time + 24*60] = addition[time]
            lineOutTimesList.update(tmp)
        lineOutResult = [lineOutTimesList[x] for x in sorted(lineOutTimesList)][0:limit]

        return lineInResult, lineOutResult
    
    connections = {}
    timestamp = 1491578160
    
    nearestStationName, nearestStationLines = calculateNearestStation("", "")
    for lineId in nearestStationLines:
        lineData = getLineData(lineId)
        soonestConnectionsIn, soonestConnectionsOut = getSoonestConnections(lineData, timestamp)
        inDestination = lineData["inDestination"]
        outDestination = lineData["outDestination"]
        vehicleType = lineData["type"]
        connections[lineId] = {
            "type": vehicleType,
            "in": {
                "destination": inDestination,
                "connections": soonestConnectionsIn
            },
            "out": {
                "destination": outDestination,
                "connections": soonestConnectionsOut
            }
        }
    return {
        "station": nearestStationName,
        "lines": connections
    }
