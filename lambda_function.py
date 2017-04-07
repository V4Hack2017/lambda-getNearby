# coding=utf8
import boto3, os

def getBotoSession(awsAccessKeyId, awsSecretAccessKey):
    return boto3.session.Session(
        aws_access_key_id=awsAccessKeyId,
        aws_secret_access_key=awsSecretAccessKey,
        region_name="eu-central-1"
    )

def calculateNearestStation(latitude, longitude):
    return "Vlhká", ["8"]

def getLineData(lineId):
    return {
        "out": ["15:15", "15:45"],
        "in": ["15:00", "15:30", "16:00"],
        "outDestination": "Starý Lískovec",
        "inDestination": "Mifkova"
    }

def getSoonestConnections(lineData, limit=1):
    return ["15:00"], ["15:15"]

def lambda_handler(event, context):
    boto = getBotoSession(os.environ['awsAccessKeyId'], os.environ['awsSecretAccessKey'])
    
    connections = {}
    
    nearestStationName, nearestStationLines = calculateNearestStation("", "")
    for lineId in nearestStationLines:
        lineData = getLineData(lineId)
        soonestConnectionsIn, soonestConnectionsOut = getSoonestConnections(lineData)
        inDestination = lineData["inDestination"]
        outDestination = lineData["outDestination"]
        connections[lineId] = {
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
