# coding=utf8
import boto3, os, math, json
from datetime import datetime 



def distance(lat1, lon1, lat2, lon2):
    radius = 6371 # km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d


def getBotoSession(awsAccessKeyId, awsSecretAccessKey):
    return boto3.session.Session(
        aws_access_key_id=awsAccessKeyId,
        aws_secret_access_key=awsSecretAccessKey,
        region_name="eu-central-1"
    )


def lambda_handler(event, context):
    boto = getBotoSession(os.environ["awsAccessKeyId"], os.environ["awsSecretAccessKey"])
    dynamodb = boto.resource("dynamodb")
    dynamodbClient = boto.client("dynamodb")
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
        stations = dynamodbClient.scan(
            TableName='hackathon-stations',
            AttributesToGet=[
                'name', 'latitude', 'lines', 'longitude'
            ]
        )["Items"]
        bestStation = ''
        bestLines = []
        bestDistance = False
        for station in stations:
            stationLatitude = float(station["latitude"]["S"])
            stationLongitude = float(station["longitude"]["S"])
            dist = distance(stationLatitude, stationLongitude, latitude, longitude)
            if(bestDistance == False or bestDistance > dist): 
                bestDistance = dist
                bestStation = station["name"]["S"]
                bestLines = station["lines"]["SS"]
        return bestStation, bestLines


    def getSoonestConnections(lineData, timestamp, limit=2):
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
    timestamp = int(event["queryStringParameters"]["timestamp"])
    clientLat = float(event["queryStringParameters"]["lat"])
    clientLng = float(event["queryStringParameters"]["lng"])
    
    nearestStationName, nearestStationLines = calculateNearestStation(clientLat, clientLng)
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
        "body":  json.dumps({
            "station": nearestStationName,
            "lines": connections
        }, ensure_ascii=False)
    }
