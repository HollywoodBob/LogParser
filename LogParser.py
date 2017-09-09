from pyparsing import alphas,nums, dblQuotedString, Combine, Word, Group, delimitedList, Suppress, removeQuotes
import string
import glob
import sys
import geoip2.database
import time
import re
from datetime import datetime
from datetime import timedelta
from socket import gethostname
from calendar import timegm
from datetime import datetime
import arrow

import json

cLinesToPro     cess = 1000000000000
cOutputRecords = 0
cBulkRecords = 0
minDuration = 5
POST_LIMIT = 100
geoDB = 'GeoLite2-City.mmdb'
currentLine = 0
err_file = "error_log.txt"
errcnt = 0
input_file = open('test.log', 'r')
ELASTIC_INDEX = "test"

def getLocFromIP(IPAddr):
    loc = {}


def getCmdFields(t):
    # TODO : this assumes 3 fields.  found a Bing Search Bot in Redmond that doesn't send protocol
    # parse error, t is  ['"GET /high.m3u"']
    try:
        t["method"],t["requestURI"],t["protocolVersion"] = t[0].strip('"').split()
    except: # found a Bing Search Bot in Redmond that doesn't send protocol field. only "GET /high.m3u"
        t["method"], t["requestURI"] = t[0].strip('"').split()
        t["protocolVersion"] = "-"


def getLogLineBNF():
    global logLineBNF

    if logLineBNF is None:
        integer = Word( nums )
        ipAddress = delimitedList( integer, ".", combine=True )

        timeZoneOffset = Word("+-",nums)
        month = Word(string.uppercase, string.lowercase, exact=3)
        serverDateTime = Group( Suppress("[") + Combine( integer + "/" + month + "/" + integer + ":" + integer + ":" \
                                                         + integer + ":" + integer ) + timeZoneOffset + Suppress("]") )

        logLineBNF = ( ipAddress.setResultsName("ipAddr") +
                       Suppress("-") +
                       ("-" | Word( alphas+nums+"@._" )).setResultsName("auth") +
                       serverDateTime.setResultsName("timestamp") +
                       dblQuotedString.setResultsName("mountpoint").setParseAction(getCmdFields) +
                       (integer | "-").setResultsName("statusCode") +
                       (integer | "-").setResultsName("numBytesSent")  +
                       dblQuotedString.setResultsName("referer").setParseAction(removeQuotes) +
                       dblQuotedString.setResultsName("userAgent").setParseAction(removeQuotes) +
		       (integer | "-").setResultsName("numDurationTime"))
    return logLineBNF

def writeListenerCountFile(listenerCount):
    cBulkRecords = 0
    lcHeader = "curl -XPOST $ES/_bulk?pretty -H 'Content-Type: application/x-ndjson' -d'"
#   lcIndex = "{ \"index\" : { \"_index\" : \"sessions\", \"_type\" : \"listenerCountEntry\"} }\n"
    lcIndex = "{ \"index\" : { \"_index\" : \"" + ELASTIC_INDEX + "\", \"_type\" : \"listenerCountEntry\"} }\n"
    lcFile = open('put_recs_listenerCount.sh', 'w')
    lcFile.write(lineShebang)
    lcFile.write(lcHeader)
    for key, value in listenerCount.iteritems():
        cBulkRecords += 1
        lcData = '{\"lcTimestamp\" : ' + str(key) + ', \"listenerCount\" : ' + str(value) + '}'
        if cBulkRecords % POST_LIMIT:
            lcFile.write(lcIndex + lcData + '\n')
        else:
            lcFile.write("\n\'\n")
            lcFile.write(lcHeader)
            lcFile.write(lcIndex + lcData + '\n')

    lcFile.write("\n\'\n")
    lcFile.close()

logLineBNF = None
listenerCount = {}

#output_file = open('put_recs.sh', 'w')
output_bulk_file = open('put_recs_bulk.sh','w')
lineShebang = "#!/usr/bin/env bash\n"

output_bulk_file.write(lineShebang)

lineHeader =  "curl -XPOST $ES/_bulk?pretty -H 'Content-Type: application/x-ndjson' -d'"
output_bulk_file.write(lineHeader)

#lineIndex = "{ \"index\" : { \"_index\" : \"sessions\", \"_type\" : \"logEntry\"} }\n"
lineIndex = "{ \"index\" : { \"_index\" : \"" + ELASTIC_INDEX + "\", \"_type\" : \"logEntry\"} }\n"

reader = geoip2.database.Reader(geoDB)

ef = open(err_file, 'w')
try:
    cLines = 1
    for line in input_file:
        currentLine += 1 # for error handling
        if cLinesToProcess % 100 == 0:
            print "processing line ", cLines
            cLines += 100
        cLinesToProcess -= 1
        try:
            fields = getLogLineBNF().parseString(line)
        except:
            next
        if int(fields.numDurationTime) >= minDuration and int(fields.statusCode) == 200:
            td = fields.timestamp[0] + " " + fields.timestamp[1] # e.g.: '03/Aug/2017:09:37:03 +0000'
            tUTC  = td.replace('+0000','UTC')
            ts = time.strptime(tUTC, "%d/%b/%Y:%H:%M:%S %Z" ) # create a time structure
            startTime = timegm(ts) # convert time structure to an integer value
            endTime = startTime + int(fields.numDurationTime)
            # use arrow so we can round off timestamps to nearest minute for doing our listener count calculations.
            aStart = arrow.get(td, "DD/MMM/YYYY:HH:mm:ss Z")
            aEnd = aStart.shift(seconds=int(fields.numDurationTime))

            for t in range(aStart.floor('minute').timestamp, aEnd.floor('minute').timestamp, 60):
                if listenerCount.has_key(t) :
                    listenerCount[t] += 1
                else:
                    listenerCount[t] = 1

            response = reader.city(fields.ipAddr)
            lat = response.location.latitude
            lon = response.location.longitude
            country_iso = response.country.iso_code
            try:
                doc = json.dumps({'ipAddr': fields.ipAddr, \
                              'auth': fields.auth, \
                              'timestamp': fields.timestamp[0] + " " + fields.timestamp[1], \
                              'startTime': startTime, \
                              'endTime': endTime, \
                              'cmd': fields.mountpoint, \
                              'statusCode': fields.statusCode, \
                              'numBytesSent': fields.numBytesSent, \
                              'referer': fields.referer, \
                              'userAgent': fields.userAgent, \
                              'connectTimeInMinutes': str(int(fields.numDurationTime)/60), \
                              'location': str(lat) + "," + str(lon), \
                              'country_iso': country_iso, \
                              'numDurationTime': fields.numDurationTime}, \
                              separators=(',',':')
                            )
            except:

                ef.write("error encountered on json.dumps\n")
                ef.write("found in line " + str(currentLine) + " of the input file\n")
                ef.write("line from file is " + line + "\n")
                ef.write("parsed fields are "+ str(fields) + "\n")
                ef.write("***************************************\n\n")
                errcnt += 1
                next
                #exit(-1)

#            outline = "curl -XPOST $ES/sessions/logEntry/?pretty -H 'Content-Type: application/json' -d'" + doc + "'" + "\n"
#            print outline
#            output_file.write(outline)

            cBulkRecords += 1
            if cBulkRecords % POST_LIMIT:
                output_bulk_file.write(lineIndex+doc+"\n")
            else:
                output_bulk_file.write("\n\'\n")
                output_bulk_file.write(lineHeader)
                output_bulk_file.write(lineIndex+doc+"\n")

        if cLinesToProcess <= 0:
            break
finally:
    input_file.close()
#    output_file.close()
    output_bulk_file.write("\n\'\n")
    output_bulk_file.close()
    ef.write("TOTAL ERROR COUNT = " + str(errcnt) + "\n")
    ef.close()
    writeListenerCountFile(listenerCount)

