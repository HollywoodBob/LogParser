"""
LogParser.py
Usage: LogParser.py -e | --elastic_URL Elasticsearch_URL -i | --elastic_INDEX Elasticsearch_Index
"""
from pyparsing import alphas,nums, dblQuotedString, Combine, Word, Group, delimitedList, Suppress, removeQuotes
import string
import sys
import getopt
import geoip2.database
import time
import re
from datetime import datetime
from datetime import timedelta
from socket import gethostname
from calendar import timegm
from datetime import datetime
import arrow
import subprocess

import json

# cLinesToProcess = 1000000000000
# cOutputRecords = 0
# cBulkRecords = 0
# minDuration = 5
# POST_LIMIT = 100
# err_file = "error_log.txt"
# errcnt = 0
# input_file = open('test.log', 'r')
# outputFileName = "put_recs_bulk.sh"

ELASTICSEARCH_INDEX = "test"
ELASTICSEARCH_URL = "104.199.119.231:9200"
MIN_DURATION = 5
GEO_DB = 'GeoLite2-City.mmdb'


def getCmdFields(t):
#currentLine = 0
#  parse error, t is  ['"GET /high.m3u"']
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

def writeListenerCount(listenerCount):
    lcHeader = "curl -XPOST $ES/_bulk?pretty -H 'Content-Type: application/x-ndjson' -d'"
    cBulkRecords = 0
    output_bulk_file.write(lcHeader)
    lcIndex = "{ \"index\" : { \"_index\" : \"" + ELASTIC_INDEX + "\", \"_type\" : \"logEntry\"} }\n"
    for key, value in listenerCount.iteritems():
        cBulkRecords += 1
        lcData = '{\"timestamp\" : ' + str(key) + ', \"listenerCount\" : ' + str(value) + '}'
        if cBulkRecords % POST_LIMIT:
            output_bulk_file.write(lcIndex + lcData + '\n')
        else:
            output_bulk_file.write("\n\'\n")
            output_bulk_file.write(lcHeader)
            output_bulk_file.write(lcIndex + lcData + '\n')

    output_bulk_file.write("\n\'\n")

logLineBNF = None
listenerCount = {}

#output_file = open('put_recs.sh', 'w')
#output_bulk_file = open(outputFileName,'w')
#lineShebang = "#!/usr/bin/env bash\n"
#
#output_bulk_file.write(lineShebang)
#
#lineHeader =  "curl -XPOST $ES/_bulk?pretty -H 'Content-Type: application/x-ndjson' -d'"
#output_bulk_file.write(lineHeader)
#
##lineIndex = "{ \"index\" : { \"_index\" : \"sessions\", \"_type\" : \"logEntry\"} }\n"
#lineIndex = "{ \"index\" : { \"_index\" : \"" + ELASTIC_INDEX + "\", \"_type\" : \"logEntry\"} }\n"

reader = geoip2.database.Reader(GEO_DB)

#ef = open(err_file, 'w')
#try:
#    cLines = 1
#    for line in input_file:
#        currentLine += 1 # for error handling
#        if cLinesToProcess % 100 == 0:
#            print "processing line ", cLines
#            cLines += 100
#        cLinesToProcess -= 1
#        try:
#            fields = getLogLineBNF().parseString(line)
#        except:
#            next
#        if int(fields.numDurationTime) >= minDuration and int(fields.statusCode) == 200:
#            td = fields.timestamp[0] + " " + fields.timestamp[1] # e.g.: '03/Aug/2017:09:37:03 +0000'
#            tUTC  = td.replace('+0000','UTC')
#            ts = time.strptime(tUTC, "%d/%b/%Y:%H:%M:%S %Z" ) # create a time structure
#            startTime = timegm(ts) # convert time structure to an integer value
#            endTime = startTime + int(fields.numDurationTime)
#            # use arrow so we can round off timestamps to nearest minute for doing our listener count calculations.
#            aStart = arrow.get(td, "DD/MMM/YYYY:HH:mm:ss Z")
#            aEnd = aStart.shift(seconds=int(fields.numDurationTime))
#
#            for t in range(aStart.floor('minute').timestamp, aEnd.floor('minute').timestamp, 60):
#                if listenerCount.has_key(t) :
#                    listenerCount[t] += 1
#                else:
#                    listenerCount[t] = 1
#
#            response = reader.city(fields.ipAddr)
#            lat = response.location.latitude
#            lon = response.location.longitude
#            country_iso = response.country.iso_code
#            state_iso = response.subdivisions.most_specific.iso_code
#            city = response.city.name
#            zip_code = response.postal.code
#            try:
#                doc = json.dumps({'ipAddr': fields.ipAddr, \
#                              'auth': fields.auth, \
#                              'timestamp': startTime, \
#                              'endTime': endTime, \
#                              'cmd': fields.mountpoint, \
#                              'statusCode': fields.statusCode, \
#                              'numBytesSent': fields.numBytesSent, \
#                              'referer': fields.referer, \
#                              'userAgent': fields.userAgent, \
#                              'connectTimeInMinutes': str(int(fields.numDurationTime)/60), \
#                              'location': str(lat) + "," + str(lon), \
#                              'country_iso': country_iso, \
#                              'state_iso' : state_iso, \
#                              'city' : city, \
#                              'zip_code' : zip_code, \
#                              'numDurationTime': fields.numDurationTime}, \
#                              separators=(',',':')
#                            )
#            except:
#
#                ef.write("error encountered on json.dumps\n")
#                ef.write("found in line " + str(currentLine) + " of the input file\n")
#                ef.write("line from file is " + line + "\n")
#                ef.write("parsed fields are "+ str(fields) + "\n")
#                ef.write("***************************************\n\n")
#                errcnt += 1
#                next
#
#            cBulkRecords += 1
#            if cBulkRecords % POST_LIMIT:
#                output_bulk_file.write(lineIndex+doc+"\n")
#            else:
#                output_bulk_file.write("\n\'\n")
#                output_bulk_file.write(lineHeader)
#                output_bulk_file.write(lineIndex+doc+"\n")
#
#        if cLinesToProcess <= 0:
#            break
#finally:
#    input_file.close()
#    output_bulk_file.write("\n\'\n")
#    writeListenerCount(listenerCount)
#    output_bulk_file.close()
#    ef.write("TOTAL ERROR COUNT = " + str(errcnt) + "\n")
#    ef.close()
#
class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def processLogLine(line, json_file):
    lineIndex = "{ \"index\" : { \"_index\" : \"" + ELASTICSEARCH_INDEX + "\", \"_type\" : \"logEntry\"} }\n"

    fields = getLogLineBNF().parseString(line)


    if int(fields.numDurationTime) >= MIN_DURATION and int(fields.statusCode) == 200:
        td = fields.timestamp[0] + " " + fields.timestamp[1]  # e.g.: '03/Aug/2017:09:37:03 +0000'
        tUTC = td.replace('+0000', 'UTC')
        ts = time.strptime(tUTC, "%d/%b/%Y:%H:%M:%S %Z")  # create a time structure
        startTime = timegm(ts)  # convert time structure to an integer value
        endTime = startTime + int(fields.numDurationTime)
        # use arrow so we can round off timestamps to nearest minute for doing our listener count calculations.
        aStart = arrow.get(td, "DD/MMM/YYYY:HH:mm:ss Z")
        aEnd = aStart.shift(seconds=int(fields.numDurationTime))

        for t in range(aStart.floor('minute').timestamp, aEnd.floor('minute').timestamp, 60):
            if listenerCount.has_key(t):
                listenerCount[t] += 1
            else:
                listenerCount[t] = 1

        response = reader.city(fields.ipAddr)
        lat = response.location.latitude
        lon = response.location.longitude
        country_iso = response.country.iso_code
        state_iso = response.subdivisions.most_specific.iso_code
        city = response.city.name
        zip_code = response.postal.code
        try:
            doc = json.dumps({'ipAddr': fields.ipAddr, \
                              'auth': fields.auth, \
                              'timestamp': startTime, \
                              'endTime': endTime, \
                              'cmd': fields.mountpoint, \
                              'statusCode': fields.statusCode, \
                              'numBytesSent': fields.numBytesSent, \
                              'referer': fields.referer, \
                              'userAgent': fields.userAgent, \
                              'connectTimeInMinutes': str(int(fields.numDurationTime) / 60), \
                              'location': str(lat) + "," + str(lon), \
                              'country_iso': country_iso, \
                              'state_iso': state_iso, \
                              'city': city, \
                              'zip_code': zip_code, \
                              'numDurationTime': fields.numDurationTime}, \
                             separators=(',', ':')
                             )
        except:

             print >>sys.stderr, "error encountered on json.dumps\n"
             print >>sys.stderr, "found in line " + str(currentLine) + " of the input file\n"
             print >>sys.stderr, "line from file is " + line + "\n"
             print >>sys.stderr, "parsed fields are " + str(fields) + "\n"
             print >>sys.stderr, "***************************************\n\n"
             return -2

        json_file.write((lineIndex+doc+"\n"))
    return 0


def main(argv=None):
    global ELASTICSEARCH_URL
    global ELASSTICSEARCH_INDEX
    global MIN_DURATION
    global GEO_DB
    global FIRST_LINES

    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(sys.argv[1:], "he:i:m:g:f:", ["help", "elastic_url=", "elastic_index=", \
                                                                 "min_duration=", "geo_db=", "first_lines="])
        except getopt.error, msg:
             raise Usage(msg)
        # process options
        for o, a in opts:
            if o in ("-h", "--help"):
                print __doc__
                return(0)
            if o in ("-e", "--elastic_url"):
                ELASTICSEARCH_URL = a
            if o in ("-i", "--elastic_index"):
                ELASTICSEARCH_INDEX = a
            if o in ("-m", "--min_duration"):
                MIN_DURATION = int(a)
            if o in ("-g", "--geo_db"):
                GEO_DB = a
            if o in ("-f", "--FIRST_LINES"):
                FIRST_LINES = int(a)


        json_file = open("elastic_bulk_write.json","w")
        for line in sys.stdin:
            retCode = processLogLine(line, json_file)
            if retCode:
                return retCode
            try: # do a try/except because FIRST_LINES only exists when explicitly used as command option.
                FIRST_LINES -= 1
                if FIRST_LINES == 0:
                    break
            except:
                next

        json_file.close()
        lineShebang = "#!/usr/bin/env bash\n"
        lineHeader = "curl -s -H \"Content-Type: application/x-ndjson\" -XPOST " + ELASTICSEARCH_URL + "/_bulk?pretty \
        --data-binary \"@elastic_bulk_write.json\"\n"


        shScript = open("bulk_write.sh","w")
        shScript.write(lineShebang + lineHeader)
        shScript.close()

        subprocess.call(["chmod", "755", "./bulk_write.sh"])
        subprocess.call("./bulk_write.sh")


    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())
