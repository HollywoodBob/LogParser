"""
LogParser.py
Usage: LogParser.py [options] < log
options are:
    [-h | --help] (print docstring and exit)
    [-e | --elastic_URL] "Elasticsearch_URL"
    [-i | --elastic_INDEX] "Elasticsearch_Index"
    [-m | --min_duration] min_duration_in_sec (connections of time < this duration are filtered out, default = 5 sec)
    [-g | --geo_db] "path_to_geo_database_file" (default is GeoLite2-City.mmdb)
    [-f, --first_lines] lines_to_process (specifies how many of the lines in the log read from stdin should be
                                          processed, default is 1e9)
    [-t, --test_mode] (create the .sh and .json files but don't execute the .sh scripts)

log is received on stdin, typically this will be an icecast2 access.log type file or input lines.

this script will create .sh scripts and accompanying JSON files:
    bulk_write_connects - for inputting the connection documents into elasticsearch.
    bulk_write_listener_count - for inputting listener count into elasticsearch.

It then executes the .sh scripts unless the -t or --test_mode options were set on the command line.
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


class cmdOptions:
    ELASTICSEARCH_INDEX = "test"
    ELASTICSEARCH_URL = "104.199.119.231:9200"
    MIN_DURATION = 5
    GEO_DB = 'GeoLite2-City.mmdb'
    FIRST_LINES = 1e9
    TEST_MODE = False

def getCmdFields(t):
    try:
        t["method"],t["requestURI"],t["protocolVersion"] = t[0].strip('"').split()
    except: # found a Bing Search Bot in Redmond that doesn't send protocol field. only "GET /high.m3u"
        t["method"], t["requestURI"] = t[0].strip('"').split()
        t["protocolVersion"] = "-"

logLineBNF = None
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



class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def myfunc():
  if not hasattr(myfunc, "counter"):
     myfunc.counter = 0  # it doesn't exist yet, so initialize it
  myfunc.counter += 1

def processLogLine(line, json_file, cmdOptions, reader, listenerCount):

    lineIndex = "{ \"index\" : { \"_index\" : \"" + cmdOptions.ELASTICSEARCH_INDEX + "\", \"_type\" : \"logEntry\"} }\n"
    try:
        fields = getLogLineBNF().parseString(line)
    except:
        #TODO: dig deeper into this.
        print >> sys.stderr, "parse problem, line was ", line, "continuing"
        return


    if int(fields.numDurationTime) >= cmdOptions.MIN_DURATION and int(fields.statusCode) == 200:
        td = fields.timestamp[0] + " " + fields.timestamp[1]  # e.g.: '03/Aug/2017:09:37:03 +0000'
        tUTC = td.replace('+0000', 'UTC')
        ts = time.strptime(tUTC, "%d/%b/%Y:%H:%M:%S %Z")  # create a time structure
        startTime = timegm(ts)  # convert time structure to an integer value
        endTime = startTime + int(fields.numDurationTime)
        # use arrow so we can round off timestamps to nearest minute for doing our listener count calculations.
        aStart = arrow.get(td, "DD/MMM/YYYY:HH:mm:ss Z")
        aEnd = aStart.shift(seconds=int(fields.numDurationTime))


        for t in range(aStart.floor('minute').timestamp, aEnd.floor('minute').timestamp, 60):
            if (t > 1505128320):
                print "t is too large!"

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
             # print >>sys.stderr, "found in line " + str(currentLine) + " of the input file\n"
             print >>sys.stderr, "line from file is " + line + "\n"
             print >>sys.stderr, "parsed fields are " + str(fields) + "\n"
             print >>sys.stderr, "***************************************\n\n"
             print >>sys.stderr, "Continuing\n"
             return

        json_file.write((lineIndex+doc+"\n"))
    return 0

#TODO integrate the following function
def writeListenerCount(listenerCount, cmdOptions, fname):
    lineShebang = "#!/usr/bin/env bash\n"
    lineHeader = "curl -s -H \"Content-Type: application/x-ndjson\" -XPOST " + cmdOptions.ELASTICSEARCH_URL + "/_bulk?pretty \
    --data-binary \"@"+fname+".json\"\n"
    shScript = open(fname+".sh", "w")
    shScript.write(lineShebang + lineHeader)
    shScript.close()

    output_bulk_file = open(fname+".json","w")

    lcIndex = "{ \"index\" : { \"_index\" : \"" + cmdOptions.ELASTICSEARCH_INDEX + "\", \"_type\" : \"logEntry\"} }\n"
    linesToProcess = cmdOptions.FIRST_LINES
    for key, value in listenerCount.iteritems():
        lcData = '{\"timestamp\" : ' + str(key) + ', \"listenerCount\" : ' + str(value) + '}\n'
        output_bulk_file.write(lcIndex + lcData)
        linesToProcess -= 1
        if linesToProcess == 0:
            break
    output_bulk_file.close()

""" Following is a test harness function
def writeListenerCountTest():
    cmdOptions.ELASTICSEARCH_URL="myURL"
    cmdOptions.ELASTICSEARCH_INDEX="myIndex"
    listenerCount={}
    listenerCount["1000"] = 5
    listenerCount["2000"] = 10
    writeListenerCount(listenerCount, cmdOptions, "myTestFile")
"""

def main(argv=None):
    listenerCount = {}

    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(sys.argv[1:], "he:i:m:g:f:t", ["help", "elastic_url=", "elastic_index=", \
                                                                 "min_duration=", "geo_db=", "first_lines=", "test_mode"])
        except getopt.error, msg:
             raise Usage(msg)
        # process options
        for o, a in opts:
            if o in ("-h", "--help"):
                print __doc__
                return(0)
            if o in ("-e", "--elastic_url"):
                cmdOptions.ELASTICSEARCH_URL = a
            if o in ("-i", "--elastic_index"):
                cmdOptions.ELASTICSEARCH_INDEX = a
            if o in ("-m", "--min_duration"):
                cmdOptions.MIN_DURATION = int(a)
            if o in ("-g", "--geo_db"):
                cmdOptions.GEO_DB = a
            if o in ("-f", "--first_lines"):
                cmdOptions.FIRST_LINES = int(a)
            if o in ("-t", "--test_mode"):
                cmdOptions.TEST_MODE = True


        json_file = open("bulk_write_connects.json","w")
        reader = geoip2.database.Reader(cmdOptions.GEO_DB)

        linesToProcess = cmdOptions.FIRST_LINES

        infile = open("logs/access.log.20170907_124104", "r")
        for line in infile:
        # for line in sys.stdin:
            retCode = processLogLine(line, json_file, cmdOptions, reader, listenerCount)
            if retCode:
                return retCode
            linesToProcess-= 1
            if linesToProcess == 0:
                break

        json_file.close()
        lineShebang = "#!/usr/bin/env bash\n"
        lineHeader = "curl -s -H \"Content-Type: application/x-ndjson\" -XPOST " + cmdOptions.ELASTICSEARCH_URL + "/_bulk?pretty \
        --data-binary \"@bulk_write_connects.json\"\n"


        shScript = open("bulk_write_connects.sh","w")
        shScript.write(lineShebang + lineHeader)
        shScript.close()

        if (cmdOptions.TEST_MODE == False):
            subprocess.call(["chmod", "755", "./bulk_write_connects.sh"])
            subprocess.call("./bulk_write_connects.sh")

        writeListenerCount(listenerCount, cmdOptions, "bulk_write_listener_count")

        if (cmdOptions.TEST_MODE == False):
            subprocess.call(["chmod", "755", "./bulk_write_listener_count.sh"])
            subprocess.call("./bulk_write_listener_count.sh")


    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())

