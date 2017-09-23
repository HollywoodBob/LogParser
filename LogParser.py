"""
LogParser.py
Usage: LogParser.py [options]  input_file output_file
options are:
    [-h | --help] (print docstring and exit)
    [-e | --elastic_URL] "Elasticsearch_URL" (don't need the leading http://, just the IP address:port)
    [-i | --elastic_INDEX] "Elasticsearch_Index"
    [-m | --min_duration] min_duration_in_sec (connections of time < this duration are filtered out, default = 5 sec)
    [-g | --geo_db] "path_to_geo_database_file" (default is GeoLite2-City.mmdb)
    [-f, --first_lines] lines_to_process (specifies how many of the lines in the log read from stdin should be
                                          processed, default is 1e9)
    [-t, --test_mode] (create the .sh and .json files but don't execute the .sh scripts)

log is received on stdin, typically this will be an icecast2 access.log type file or input lines.

input_file: typically this will be an icecast2 access.log type file or input lines.
output_file: contains logging type messages, useful to "tail" while this is running to check progress

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
import requests

class cmdOptions:
    ELASTICSEARCH_INDEX = "test"
    ELASTICSEARCH_URL = "104.199.119.231:9200"
    MIN_DURATION = 5
    GEO_DB = 'GeoLite2-City.mmdb'
    FIRST_LINES = 1e9
    TEST_MODE = False

class cmdArgs:
    INPUT_FILE = ""
    OUTPUT_FILE = ""


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

def processLogLine(line, json_file, cmdOptions, reader, listenerCount):
    # this function returns the time since epoch when the session ended.  When line can't be parsed, 0 is returned.

    lineIndex = "{ \"index\" : { \"_index\" : \"" + cmdOptions.ELASTICSEARCH_INDEX + "\", \"_type\" : \"logEntry\"} }\n"
    try:
        fields = getLogLineBNF().parseString(line)
    except:
        #TODO: dig deeper into this.
        print >> sys.stderr, "parse problem, line was ", line, "continuing"
        return 0


    if int(fields.numDurationTime) >= cmdOptions.MIN_DURATION and int(fields.statusCode) == 200 \
            and fields.mountpoint.rfind("GET") >= 0:
        td = fields.timestamp[0] + " " + fields.timestamp[1]  # e.g.: '03/Aug/2017:09:37:03 +0000'
        tUTC = td.replace('+0000', 'UTC')
        ts = time.strptime(tUTC, "%d/%b/%Y:%H:%M:%S %Z")  # create a time structure
        endTime = timegm(ts)  # convert time structure to an integer value
        startTime = endTime - int(fields.numDurationTime)
        # use arrow so we can round off timestamps to nearest minute for doing our listener count calculations.
        aEnd = arrow.get(td, "DD/MMM/YYYY:HH:mm:ss Z")
        aStart = aEnd.shift(seconds=0-int(fields.numDurationTime))


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
                              'timestamp': endTime, \
                              'startTime': startTime, \
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
        return endTime
    else:
        return 0

def checkForListenerCountEntry(cmdOptions, firstEndTime):
    """

    :rtype: listenerCountExists, id, entryTime, entryCount
    """
    url = 'http://' + cmdOptions.ELASTICSEARCH_URL + '/' + cmdOptions.ELASTICSEARCH_INDEX + '/_search'
    headers = {'Content-type': 'application/json'}
    data = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"startTime": firstEndTime}},
                    {"exists": {"field": "listenerCount"}}
                ]
            }
        }
    }
    resp = requests.post(url, data=json.dumps(data), headers=headers)
    assert resp.status_code == 200

    matchCount = resp.json()["hits"]["total"]

    try:
        assert (matchCount == 0 or matchCount == 1)
    except:
        print "assertion failure, matchCount = ", matchCount
        exit(-1)

    if matchCount == 1:
        return True, resp.json()["hits"]["hits"][0]["_id"], resp.json()["hits"]["hits"][0]["_source"]["startTime"], \
               resp.json()["hits"]["hits"][0]["_source"]["listenerCount"]
    else:
        return False, "0", 0, 0

def updateListenerCountEntry(cmdOptions, key, value):
    url = 'http://' + cmdOptions.ELASTICSEARCH_URL + '/' + cmdOptions.ELASTICSEARCH_INDEX + '/_update_by_query'
    headers = {'Content-type': 'application/json'}
    painlessLine= "ctx._source.listenerCount += " + str(value)
    data = {
        "script": {
            "lang": "painless",
            "inline": painlessLine
        },
        "query": {
            "bool": {
                "must": [
                    {"match": {"startTime": str(key)}},
                    {"exists": {"field": "listenerCount"}}
                ]
            }
        }
    }
    resp = requests.post(url, data=json.dumps(data), headers=headers)
    try:
        assert resp.status_code == 200
    except:
        print "assertion failed on status code of the update by query"
        print resp
        print url
        print headers
        print data
        print resp.json()
        print resp.status_code
        print resp.request
        exit(-1)


def writeListenerCount(listenerCount, cmdOptions, firstEndTime, fname):
    lineShebang = "#!/usr/bin/env bash\n"
    lineHeader = "curl -s -H \"Content-Type: application/x-ndjson\" -XPOST " + cmdOptions.ELASTICSEARCH_URL + "/_bulk?pretty \
    --data-binary \"@"+fname+".json\"\n"
    shScript = open(fname+".sh", "w")
    shScript.write(lineShebang + lineHeader)
    shScript.close()

    output_bulk_file = open(fname+".json","w")

    lcIndex = "{ \"index\" : { \"_index\" : \"" + cmdOptions.ELASTICSEARCH_INDEX + "\", \"_type\" : \"logEntry\"} }\n"
    for key, value in listenerCount.iteritems():
        # the key value in listenerCount is the time for which to add the listener count.  If this value is less than when
        # the first session in our log file finished, we need to either create an entry for this earlier time, or
        # increment an entry that already exists for that time.

        if int(key) < firstEndTime: # only do the checking for this condition because the checking adds overhead.
            listenerCountExists, id, entryTime, entryCount = checkForListenerCountEntry(cmdOptions, int(key))
            if listenerCountExists: # delete the old doc and ensure the new one's listenerCount is updated
                lcDelete = "{ \"delete\" : { \"_index\" : \"" + cmdOptions.ELASTICSEARCH_INDEX + \
                              "\", \"_type\" : \"logEntry\", \"_id\" : \"" + id + "\"} }\n"
                output_bulk_file.write(lcDelete)
                value += entryCount
            lcData = '{\"startTime\" : ' + str(key) + ', \"listenerCount\" : ' + str(value) + '}\n'
            output_bulk_file.write(lcIndex + lcData)
        else:
            lcData = '{\"startTime\" : ' + str(key) + ', \"listenerCount\" : ' + str(value) + '}\n'
            output_bulk_file.write(lcIndex + lcData)

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
            if o in ("-h", "--help") or len(args) != 2:
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

        # process arguments
        cmdArgs.INPUT_FILE = args[0]
        cmdArgs.OUTPUT_FILE = args[1]

        json_file = open("bulk_write_connects.json","w")
        reader = geoip2.database.Reader(cmdOptions.GEO_DB)

        linesToProcess = cmdOptions.FIRST_LINES

        infile = open(cmdArgs.INPUT_FILE, "r")
        outfile = open(cmdArgs.OUTPUT_FILE, "a")
        outfile.write(time.strftime("%d/%b/%YT%H:%M:%SZ", time.gmtime()) + " begin processing file "+cmdArgs.INPUT_FILE+"\n")

        lineCount = 0
        firstEndTime = 0
        for line in infile:
            lineCount += 1
            if (lineCount % 1000 == 0):
                outfile.write("File "+cmdArgs.INPUT_FILE+" about to process line "+str(lineCount)+"\n")
            retCode = processLogLine(line, json_file, cmdOptions, reader, listenerCount)
            if firstEndTime == 0:
                if retCode > 0:
                    firstEndTime = retCode
            linesToProcess-= 1
            if linesToProcess == 0:
                break

        outfile.write(time.strftime("%d/%b/%YT%H:%M:%SZ", time.gmtime()) + " done processing file "+cmdArgs.INPUT_FILE+"\n")

        json_file.close()
        lineShebang = "#!/usr/bin/env bash\n"
        lineHeader = "curl -s -H \"Content-Type: application/x-ndjson\" -XPOST " + cmdOptions.ELASTICSEARCH_URL + "/_bulk?pretty \
        --data-binary \"@bulk_write_connects.json\"\n"


        shScript = open("bulk_write_connects.sh","w")
        shScript.write(lineShebang + lineHeader)
        shScript.close()


        if (cmdOptions.TEST_MODE == False):
            subprocess.call(["chmod", "755", "./bulk_write_connects.sh"])
            outfile.write("\n"+time.strftime("%d/%b/%YT%H:%M:%SZ", time.gmtime())+ " about to execute bulk_write_connects.sh\n")
            subprocess.call("./bulk_write_connects.sh")
            outfile.write(time.strftime("%d/%b/%YT%H:%M:%SZ", time.gmtime())+ " done with bulk_write_connects.sh\n")

        writeListenerCount(listenerCount, cmdOptions, firstEndTime, "bulk_write_listener_count")

        if (cmdOptions.TEST_MODE == False):
            subprocess.call(["chmod", "755", "./bulk_write_listener_count.sh"])
            outfile.write(time.strftime("%d/%b/%YT%H:%M:%SZ", time.gmtime())+ " about to execute bulk_write_listener_count.sh\n")
            subprocess.call("./bulk_write_listener_count.sh")
            outfile.write(time.strftime("%d/%b/%YT%H:%M:%SZ", time.gmtime())+ " done with bulk_write_listener_counts.sh\n\n")

        infile.close()
        outfile.close()

    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())

