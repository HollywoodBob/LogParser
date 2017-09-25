"""
AddProgramInfo.py
Usage: AddProgramInfo.py -e | --elastic_url Elasticsearch_URL -i | --elastic_index Elasticsearch_Index
This module reads program information lines from stdin and does an update by query into Elasticsearch to add the program
info to docs that have timestamps corresponding to the program.
Program information line is comma separated:
ProgramName, Artist, StartTime, EndTime
and a '#' in column 1 indicates a comment
StartTime and EndTime format is %Y-%m-%dT%H:%M without a timezone specified.  This script assumes Pacific timezone.
example program information line:
Blues Festival, Chris Isaak, 2017-06-30T21:00, 2017-06-30T22:15

Example invocation:
python AddProgramInfo.py -e 104.199.119.231:9200 -i test < ProgramSchedule.csv

Each line from stdin is echo'ed to stdout before it is processed.
This script creates the elastic_ubq.json and the update_by_query.sh file for each processed line, and each line
overwrites these files with the next rendition of content.  When the script exits, these files are NOT cleaned up, so
one can look at them for debug purposes.

This script enables one to add program information to the elasticsearch index after the icecast logs have been scanned
and input to the index.
"""

import datetime
import arrow
import subprocess
import sys
import getopt

#TODO: remove these defaults.
ELASTICSEARCH_URL = "104.199.119.231:9200"
ELASTICSEARCH_INDEX = "index_2017"

def process(arg):
    return


def formatTimeAsPacific(sTime):
    datetime_dt = datetime.datetime.strptime(sTime, "%Y-%m-%dT%H:%M")
    assert isinstance(datetime_dt, object)
    arrow_dt = arrow.get(datetime_dt, "US/Pacific")
    assert isinstance(arrow_dt, object)
    return arrow_dt.isoformat()


def parseInput(line):
    print line
    fields = line.split(",")
    programName = fields[0].strip()
    artist = fields[1].strip()
    startTime = fields[2].strip()
    endTime = fields[3].strip()
    return programName, artist, formatTimeAsPacific(startTime), formatTimeAsPacific(endTime)

def updateIndexWithProgram(elasticURL, index, programName, artist, startTime, endTime):
    json_file = open("elastic_ubq.json", "w")
    json_file.write("{\n")
    json_file.write("  \"script\": {\n")
    json_file.write("     \"lang\": \"painless\",\n")
    json_file.write("     \"inline\": \"ctx._source.artist = params.artist; ctx._source.programName = params.programName\",\n")
    json_file.write("     \"params\": {\n")
    line = "       \"artist\": \""+artist+"\",\n"
    # line = "       \"artist\": \"\",\n"
    json_file.write(line)
    line = "       \"programName\": \""+programName+"\"\n"
    # line = "       \"programName\": \"\"\n"
    json_file.write(line)
    json_file.write("     }\n")
    json_file.write("  },\n")
    #json_file.write("}\n"),
    json_file.write("   \"query\": {\n")
    json_file.write("     \"range\": {\n")
    json_file.write("       \"startTime\": {\n")
    json_file.write("         \"gte\": \""+startTime+"\",\n")
    json_file.write("         \"lt\": \""+endTime+"\",\n")
    json_file.write("         \"format\": \"date_optional_time\"\n")
    json_file.write("       }\n")
    json_file.write("     }\n")
    json_file.write("   }\n")
    json_file.write("}\n")
    json_file.write("\n")
    json_file.close()

    lineShebang = "#!/usr/bin/env bash\n"
    lineHeader = "curl -XPOST " + elasticURL + "/" + index + "/_update_by_query?pretty   -d@elastic_ubq.json"

    shScript = open("update_by_query.sh","w")
    shScript.write(lineShebang + lineHeader)
    shScript.close()

    subprocess.call(["chmod", "755", "./update_by_query.sh"])
    subprocess.call("./update_by_query.sh")


def main():

    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "he:i:", ["help", "elastic_url=", "elastic_index="])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        return(2)

    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print __doc__
            return(0)
        if o in ("-e", "--elastic_url"):
            ELASTICSEARCH_URL = a
        if o in ("-i", "--elastic_index"):
            ELASTICSEARCH_INDEX = a

    # process arguments
    for arg in args:
        process(arg) # process() is defined elsewhere

    for line in sys.stdin:
            if line[0] != '#':
            programName, artist, startTime, endTime = parseInput(line)
            updateIndexWithProgram(ELASTICSEARCH_URL, ELASTICSEARCH_INDEX, programName, artist, startTime, endTime)

    return 0

if __name__ == "__main__":
    sys.exit(main())

