from pyparsing import alphas,nums, dblQuotedString, Combine, Word, Group, delimitedList, Suppress, removeQuotes
import string
import time
import sys
from datetime import datetime
from datetime import timedelta


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

#NOTE WELL: IF WE ADD A MOUNTPOINT, NEED TO ADD AN ENTRY HERE!
def findMountpoint(mount):
    if mount.find("high") != -1:
        return "high"
    if mount.find("mobile") != -1:
        return "mobile"
    if mount.find("boo2") != -1:
        return "boo2"
    return "-"


logLineBNF = None
MIN_DURATION = 5

for line in sys.stdin:
    try:
        fields = getLogLineBNF().parseString(line)
    except:
        next

    if int(fields.numDurationTime) >= MIN_DURATION and int(fields.statusCode) == 200:
        td = fields.timestamp[0] + " " + fields.timestamp[1] # e.g.: '03/Aug/2017:09:37:03 +0000'
        td  = td.replace('+0000','UTC')
        ts = time.strptime(td, "%d/%b/%Y:%H:%M:%S %Z" ) # create a time structure
        myDate = time.strftime("%Y-%m-%d", ts)
        myTime = time.strftime("%I:%M:%S %p", ts)
        outline = fields.ipAddr + "\t" + myDate + "\t" + myTime + "\t" + findMountpoint(fields.mountpoint) + "\t"+ fields.cmd + "\t" + \
                  fields.numDurationTime + "\t" + fields.statusCode + "\t" + fields.userAgent
        print outline

