#!/usr/bin/env python
""" Invocation: python Process-Prog-Sched.py [options] input-csv-file  output-flat-csv-file
"""

import datetime, time, sys, getopt

def convertToDate(y, m, weekday, ordinal):
    """ convert input args to a datetime.date object

    input args:
            year (an integer)
            month (an integer starting with 1 for January
            weekday (a day of week starting with 0 for monday)
            ordinal (the nth weekday of the month)
        returns:
            date exists (bool that tells whether desired date exists in that month - needed for the 5th day of a month)
            datetime - a datetime.datetime object
        """
    d = datetime.datetime(y,m,1)
    first_day_of_month = d.weekday()
    add_factor = weekday-first_day_of_month
    d_calc = d + datetime.timedelta(add_factor)
    if d_calc < d:
        d_calc += datetime.timedelta(7)
    d_calc += datetime.timedelta(7 * (int(ordinal) - 1))
    if d_calc.month == d.month:
        return True, d_calc
    else:
        return False, d_calc

class cGuideInput:
    line=""
    programName=""
    programHost=""
    programGuest=""
    dayofWeek=""
    startTime=""
    ordinals=""

class cProgramLine:
    programName = ""
    programHost = ""
    programGuest = ""
    startTime = datetime.datetime
    endTime = datetime.datetime
    lineDict={}

dayDict = {"Monday":0, "Tuesday":1, "Wednesday":2, "Thursday":3, "Friday":4,"Saturday":5, "Sunday":6}
def parseProgramGuideInput(line, year, month, outDict):
    gI = cGuideInput()
    pL = cProgramLine()

    gI.line = line
    fields = line.split(",")
    gI.programName = fields[0].strip()
    gI.programHost = fields[1].strip()
    gI.programGuest = fields[2].strip()
    gI.dayOfWeek = fields[3].strip()
    gI.startTime = fields[4].strip()
    gI.endTime = fields[5].strip()
    try: # the ordinals field is optional, so use a try clause.  If it's not present, we assume show runs each week
        ordinals = fields[6].strip()
    except:
        ordinals = "12345"

    pL.programName  = gI.programName
    pL.programHost = gI.programHost
    pL.programGuest = gI.programGuest

    for o in ordinals:
        wkExists, pL.startTime = convertToDate(year, month, dayDict[gI.dayOfWeek], o)
        if wkExists:  # need to account for the case of ordinal = 5 when there are only 4 of that day in the month.
            pL.endTime = pL.startTime + datetime.timedelta(hours=int(gI.endTime.split(":")[0]), minutes = int(gI.endTime.split(":")[1]))
            pL.startTime = pL.startTime + datetime.timedelta(hours=int(gI.startTime.split(":")[0]), minutes = int(gI.startTime.split(":")[1]))
            outLine = pL.programName + ", " + pL.programHost + ", " + pL.programGuest + ", " + \
                      pL.startTime.isoformat() + ", " + pL.endTime.isoformat()
            k = str(int(time.mktime(pL.startTime.timetuple())))
            pL.lineDict[k]=outLine
    return pL.lineDict

"""
outDict = {}
l1 = "Bob and Eddie Show, Bob, Monday, 08:00, 09:00, 13"
l2 = "Bob and Eddie Show, Eddie, Monday, 08:00, 09:00, 24"
ld1 = parseProgramGuideInput(l1, outDict)
for k in ld1.iterkeys():
    outDict[k] = ld1[k]
ld1 = parseProgramGuideInput(l2, outDict)
for k in ld1.iterkeys():
    outDict[k] = ld1[k]

for k in sorted(outDict.keys()):
    print outDict[k]
#for k in outDict:
#    print outDict[k]
"""
def main():

    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        return(2)

    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print __doc__
            return(0)

    # process arguments
    try:
        input_csv_file = open(args[0],'r')
    except:
        print "could not open file ", args[0], "for reading.  Exiting"
    try:
        output_flat_csv_file = open(args[1], 'w')
    except:
        print "could not open file ", args[1], "for writing.  Exiting"

    outDict = {}
    year = 2017
    month = 1
    for line in input_csv_file:
        if line[0] != '#':
            for month in range(1, 10):
                try:
                    retDict = parseProgramGuideInput(line, year, month, outDict)
                except:
                    print line
                    return -1
                for k in retDict.iterkeys():
                    outDict[k] = retDict[k]

    for k in sorted(outDict.keys()):
        output_flat_csv_file.write(outDict[k]+"\n")

    input_csv_file.close()
    output_flat_csv_file.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())
