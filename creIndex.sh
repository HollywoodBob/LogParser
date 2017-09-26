#!/usr/bin/env bash
while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -e|--elastic_url)
    ELASTIC_URL="$2"
    shift # past argument
    ;;
    -i|--elastic_index)
    ELASTIC_INDEX="$2"
    shift # past argument
    ;;
    -h|--help)
    echo arguments are -e "elastic URL" -i "elastic index"
    exit 1
    ;;
    --default)
    DEFAULT=YES
    ;;
    *)
            # unknown option
    ;;
esac
shift # past argument or value
done

echo ELASTIC_URL  = "${ELASTIC_URL}"
echo ELASTIC_INDEX     = "${ELASTIC_INDEX}"

curl -XPUT ${ELASTIC_URL}/${ELASTIC_INDEX}?pretty -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "logEntry": {
      "properties": {
        "ipAddr": { "type": "ip" },
        "auth": { "type": "text" },
        "timestamp": { "type": "date",
                       "format": "epoch_second"
        },
        "startTime": { "type": "date",
                       "format": "epoch_second"
        },
        "cmd": { "type": "text" },
        "statusCode": { "type": "text" },
        "numBytesSent": { "type": "long" },
        "referer": {"type": "text",
                    "fields": {
                       "raw": {
                         "type": "keyword" }
                       }
                     },
        "userAgent": {"type": "text",
                      "fields": {
                        "raw": {
                           "type": "keyword" }
                        }
                      },
        "connectTimeInMinutes": { "type": "long" },
        "numDurationTime": { "type": "long" },
        "location": { "type": "geo_point"},
        "programName": {"type": "text",
                        "fields" : {
                          "raw": {
                            "type": "keyword" }
                          }
                        },
        "host":   {"type": "text",
                       "fields" : {
                         "raw": {
                           "type": "keyword" }
                       }
                     },
        "guest":   {"type": "text",
                       "fields" : {
                         "raw": {
                           "type": "keyword" }
                       }
                     },
        "country_iso": {"type": "text",
                        "fields": {
                           "raw": {
                             "type": "keyword" }
                           }
                        },
        "state_iso" : {"type": "text",
                       "fields": {
                          "raw": {
                            "type": "keyword" }
                          }
                       },
        "city" : {"type": "text",
                  "fields": {
                     "raw": {
                       "type": "keyword" }
                     }
                  },
        "zip_code" : {"type": "text",
                      "fields": {
                         "raw": {
                           "type": "keyword" }
                         }
                      },
        "listenerCount": { "type": "long"}
      }
    }
  }
}

'
