#!/usr/bin/env bash
curl -XPUT $ES/test?pretty -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "logEntry": {
      "properties": {
        "ipAddr": { "type": "ip" },
        "auth": { "type": "text" },
        "timestamp": { "type": "date",
                       "format": "epoch_second"
        },
        "endTime": { "type": "date",
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
        "artist":   {"type": "text",
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
