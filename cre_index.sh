#!/usr/bin/env bash
curl -XPUT $ES/sessions?pretty -H 'Content-Type: application/json' -d'
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
        "referer": { "type": "text" },
        "userAgent": { "type": "text" },
        "connectTimeInMinutes": { "type": "long" },
        "numDurationTime": { "type": "long" },
        "location": { "type": "geo_point"},
        "country_iso": {"type": "text"}
      }
    },
    "listenerCountEntry": {
      "properties": {
        "lcTimestamp": { "type": "date",
                       "format": "epoch_second"
        },
        "listenerCount": { "type": "long"}
      }
    }
  }
}
'
