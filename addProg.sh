#!/usr/bin/env bash
echo curl -XPOST $ES/test/_update_by_query -d\'
echo {
echo   \"script\": {
echo     \"lang\": \"painless\",
echo     \"inline\": \"ctx._source.artist = params.artist\; ctx._source.programName = params.programName\",
echo     \"params\": {
echo       \"artist\": \"King Louies Blues Revue\",
echo       \"programName\": \"Waterfront Blues Festival\"
echo     }
echo   },
echo   \"query\": {
echo     \"range\": {
echo       \"timestamp\": {
echo         \"gte\": \"2017-07-02T16:00:00-07:00\",
echo         \"lt\": \"2017-07-02T17:00:00-07:00\",
echo         \"format\": \"date_optional_time\"
echo       }
echo     }
echo   }
echo }\'


