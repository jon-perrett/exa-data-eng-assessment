#!/bin/bash
/usr/bin/mc alias set dockerminio http://minio:9000 $MINIO_USER $MINIO_PWD
mc cp --recursive $DATA_LOCATION dockerminio/$RAW_BUCKET