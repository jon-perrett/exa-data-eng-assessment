#!/bin/bash
/usr/bin/mc alias set dockerminio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
mc cp --recursive $DATA_LOCATION dockerminio/$RAW_BUCKET