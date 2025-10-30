#!/bin/bash
sleep 5
/usr/bin/mc alias set dockerminio http://minio:9000 $MINIO_USER $MINIO_PWD
/usr/bin/mc mb dockerminio/$RAW_BUCKET
/usr/bin/mc mb dockerminio/$TRANSFORMED_BUCKET
exit 0