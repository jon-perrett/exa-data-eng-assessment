#!/bin/bash
sleep 5
/usr/bin/mc alias set dockerminio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
/usr/bin/mc mb dockerminio/$RAW_BUCKET
/usr/bin/mc mb dockerminio/$TRANSFORMED_BUCKET
exit 0