#!/bin/bash
/usr/bin/mc alias set dockerminio http://minio:9000 $MINIO_USER $MINIO_PWD
/usr/bin/mc admin config set dockerminio notify_webhook:1 endpoint="http://$WEBHOOK_SERVER/transform" queue_limit="10000"
/usr/bin/mc admin service restart dockerminio --json
sleep 5
/usr/bin/mc event add  --event put dockerminio/$RAW_BUCKET arn:minio:sqs::1:webhook