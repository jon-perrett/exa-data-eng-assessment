from minio import Minio
from minio.error import S3Error
import logging

class GetObjectError(Exception):
    """An exception raised when get_object fails"""

class MinIOClient:
    def __init__(self, endpoint: str,  username: str, password: str, use_tls: bool = False):
        self._username = username
        self._password = password
        self._client = Minio(endpoint, access_key=username, secret_key=password, secure=use_tls)
    
    def get_object(self, bucket: str, key: str) -> bytes:
        response = None
        try:
            response = self._client.get_object(bucket, key)
            body = response.read()
        except S3Error as e:
            raise GetObjectError() from e
        finally:
            if response:
                response.close()
                response.release_conn()
        return body