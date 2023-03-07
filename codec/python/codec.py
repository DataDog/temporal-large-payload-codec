import requests
import hashlib
import base64
import json
from typing import Iterable, List

from temporalio.api.common.v1 import Payload
from temporalio.converter import PayloadCodec, DefaultPayloadConverter
from gen import codec_pb2

MIN_LARGE_PAYLOAD_BYTES = 128_000
REMOTE_CODEC_NAME = "temporal.io/remote-codec"

class LargePayloadCodec(PayloadCodec):
    def __init__(self, namespace:str, url:str, min_bytes: int = MIN_LARGE_PAYLOAD_BYTES) -> None:
        super().__init__()
        self.namespace = namespace
        self.url = url
        self.min_bytes = min_bytes

        # Initialize dataconverter 
        self.converter = DefaultPayloadConverter()

        # TODO Check connectivity to server


    async def encode(self, payloads: Iterable[Payload]) -> List[Payload]:
        result = []
        for payload in payloads:
            if payload.ByteSize() > self.min_bytes:
                result.append(self.putLargePayload(payload))
            else:
                result.append(payload)
        return result

    async def decode(self, payloads: Iterable[Payload]) -> List[Payload]:
        result = []
        for payload in payloads:
            # Payloads with the remote codec name in the metadata
            # are large payloads 
            if payload.metadata.get(REMOTE_CODEC_NAME) != None:
                result.append(self.getLargePayload(payload))
            else:
                result.append(payload)

        return result

    def putLargePayload(self, payload: Payload) -> Payload:
        # Set payload parameters
        digest = "sha256:" + hashlib.sha256(payload.data).hexdigest()
        request_payload = {"digest": digest, "namespace": self.namespace} 
        headers = {"Content-Type": "application/octet-stream"}
        
        # Set Temporal Metadata Header
        metadata_json = json.dumps(payload.metadata)
        b64_bytes = base64.b64encode(metadata_json.encode())
        b64_metadata = b64_bytes.decode()
        headers.update({"X-Temporal-Metadata": b64_metadata})
        
        resp = requests.put(
                self.url + "v2/blobs/put", 
                data=payload.data, 
                params=request_payload, 
                headers=headers, 
        ) 
        resp.raise_for_status()
    
        result = self.converter.to_payloads([codec_pb2.RemotePayload(
            metadata=payload.metadata,
            size=len(payload.data),
            digest=digest,
            key=resp.json()["key"],
            )]
        )

        return result[0]

    def getLargePayload(self, payload: Payload) -> Payload:
        # TODO implement me
        
        remote_payload = self.converter.from_payloads([payload], type_hints=[codec_pb2.RemotePayload])[0]
        
        request_params = {"key", remote_payload.key}
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Payload-Expected-Content-Length": base64.b64decode(remote_payload.size).decode(),
        }
        
        # TODO we likely need to stream the response and read it in in chunks
        resp = requests.get(
                self.url + "v2/blobs/put",
                params=request_params,
                headers=headers,
        )
        resp.raise_for_status()

        # TODO hash.Hash is a normal writer in go
        # the content of it is still the byte response
        digest = "sha256:" + hashlib.sha256(resp.raw).hexdigest()

        if digest != remote_payload.digest:
            raise Exception("Digests do not match. Wanted object sha:", remote_payload.digest, "got: ", digest)

        return Payload(
                metadata=remote_payload.metadata,
                data=resp.raw,
        )
