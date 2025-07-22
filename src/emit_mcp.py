from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    SchemaMetadataClass,
    SchemaFieldClass,
    StringTypeClass,
    AuditStampClass,
)
import time

emitter = DatahubRestEmitter("http://localhost:8080")

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,mydb.mytable,PROD)"

fields = [
    SchemaFieldClass(
        fieldPath="customer_id",
        type=StringTypeClass(),
        nativeDataType="VARCHAR",
        description="Unique customer identifier"
    ),
    SchemaFieldClass(
        fieldPath="purchase_date",
        type=StringTypeClass(),
        nativeDataType="DATE",
        description="Date of purchase"
    )
]

now_ms = int(time.time() * 1000)

schema_metadata = SchemaMetadataClass(
    schemaName="mytable",
    platform="urn:li:dataPlatform:mysql",
    version=0,
    hash="",
    platformSchema={},  # <-- Use empty dict here!
    created=AuditStampClass(
        time=now_ms,
        actor="urn:li:corpuser:etl"
    ),
    lastModified=AuditStampClass(
        time=now_ms,
        actor="urn:li:corpuser:etl"
    ),
    fields=fields
)

mcp = MetadataChangeProposalWrapper(
    entityUrn=dataset_urn,
    aspect=schema_metadata,
    aspectName="schemaMetadata"
)

emitter.emit(mcp)
print(f"Emitted MCP for {dataset_urn}")

