from __future__ import annotations

import base64
import json
import uuid
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from app.services.storage_types import InvalidStorageCursorError, StorageConflictError


PK = "pk"
SK = "sk"
VERSION = "version"
GSI1_NAME = "gsi1"
GSI2_NAME = "gsi2"
GSI3_NAME = "gsi3"


def _to_dynamodb(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {str(key): _to_dynamodb(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_dynamodb(item) for item in value]
    if isinstance(value, tuple):
        return [_to_dynamodb(item) for item in value]
    return value


def _from_dynamodb(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, dict):
        return {str(key): _from_dynamodb(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_from_dynamodb(item) for item in value]
    return value


def _encode_cursor(key: Optional[Dict[str, Any]]) -> Optional[str]:
    if not key:
        return None
    raw = json.dumps(_from_dynamodb(key), sort_keys=True, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_cursor(cursor: Optional[str]) -> Optional[Dict[str, Any]]:
    if not cursor:
        return None
    try:
        padding = "=" * (-len(cursor) % 4)
        decoded = base64.urlsafe_b64decode((cursor + padding).encode("ascii"))
        value = json.loads(decoded.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidStorageCursorError("Invalid pagination cursor") from exc
    if not isinstance(value, dict):
        raise InvalidStorageCursorError("Invalid pagination cursor")
    return _to_dynamodb(value)


class DynamoDBStorage:
    """DynamoDB implementation of SCENE's collection storage contract."""

    def __init__(
        self,
        *,
        table_name: str,
        region_name: str,
        default_config_factory: Callable[[], Dict[str, Any]],
        config_override: Callable[[Dict[str, Any]], Dict[str, Any]],
        endpoint_url: Optional[str] = None,
        table: Any = None,
        validate_table: bool = True,
    ) -> None:
        if not table_name.strip():
            raise ValueError("SCENE_DYNAMODB_TABLE is required for the DynamoDB backend")
        self.table_name = table_name.strip()
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self._default_config_factory = default_config_factory
        self._config_override = config_override
        if table is None:
            resource = boto3.resource(
                "dynamodb",
                region_name=region_name,
                endpoint_url=endpoint_url,
            )
            table = resource.Table(self.table_name)
        self._table = table
        if validate_table:
            self._validate_table()

    @staticmethod
    def _collection_pk(collection: str) -> str:
        return f"COLLECTION#{collection}"

    @staticmethod
    def _sort_value(payload: Dict[str, Any], item_id: str) -> str:
        created_at = str(payload.get("created_at") or "")
        sequence = payload.get("sequence")
        if sequence is not None:
            try:
                return f"{int(sequence):012d}#{created_at}#{item_id}"
            except (TypeError, ValueError):
                pass
        return f"{created_at}#{item_id}"

    def _validate_table(self) -> None:
        try:
            description = self._table.meta.client.describe_table(
                TableName=self.table_name
            )["Table"]
        except ClientError as exc:
            raise RuntimeError(
                f"DynamoDB table '{self.table_name}' is unavailable in {self.region_name}"
            ) from exc
        key_schema = {entry["AttributeName"]: entry["KeyType"] for entry in description["KeySchema"]}
        if key_schema != {PK: "HASH", SK: "RANGE"}:
            raise RuntimeError(
                f"DynamoDB table '{self.table_name}' must use pk HASH and sk RANGE keys"
            )
        indexes = {
            index["IndexName"]: {
                entry["AttributeName"]: entry["KeyType"]
                for entry in index.get("KeySchema", [])
            }
            for index in description.get("GlobalSecondaryIndexes", [])
        }
        missing = {GSI1_NAME, GSI2_NAME, GSI3_NAME} - set(indexes)
        if missing:
            raise RuntimeError(
                f"DynamoDB table '{self.table_name}' is missing indexes: "
                + ", ".join(sorted(missing))
            )
        expected_indexes = {
            GSI1_NAME: {"gsi1pk": "HASH", "gsi1sk": "RANGE"},
            GSI2_NAME: {"gsi2pk": "HASH", "gsi2sk": "RANGE"},
            GSI3_NAME: {"gsi3pk": "HASH", "gsi3sk": "RANGE"},
        }
        invalid = [
            name for name, expected in expected_indexes.items() if indexes[name] != expected
        ]
        if invalid:
            raise RuntimeError(
                f"DynamoDB table '{self.table_name}' has incorrectly keyed indexes: "
                + ", ".join(invalid)
            )

    def _item(
        self,
        collection: str,
        item_id: str,
        payload: Dict[str, Any],
        version: int,
    ) -> Dict[str, Any]:
        clean_payload = {key: value for key, value in payload.items() if key != "_version"}
        sort_value = self._sort_value(clean_payload, item_id)
        item: Dict[str, Any] = {
            PK: self._collection_pk(collection),
            SK: item_id,
            VERSION: version,
            "entity_type": collection,
            "created_sort": sort_value,
            "payload": _to_dynamodb(clean_payload),
            "gsi3pk": self._collection_pk(collection),
            "gsi3sk": sort_value,
        }
        project_id = clean_payload.get("project_id")
        if project_id:
            item["gsi1pk"] = f"PROJECT#{project_id}#{collection}"
            item["gsi1sk"] = sort_value
        if collection in {"runs", "baselines"} and clean_payload.get("batch_id"):
            item["gsi2pk"] = f"BATCH#{clean_payload['batch_id']}#{collection}"
            item["gsi2sk"] = sort_value
        elif collection == "executions" and clean_payload.get("run_id"):
            item["gsi2pk"] = f"RUN#{clean_payload['run_id']}#executions"
            item["gsi2sk"] = sort_value
        return item

    @staticmethod
    def _record(item: Dict[str, Any]) -> Dict[str, Any]:
        payload = _from_dynamodb(item.get("payload") or {})
        payload["_version"] = int(item.get(VERSION, 0))
        return payload

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Large idempotent imports can exceed DynamoDB's 100-operation transaction
        # limit. Domain validation happens before this block and individual writes
        # remain conditional, so interrupted imports are safe to replay.
        yield

    def get_config(self) -> Dict[str, Any]:
        record = self.get("config", "global")
        config = self._default_config_factory()
        if record:
            config.update({key: value for key, value in record.items() if key != "_version"})
        return self._config_override(config)

    def update_config(self, **updates: Any) -> Dict[str, Any]:
        for attempt in range(5):
            record = self.get("config", "global")
            if record is None:
                record = self._default_config_factory()
            for key, value in updates.items():
                if value is not None:
                    record[key] = value
            try:
                saved = self.upsert("config", "global", record)
                return self._config_override(
                    {key: value for key, value in saved.items() if key != "_version"}
                )
            except StorageConflictError:
                if attempt == 4:
                    raise
        raise AssertionError("unreachable")

    def upsert(
        self,
        collection: str,
        item_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        expected_version = payload.get("_version")
        next_version = int(expected_version or 0) + 1
        item = self._item(collection, item_id, payload, next_version)
        names = {"#pk": PK, "#sk": SK, "#version": VERSION}
        values: Dict[str, Any] = {}
        if expected_version is None:
            condition = "attribute_not_exists(#pk) AND attribute_not_exists(#sk)"
        elif int(expected_version) == 0:
            condition = (
                "attribute_exists(#pk) AND "
                "(attribute_not_exists(#version) OR #version = :expected)"
            )
            values[":expected"] = 0
        else:
            condition = "attribute_exists(#pk) AND #version = :expected"
            values[":expected"] = int(expected_version)
        request: Dict[str, Any] = {
            "Item": item,
            "ConditionExpression": condition,
            "ExpressionAttributeNames": names,
        }
        if values:
            request["ExpressionAttributeValues"] = values
        try:
            self._table.put_item(**request)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                raise StorageConflictError(
                    f"{collection}/{item_id} changed during update"
                ) from exc
            raise
        result = {key: value for key, value in payload.items() if key != "_version"}
        result["_version"] = next_version
        return result

    def get(self, collection: str, item_id: str) -> Optional[Dict[str, Any]]:
        response = self._table.get_item(
            Key={PK: self._collection_pk(collection), SK: item_id},
            ConsistentRead=True,
        )
        item = response.get("Item")
        return self._record(item) if item else None

    def delete(self, collection: str, item_id: str) -> None:
        self._table.delete_item(Key={PK: self._collection_pk(collection), SK: item_id})

    def _query_all(self, **kwargs: Any) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        request = dict(kwargs)
        while True:
            response = self._table.query(**request)
            items.extend(self._record(item) for item in response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                return items
            request["ExclusiveStartKey"] = last_key

    def list(self, collection: str) -> List[Dict[str, Any]]:
        return self._query_all(
            KeyConditionExpression=Key(PK).eq(self._collection_pk(collection)),
            ConsistentRead=True,
        )

    @staticmethod
    def _scope(
        collection: str,
        key: Optional[str],
        value: Any,
    ) -> Optional[Tuple[str, str, str, str]]:
        if key == "project_id" and collection in {
            "pages",
            "tasks",
            "batches",
            "runs",
            "baselines",
        }:
            return GSI1_NAME, "gsi1pk", f"PROJECT#{value}#{collection}", "gsi1sk"
        if key == "batch_id" and collection in {"runs", "baselines"}:
            return GSI2_NAME, "gsi2pk", f"BATCH#{value}#{collection}", "gsi2sk"
        if key == "run_id" and collection == "executions":
            return GSI2_NAME, "gsi2pk", f"RUN#{value}#executions", "gsi2sk"
        return None

    def filter(self, collection: str, *, key: str, value: Any) -> List[Dict[str, Any]]:
        scope = self._scope(collection, key, value)
        if scope:
            index_name, pk_name, pk_value, _sort_name = scope
            return self._query_all(
                IndexName=index_name,
                KeyConditionExpression=Key(pk_name).eq(pk_value),
            )
        return [record for record in self.list(collection) if record.get(key) == value]

    def bulk_delete(self, collection: str, item_ids: Iterable[str]) -> None:
        with self._table.batch_writer() as batch:
            for item_id in item_ids:
                batch.delete_item(
                    Key={PK: self._collection_pk(collection), SK: str(item_id)}
                )

    def query_page(
        self,
        collection: str,
        *,
        key: Optional[str] = None,
        value: Any = None,
        limit: int = 25,
        cursor: Optional[str] = None,
        descending: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        scope = self._scope(collection, key, value)
        if scope:
            index_name, pk_name, pk_value, _sort_name = scope
        elif key is None:
            index_name = GSI3_NAME
            pk_name = "gsi3pk"
            pk_value = self._collection_pk(collection)
        else:
            records = self.filter(collection, key=key, value=value)
            records.sort(
                key=lambda record: (
                    str(record.get("created_at") or ""),
                    str(record.get("id") or ""),
                ),
                reverse=descending,
            )
            try:
                offset = int(cursor or 0)
            except (TypeError, ValueError) as exc:
                raise InvalidStorageCursorError("Invalid pagination cursor") from exc
            if offset < 0:
                raise InvalidStorageCursorError("Invalid pagination cursor")
            page_size = max(1, min(int(limit), 100))
            page = records[offset : offset + page_size]
            next_offset = offset + len(page)
            return page, str(next_offset) if next_offset < len(records) else None

        request: Dict[str, Any] = {
            "IndexName": index_name,
            "KeyConditionExpression": Key(pk_name).eq(pk_value),
            "Limit": max(1, min(int(limit), 100)),
            "ScanIndexForward": not descending,
        }
        start_key = _decode_cursor(cursor)
        if start_key:
            request["ExclusiveStartKey"] = start_key
        response = self._table.query(**request)
        records = [self._record(item) for item in response.get("Items", [])]
        return records, _encode_cursor(response.get("LastEvaluatedKey"))

    def count(
        self,
        collection: str,
        *,
        key: Optional[str] = None,
        value: Any = None,
    ) -> int:
        scope = self._scope(collection, key, value)
        if scope:
            index_name, pk_name, pk_value, _sort_name = scope
            request: Dict[str, Any] = {
                "IndexName": index_name,
                "KeyConditionExpression": Key(pk_name).eq(pk_value),
                "Select": "COUNT",
            }
        elif key is None:
            request = {
                "KeyConditionExpression": Key(PK).eq(self._collection_pk(collection)),
                "Select": "COUNT",
                "ConsistentRead": True,
            }
        else:
            return len(self.filter(collection, key=key, value=value))
        count = 0
        while True:
            response = self._table.query(**request)
            count += int(response.get("Count", 0))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                return count
            request["ExclusiveStartKey"] = last_key

    def backend_info(self) -> Dict[str, Any]:
        return {
            "backend": "dynamodb",
            "region": self.region_name,
            "table": self.table_name,
            "endpoint_override": bool(self.endpoint_url),
        }

    def probe(self) -> Dict[str, Any]:
        probe_id = str(uuid.uuid4())
        record = {
            "id": probe_id,
            "created_at": "probe",
            "purpose": "readiness",
        }
        try:
            self.upsert("probes", probe_id, record)
            if self.get("probes", probe_id) is None:
                raise RuntimeError("DynamoDB readiness probe record could not be read")
        finally:
            self.delete("probes", probe_id)
        return {"ok": True, "write_read_delete": True, **self.backend_info()}
