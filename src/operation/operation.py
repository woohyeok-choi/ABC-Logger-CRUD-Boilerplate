from grpc import insecure_channel
from src.pb import service_pb2_grpc, service_pb2, subject_pb2, datum_pb2, aggregate_pb2, heart_beat_pb2
from typing import List, Union, Optional, Dict, Iterator


def _safe_list(value: Optional[Union[any, List[any]]]) -> List[any]:
    if value is None:
        return []
    elif type(value) is List:
        return value
    else:
        return [value]


def _read_query(
        datum_type: Optional[Union[int, List[int]]] = None,
        group_name: Optional[Union[str, List[str]]] = None,
        email: Optional[Union[str, List[str]]] = None,
        instance_id: Optional[Union[str, List[str]]] = None,
        source: Optional[Union[str, List[str]]] = None,
        device_manufacturer: Optional[Union[str, List[str]]] = None,
        device_model: Optional[Union[str, List[str]]] = None,
        device_version: Optional[Union[str, List[str]]] = None,
        device_os: Optional[Union[str, List[str]]] = None,
        app_id: Optional[Union[str, List[str]]] = None,
        app_version: Optional[Union[str, List[str]]] = None,
        from_timestamp: Optional[int] = None,
        to_timestamp: Optional[int] = None,
        limit: Optional[int] = None,
        is_ascending: bool = False,
        auth_token: str = None
) -> Dict:
    query = dict(
        datum_type=_safe_list(datum_type),
        group_name=_safe_list(group_name),
        email=_safe_list(email),
        instance_id=_safe_list(instance_id),
        source=_safe_list(source),
        device_manufacturer=_safe_list(device_manufacturer),
        device_model=_safe_list(device_model),
        device_version=_safe_list(device_version),
        device_os=_safe_list(device_os),
        app_id=_safe_list(app_id),
        app_version=_safe_list(app_version),
        from_timestamp=0 if None else from_timestamp,
        to_timestamp=0 if None else to_timestamp,
        limit=0 if None else limit,
        is_ascending=is_ascending
    )

    return dict(
        request=service_pb2.Query.Read(**query),
        metadata=[('auth_token', auth_token)]
    )


def _aggregate_query(
        datum_type: Optional[Union[int, List[int]]] = None,
        group_name: Optional[Union[str, List[str]]] = None,
        email: Optional[Union[str, List[str]]] = None,
        instance_id: Optional[Union[str, List[str]]] = None,
        source: Optional[Union[str, List[str]]] = None,
        device_manufacturer: Optional[Union[str, List[str]]] = None,
        device_model: Optional[Union[str, List[str]]] = None,
        device_version: Optional[Union[str, List[str]]] = None,
        device_os: Optional[Union[str, List[str]]] = None,
        app_id: Optional[Union[str, List[str]]] = None,
        app_version: Optional[Union[str, List[str]]] = None,
        from_timestamp: Optional[int] = None,
        to_timestamp: Optional[int] = None,
        auth_token: str = None
) -> Dict:
    query = dict(
        datum_type=_safe_list(datum_type),
        group_name=_safe_list(group_name),
        email=_safe_list(email),
        instance_id=_safe_list(instance_id),
        source=_safe_list(source),
        device_manufacturer=_safe_list(device_manufacturer),
        device_model=_safe_list(device_model),
        device_version=_safe_list(device_version),
        device_os=_safe_list(device_os),
        app_id=_safe_list(app_id),
        app_version=_safe_list(app_version),
        from_timestamp=0 if None else from_timestamp,
        to_timestamp=0 if None else to_timestamp,
    )

    return dict(
        request=service_pb2.Query.Aggregate(**query),
        metadata=[('auth_token', auth_token)]
    )

class Operation:
    def __init__(self, server_address: str, auth_token: str):
        self._server_address = server_address
        self._auth_token = auth_token
        self._channel = insecure_channel(self._server_address)
        self._data_stub = service_pb2_grpc.DataOperationsStub(self._channel)
        self._subject_stub = service_pb2_grpc.SubjectsOperationsStub(self._channel)
        self._heart_beat_stub = service_pb2_grpc.HeartBeatsOperationStub(self._channel)
        self._aggregate_stub = service_pb2_grpc.AggregateOperationsStub(self._channel)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._channel.close()

    def read_data(
            self,
            datum_type: Optional[Union[int, List[int]]] = None,
            group_name: Optional[Union[str, List[str]]] = None,
            email: Optional[Union[str, List[str]]] = None,
            instance_id: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = None,
            device_manufacturer: Optional[Union[str, List[str]]] = None,
            device_model: Optional[Union[str, List[str]]] = None,
            device_version: Optional[Union[str, List[str]]] = None,
            device_os: Optional[Union[str, List[str]]] = None,
            app_id: Optional[Union[str, List[str]]] = None,
            app_version: Optional[Union[str, List[str]]] = None,
            from_timestamp: Optional[int] = None,
            to_timestamp: Optional[int] = None,
            limit: Optional[int] = None,
            is_ascending: bool = False
    ) -> List[datum_pb2.Datum]:
        response = self._data_stub.ReadData(
            **_read_query(
                datum_type=datum_type,
                group_name=group_name,
                email=email,
                instance_id=instance_id,
                source=source,
                device_manufacturer=device_manufacturer,
                device_model=device_model,
                device_version=device_version,
                device_os=device_os,
                app_id=app_id,
                app_version=app_version,
                from_timestamp=0 if None else from_timestamp,
                to_timestamp=0 if None else to_timestamp,
                limit=0 if None else limit,
                is_ascending=is_ascending,
                auth_token=self._auth_token
            )
        )
        return response.datum

    def read_data_as_stream(
            self,
            datum_type: Optional[Union[int, List[int]]] = None,
            group_name: Optional[Union[str, List[str]]] = None,
            email: Optional[Union[str, List[str]]] = None,
            instance_id: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = None,
            device_manufacturer: Optional[Union[str, List[str]]] = None,
            device_model: Optional[Union[str, List[str]]] = None,
            device_version: Optional[Union[str, List[str]]] = None,
            device_os: Optional[Union[str, List[str]]] = None,
            app_id: Optional[Union[str, List[str]]] = None,
            app_version: Optional[Union[str, List[str]]] = None,
            from_timestamp: Optional[int] = None,
            to_timestamp: Optional[int] = None,
            limit: Optional[int] = None,
            is_ascending: bool = False
    ) -> Iterator[datum_pb2.Datum]:
        return self._data_stub.ReadDataAsStream(
            **_read_query(
                datum_type=datum_type,
                group_name=group_name,
                email=email,
                instance_id=instance_id,
                source=source,
                device_manufacturer=device_manufacturer,
                device_model=device_model,
                device_version=device_version,
                device_os=device_os,
                app_id=app_id,
                app_version=app_version,
                from_timestamp=0 if None else from_timestamp,
                to_timestamp=0 if None else to_timestamp,
                limit=0 if None else limit,
                is_ascending=is_ascending,
                auth_token=self._auth_token
            )
        )

    def read_subjects(
            self,
            datum_type: Optional[Union[int, List[int]]] = None,
            group_name: Optional[Union[str, List[str]]] = None,
            email: Optional[Union[str, List[str]]] = None,
            instance_id: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = None,
            device_manufacturer: Optional[Union[str, List[str]]] = None,
            device_model: Optional[Union[str, List[str]]] = None,
            device_version: Optional[Union[str, List[str]]] = None,
            device_os: Optional[Union[str, List[str]]] = None,
            app_id: Optional[Union[str, List[str]]] = None,
            app_version: Optional[Union[str, List[str]]] = None,
            from_timestamp: Optional[int] = None,
            to_timestamp: Optional[int] = None,
            limit: Optional[int] = None,
            is_ascending: bool = False
    ) -> List[subject_pb2.Subject]:
        response = self._subject_stub.ReadSubjects(
            **_read_query(
                datum_type=datum_type,
                group_name=group_name,
                email=email,
                instance_id=instance_id,
                source=source,
                device_manufacturer=device_manufacturer,
                device_model=device_model,
                device_version=device_version,
                device_os=device_os,
                app_id=app_id,
                app_version=app_version,
                from_timestamp=0 if None else from_timestamp,
                to_timestamp=0 if None else to_timestamp,
                limit=0 if None else limit,
                is_ascending=is_ascending,
                auth_token=self._auth_token
            )
        )
        return response.subject

    def read_subjects_as_stream(
            self,
            datum_type: Optional[Union[int, List[int]]] = None,
            group_name: Optional[Union[str, List[str]]] = None,
            email: Optional[Union[str, List[str]]] = None,
            instance_id: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = None,
            device_manufacturer: Optional[Union[str, List[str]]] = None,
            device_model: Optional[Union[str, List[str]]] = None,
            device_version: Optional[Union[str, List[str]]] = None,
            device_os: Optional[Union[str, List[str]]] = None,
            app_id: Optional[Union[str, List[str]]] = None,
            app_version: Optional[Union[str, List[str]]] = None,
            from_timestamp: Optional[int] = None,
            to_timestamp: Optional[int] = None,
            limit: Optional[int] = None,
            is_ascending: bool = False
    ) -> Iterator[subject_pb2.Subject]:
        return self._subject_stub.ReadSubjectsAsStream(
            **_read_query(
                datum_type=datum_type,
                group_name=group_name,
                email=email,
                instance_id=instance_id,
                source=source,
                device_manufacturer=device_manufacturer,
                device_model=device_model,
                device_version=device_version,
                device_os=device_os,
                app_id=app_id,
                app_version=app_version,
                from_timestamp=0 if None else from_timestamp,
                to_timestamp=0 if None else to_timestamp,
                limit=0 if None else limit,
                is_ascending=is_ascending,
                auth_token=self._auth_token
            )
        )

    def read_heart_beats(
            self,
            datum_type: Optional[Union[int, List[int]]] = None,
            group_name: Optional[Union[str, List[str]]] = None,
            email: Optional[Union[str, List[str]]] = None,
            instance_id: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = None,
            device_manufacturer: Optional[Union[str, List[str]]] = None,
            device_model: Optional[Union[str, List[str]]] = None,
            device_version: Optional[Union[str, List[str]]] = None,
            device_os: Optional[Union[str, List[str]]] = None,
            app_id: Optional[Union[str, List[str]]] = None,
            app_version: Optional[Union[str, List[str]]] = None,
            from_timestamp: Optional[int] = None,
            to_timestamp: Optional[int] = None,
            limit: Optional[int] = None,
            is_ascending: bool = False
    ) -> List[heart_beat_pb2.HeartBeat]:
        response = self._heart_beat_stub.ReadHeartBeats(
            **_read_query(
                datum_type=datum_type,
                group_name=group_name,
                email=email,
                instance_id=instance_id,
                source=source,
                device_manufacturer=device_manufacturer,
                device_model=device_model,
                device_version=device_version,
                device_os=device_os,
                app_id=app_id,
                app_version=app_version,
                from_timestamp=0 if None else from_timestamp,
                to_timestamp=0 if None else to_timestamp,
                limit=0 if None else limit,
                is_ascending=is_ascending,
                auth_token=self._auth_token
            )
        )
        return response.heart_beat

    def read_heart_beats_as_stream(
            self,
            datum_type: Optional[Union[int, List[int]]] = None,
            group_name: Optional[Union[str, List[str]]] = None,
            email: Optional[Union[str, List[str]]] = None,
            instance_id: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = None,
            device_manufacturer: Optional[Union[str, List[str]]] = None,
            device_model: Optional[Union[str, List[str]]] = None,
            device_version: Optional[Union[str, List[str]]] = None,
            device_os: Optional[Union[str, List[str]]] = None,
            app_id: Optional[Union[str, List[str]]] = None,
            app_version: Optional[Union[str, List[str]]] = None,
            from_timestamp: Optional[int] = None,
            to_timestamp: Optional[int] = None,
            limit: Optional[int] = None,
            is_ascending: bool = False
    ) -> Iterator[heart_beat_pb2.HeartBeat]:
        return self._heart_beat_stub.ReadHeartBeatsAsStream(
            **_read_query(
                datum_type=datum_type,
                group_name=group_name,
                email=email,
                instance_id=instance_id,
                source=source,
                device_manufacturer=device_manufacturer,
                device_model=device_model,
                device_version=device_version,
                device_os=device_os,
                app_id=app_id,
                app_version=app_version,
                from_timestamp=0 if None else from_timestamp,
                to_timestamp=0 if None else to_timestamp,
                limit=0 if None else limit,
                is_ascending=is_ascending,
                auth_token=self._auth_token
            )
        )

    def count_subjects(
            self,
            datum_type: Optional[Union[int, List[int]]] = None,
            group_name: Optional[Union[str, List[str]]] = None,
            email: Optional[Union[str, List[str]]] = None,
            instance_id: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = None,
            device_manufacturer: Optional[Union[str, List[str]]] = None,
            device_model: Optional[Union[str, List[str]]] = None,
            device_version: Optional[Union[str, List[str]]] = None,
            device_os: Optional[Union[str, List[str]]] = None,
            app_id: Optional[Union[str, List[str]]] = None,
            app_version: Optional[Union[str, List[str]]] = None,
            from_timestamp: Optional[int] = None,
            to_timestamp: Optional[int] = None,
    ) -> aggregate_pb2.Aggregation:
        return self._aggregate_stub.CountSubjects(
            **_aggregate_query(
                datum_type=datum_type,
                group_name=group_name,
                email=email,
                instance_id=instance_id,
                source=source,
                device_manufacturer=device_manufacturer,
                device_model=device_model,
                device_version=device_version,
                device_os=device_os,
                app_id=app_id,
                app_version=app_version,
                from_timestamp=0 if None else from_timestamp,
                to_timestamp=0 if None else to_timestamp,
                auth_token=self._auth_token
            )
        )

    def count_data(
            self,
            datum_type: Optional[Union[int, List[int]]] = None,
            group_name: Optional[Union[str, List[str]]] = None,
            email: Optional[Union[str, List[str]]] = None,
            instance_id: Optional[Union[str, List[str]]] = None,
            source: Optional[Union[str, List[str]]] = None,
            device_manufacturer: Optional[Union[str, List[str]]] = None,
            device_model: Optional[Union[str, List[str]]] = None,
            device_version: Optional[Union[str, List[str]]] = None,
            device_os: Optional[Union[str, List[str]]] = None,
            app_id: Optional[Union[str, List[str]]] = None,
            app_version: Optional[Union[str, List[str]]] = None,
            from_timestamp: Optional[int] = None,
            to_timestamp: Optional[int] = None,
    ) -> aggregate_pb2.Aggregation:
        return self._aggregate_stub.CountData(
            **_aggregate_query(
                datum_type=datum_type,
                group_name=group_name,
                email=email,
                instance_id=instance_id,
                source=source,
                device_manufacturer=device_manufacturer,
                device_model=device_model,
                device_version=device_version,
                device_os=device_os,
                app_id=app_id,
                app_version=app_version,
                from_timestamp=0 if None else from_timestamp,
                to_timestamp=0 if None else to_timestamp,
                auth_token=self._auth_token
            )
        )
