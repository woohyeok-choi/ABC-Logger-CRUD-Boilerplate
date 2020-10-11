from src.operation import operation
from src.pb import datum_pb2

if __name__ == '__main__':
    op = operation.Operation(
        server_address='abc.kaist.ac.kr:50030',
        auth_token='abc-logger-is-based-on-graduates-blood-sweat-tear'
    )

    # Read Data as List (at maximum 5,000)
    print(
        op.read_data(
            datum_type=datum_pb2.APP_USAGE_EVENT,
            limit=1000
        )
    )

    # Read Data as Stream (no limit)
    for datum in op.read_data_as_stream(
        datum_type=datum_pb2.APP_USAGE_EVENT,
        limit=1000
    ):
        print(datum)

    # Read Subjects as List
    print(
        op.read_subjects(
            datum_type=datum_pb2.APP_USAGE_EVENT,
            limit=1000
        )
    )

    # Read Subjects as Stream (no limit)
    for subject in op.read_subjects_as_stream(
            datum_type=datum_pb2.APP_USAGE_EVENT,
            limit=1000
    ):
        print(subject)

    # Read Heart Beats as List
    print(
        op.read_heart_beats(
            datum_type=datum_pb2.APP_USAGE_EVENT,
            limit=1000
        )
    )

    # Read Subjects as Stream (no limit)
    for subject in op.read_heart_beats_as_stream(
            datum_type=datum_pb2.APP_USAGE_EVENT,
            limit=1000
    ):
        print(subject)

    # Count Data
    print(
        op.count_data(
            datum_type=datum_pb2.APP_USAGE_EVENT
        )
    )

    # Count Subject
    print(
        op.count_subjects(
            datum_type=datum_pb2.APP_USAGE_EVENT
        )
    )
