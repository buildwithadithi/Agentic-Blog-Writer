from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer


def create_checkpoint_serializer() -> JsonPlusSerializer:
    return JsonPlusSerializer(
        allowed_msgpack_modules=[
            ("blog_writer.models", "EvidenceItem"),
            ("blog_writer.models", "Plan"),
        ]
    )
