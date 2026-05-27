import schemathesis


@schemathesis.deserializer("text/csv")
def deserialize_csv(_ctx, response):
    return response.text


@schemathesis.deserializer("application/vnd.apache.parquet")
def deserialize_parquet(_ctx, response):
    return response.content.decode("latin1")
