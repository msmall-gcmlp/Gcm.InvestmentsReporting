from .get_source_files import get_file_streams, save_file_streams


def main(requestBody) -> str:
    params = requestBody["params"]
    streams = get_file_streams(params)
    save_file_streams(params, streams)
    return "Done"
