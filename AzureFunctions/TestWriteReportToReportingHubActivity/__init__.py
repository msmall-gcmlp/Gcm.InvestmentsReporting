from .write_report_to_reporting_hub import write_report_to_reporting_hub


def main(requestBody) -> str:
    write_report_to_reporting_hub()
    return "Done"
