import logging


def main(coolParameter: str) -> str:
    fileName = __name__.split('.')[1]
    logging.info(f"Activity: {fileName} will start")

    response = f"This is an extremely cool activity called {fileName}"
    if coolParameter:
        response += f". And it received a parameter: {coolParameter}"

    logging.info(f"Activity: {fileName} executed successfully")
    return response
