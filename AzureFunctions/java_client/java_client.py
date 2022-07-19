import logging
import os
import jpype
import jdk

_JRE_PATH = os.path.join(os.path.expanduser('~'), '.jre')
_JRE_VERSION = '18'


def install_jre():
    jre_version_path = _get_jre_version_path()

    if jre_version_path is None:
        logging.info(f'Could not find JRE {_JRE_VERSION}. Installing it now')
        jre_version_path = jdk.install(_JRE_VERSION, jre=True)
    else:
        logging.info(f'Using the existing JRE {_JRE_VERSION} at {jre_version_path}')

    logging.info('Setting JAVA_HOME and PATH environment variables')

    os.environ['JAVA_HOME'] = jre_version_path
    os.environ['PATH'] += os.pathsep + jre_version_path + os.pathsep + 'bin'


def start_jvm():
    if not jpype.isJVMStarted():
        logging.info('Starting JVM')
        jpype.startJVM()
    else:
        logging.info('JVM is already running')


def _get_jre_version_path():
    if not os.path.exists(_JRE_PATH):
        return None

    version_substring = f"jdk-{_JRE_VERSION}"

    for v in os.listdir(_JRE_PATH):
        if version_substring in v:
            return os.path.join(_JRE_PATH, v)

    return None
