import logging
import os
import jpype
import jdk


def install_jre():
    logging.info('Starting JRE install')
    
    if 'JAVA_HOME' in os.environ:
        logging.info('Skipping JRE install. JRE is already set up')
        return

    jre_path = os.path.join(os.path.expanduser('~'), '.jre')
    java_home_path = None

    if os.path.exists(jre_path):
        logging.info('Using the existing JRE')
        jre_version = os.listdir(jre_path)[0]
        java_home_path = os.path.join(jre_path, jre_version)
    else:
        logging.info('Installing JRE')
        java_home_path = jdk.install('18', jre=True)
    
    logging.info('Setting JAVA_HOME and PATH environment variables')

    os.environ['JAVA_HOME'] = java_home_path
    os.environ['PATH'] += os.pathsep + java_home_path + os.pathsep + 'bin'


def start_jvm():
    if not jpype.isJVMStarted():
        logging.info('Starting JVM')
        jpype.startJVM()
    else:
        logging.info('JVM is already running')
