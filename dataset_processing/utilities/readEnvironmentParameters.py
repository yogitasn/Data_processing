from occupancy_processing import miscProcess

from datetime import datetime, timedelta
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, StringType
import re
import time
import os

SCRIPT_NAME = os.path.basename(__file__)

COMMENT_CHAR = "#"
COMMENT_DATE = datetime.now().strftime("%Y-%m-%d")


def global_SQLContext(SparkSession1):
    global SparkSession
    SparkSession = SparkSession1


def remove_whitespace(inParam, accepted_list="[^0-9a-zA-Z/@()_\-. ]+"):
    outParam = re.sub(accepted_list, "", inParam)
    return outParam


# Parse Parameter files
def parse_config(caller_function, filename, option_char="="):
    ReturnCode = 0
    OPTION_CHAR = option_char
    options = {}
    param_list = caller_function + "\n"

    f = open(filename)

    for line in f:
        # Ignore Empty lines
        if not line.strip():
            continue
        # First, remove comments:
        if COMMENT_CHAR in line:
            # if first char is '#' on the line, skip
            strip_line = line.strip()
            if strip_line[0] == "#":
                continue
            # split on comment char, keep on the part before
            line, comment = line.split(COMMENT_CHAR, 1)
            line += "\n"

        # Second, find lines with an option = value
        if OPTION_CHAR in line:
            param_list += "{}".format(line)
            # spliy on option char
            option, value = line.split(OPTION_CHAR, 1)
            # strip spaces:

            option = option.strip()
            value = value.strip()

            value = remove_whitespace(value)
            options[option] = value

        else:
            miscProcess.log_error(SCRIPT_NAME, "ERROR: WRONG PARAMETER ASSIGNMENT ON LINE: {}".format(line.strip()), 1)
            ReturnCode = 1
            break

    f.close()
    miscProcess.log_info(SCRIPT_NAME, param_list)
    return options, ReturnCode


def read_job_control(paramFile):
    param, ReturnCode = parse_config("Job Control Parameters", paramFile, "=")
    globals().update(param)
    return param, ReturnCode


def read_runtime_control(paramFile):
    param, ReturnCode = parse_config("Runtime Tracker Parameters", paramFile, "=")
    globals().update(param)
    return param, ReturnCode


def main(paramFile):

    paramFile, ReturnCode = read_job_control(paramFile)


if __name__ == "__main__":
    log_file = "test.log"
    miscProcess.initial_log_file(logfile)
    main(sys.argv[1])
