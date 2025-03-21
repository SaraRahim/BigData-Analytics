#!/usr/bin/python
# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------


# ------------------------------------------
# IMPORTS
# ------------------------------------------
import sys
import codecs

# ------------------------------------------
# FUNCTION process_line 
# ------------------------------------------
def process_line(line):
    # parsing each line to extract bike_id, total_duration and trip_count
    res = ()

    content = line.strip().split("\t")
    if len(content) == 2:
        # remove parentheses
        values_str = content[1][1:-1] 
        values_parts = values_str.split(", ")

        if len(values_parts) == 3:
            try:
                bike_id = int(values_parts[0])
                total_duration = int(values_parts[1])
                trip_count = int(values_parts[2])
                res = (bike_id, total_duration, trip_count)
            except ValueError:
                pass

    return res

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # inspired by examples from L09-14 Hadoop MapReduce:
    # "my_mapper.py", "my_reducer.py"

    # collecting top_n value defaulting to 10 if not passed
    try:
        top_n_bikes = int(my_reducer_input_parameters[0])
    except:
        top_n_bikes = 10

    # dictionary to accumulate total duration and trips per bike
    my_bike_totals_dict = {}

    for line in my_input_stream:
        parsed_line = process_line(line)

        bike_id, duration, trips = parsed_line
        bike_id = str(bike_id)

        if bike_id not in my_bike_totals_dict:
            my_bike_totals_dict[bike_id] = [0, 0]

        my_bike_totals_dict[bike_id][0] += duration
        my_bike_totals_dict[bike_id][1] += trips

    # sorting bikes by total duration in decreasing order
    my_sorted_bikes = sorted(my_bike_totals_dict.items(), key=lambda x: x[1][0], reverse=True)

    # writing top_n bikes to output file
    for i in range(min(top_n_bikes, len(my_sorted_bikes))):
        bike_id = my_sorted_bikes[i][0]
        total_duration, total_trips = my_sorted_bikes[i][1]
        my_str = bike_id + "\t" + "(" + str(total_duration) + ", " + str(total_trips) + ")" + "\n"
        my_output_stream.write(my_str)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We collect the input values
    my_input_stream = sys.stdin
    my_output_stream = sys.stdout
    my_reducer_input_parameters = [ top_n_bikes ]

    # 5. We call to my_reduce
    my_reduce(my_input_stream,
              my_output_stream,
              my_reducer_input_parameters
             )
