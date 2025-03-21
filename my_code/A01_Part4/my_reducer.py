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
    # parsing each line to extract station name and trip counts
    res = ()

    content = line.strip().split("\t")
    if len(content) == 2:
        station_name = content[0]
        # remove parentheses
        counts_str = content[1][1:-1]  
        counts_parts = counts_str.split(", ")

        if len(counts_parts) == 2:
            try:
                start_count = int(counts_parts[0])
                end_count = int(counts_parts[1])
                res = (station_name, (start_count, end_count))
            except ValueError:
                pass

    return res

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # inspired by examples from L09-14 Hadoop MapReduce:
    # "my_sequential_approach.py", "my_mapper.py", "my_reducer.py"

    # using a dictionary to store trip counts per station
    my_stations_dict = {}

    # traversing the lines in the input stream and extracting data
    for line in my_input_stream:
        parsed_line = process_line(line)

        if parsed_line:
            station_name, counts = parsed_line

            # initializing dictionary entry if station not yet seen
            if station_name not in my_stations_dict:
                my_stations_dict[station_name] = [0, 0]

            # updating the start and end trip counts
            my_stations_dict[station_name][0] += counts[0]
            my_stations_dict[station_name][1] += counts[1]

    # sorting station names alphabetically
    my_sorted_stations = sorted(my_stations_dict.keys())

    # writing the results to the output file
    for station_name in my_sorted_stations:
        counts = my_stations_dict[station_name]
        my_str = station_name + "\t" + "(" + str(counts[0]) + ", " + str(counts[1]) + ")" + "\n"
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
    my_reducer_input_parameters = []

    # 5. We call to my_reduce
    my_reduce(my_input_stream,
              my_output_stream,
              my_reducer_input_parameters
             )
