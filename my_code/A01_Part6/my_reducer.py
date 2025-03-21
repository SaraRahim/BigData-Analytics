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
    # parsing trips grouped per day into individual trip tuples
    res = []

    content = line.strip().split("\t")
    if len(content) == 2:
        # remove parentheses
        trips_str = content[1][1:-1]  
        trips_parts = trips_str.split(" @ ")

        if len(trips_parts) % 4 == 0:
            for idx in range(0, len(trips_parts), 4):
                trip = (
                    trips_parts[idx],
                    trips_parts[idx + 1],
                    trips_parts[idx + 2],
                    trips_parts[idx + 3]
                )
                res.append(trip)
        else:
            res = None

    return res

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    # inspired by examples from L09-14 Hadoop MapReduce:
    # "my_mapper.py", "my_reducer.py"

    # list to store all parsed trips from all days
    my_trips_list = []

    # read and flatten the input trips
    for line in my_input_stream:
        parsed_trips = process_line(line)

        if parsed_trips:
            my_trips_list.extend(parsed_trips)

    # sort trips by start_time to detect transitions
    my_trips_list.sort(key=lambda trip: trip[0])

    # compare each trip with the next one to detect truck moves
    for index in range(len(my_trips_list) - 1):
        current_trip = my_trips_list[index]
        next_trip = my_trips_list[index + 1]

        if current_trip[3] != next_trip[2]:
            my_str = "By_Truck" + "\t(" + current_trip[1] + ", " + current_trip[3] + ", " + next_trip[0] + ", " + next_trip[2] + ")\n"
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
