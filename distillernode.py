 #
# Simple (waterfall-like or chain-like data processing object.
# Copyright (C) 2019  Claudio Romero
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#

"""A DistillerNode represents a data processing step.

A DistillerNode is a simple (waterfall-like or chain-like) data
processing object that receives an input and starts a chain of data
processing, the output of each process is passed on to the next process
as input and after all processes are executed then the last output is
passed to the next DistillerNode (if any exists).

    Typical usage example:

    process = ExampleProcess()
    sampling_process = SamplingProcess()
    node = DistillerNode()
    sample_process = True or False
    node.add_process(process, sample_process)
    node.add_sampling_process(self.sampling_process)
    next_node = DistillerNode()
    node.add_node(next_node)

    node.distill_node(input)
"""

class DistillerNode:
    """Class that represents a single node of a distilling process.

    A DistillerNode object is comprised of a list of processes (data
    processing objects), a separate list of which processes should be
    sampled, a sampling process object (that receives the samples
    generated be each process) and a 'pointer' to the next DistillerNode
    (if any exists).

    Attributes:
        node_process_list: List of tuples data processing objects and
            a sampling flag.
        next_node: 'Pointer' to the next DistillerNode in the data
            processing chain.
    """
    def __init__(self):
        """Initializes the DistillerNode object with an empty
        configuration
        """
        self.node_process_list = []
        self.sampling_process = None
        self.next_node = None

        self.exec_count = 0

    def add_process(self, process, opt_num, limited, sample_flag):
        """Appends the node_process_list of tuples.

        Args:
            process: Data processing object.
            opt_num: Number of times a process should be repeated
                before data is passed to the next process, or
                total number of time a process should be executed
                and then skipped.
            limited: Indicates wheter this process has a limited
            number of executions or is a 'unlimited' process.
            sample_flag: boolean that indicates wheter a
                process should be sampled.
        """
        node_process_tuple = (process, opt_num, limited, sample_flag)
        self.node_process_list.append(node_process_tuple)

    def add_sampling_process(self, sampling_process):
        """Stores sampling process

        Args:
            sampling_process: Instance of a sampling process object.
        """
        self.sampling_process = sampling_process

    def add_node(self, node):
        """Store the 'pointer' for the next node

        Args:
            node: Instance of another DistillerNode object.
        """
        self.next_node = node

    def print_node(self):
        """Prints a visual representation of this node's processes.

        Prints to console a simple visual representation of this node's
        processes, this representation is similar to the one written on
        the configuration file.
        """
        print("   |")
        print("   |")
        print("   V")
        if self.node_process_list:
            process, *opt = self.node_process_list[0]
            del opt
            str2print = process.name
            for process, *opt in range(1, len(self.node_process_list)):
                str2print += " > " + process.name

            print(str2print)

        if self.next_node:
            self.next_node.print_node()

    def distill_node(self, node_input):
        """Executes the distill method of every process object.

        Executes the distill method of every process object, after
        every execution the samples variable return by each process
        (processes may return None) is passed to the sampling process
        (if the respective sampling flag is TRUE). The output of each
        process becomes the next process's  input.

        After all processes have been executed then the next_node, if
        any exist, 'distill_node' method is called.

        Args:
            node_input: Data source that should be compatible with the
                registered processes.

        Returns:
            The output from the last executed process, this varies with
            the input type and processes being used, if the processing
            is done in batches then typically the output should be what
            will be used as input in the next batch. If 'None' is
            returned by any process then the processing is interrupted
            and the output will also be 'None'.
        """
        node_output = None
        if self.node_process_list:
            for process, opt_num, limited, sample_flag in self.node_process_list:

                exec_count_reached = self.exec_count >= opt_num

                if (node_input is None) or (limited and exec_count_reached):
                    break

                for i in range(0, opt_num):
                    del i
                    node_input, samples = process.distill(node_input)

                    if node_input is None:
                        break

                    if self.sampling_process and sample_flag:
                        self.sampling_process.store_sample(samples)

            self.exec_count += 1

            if node_input and self.next_node:
                node_output = self.next_node.distill_node(node_input)
            else:
                node_output = node_input

        return node_output
