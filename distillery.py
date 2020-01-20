#
# Distillery is a simple and configurable data processing structure.
# Copyright (C) 2019  Claudio Romero
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#

"""Distillery is a simple and configurable data processing structure.

Distillery is a simple (waterfall-like or chain-like) data processing
structure represented by the Distillery class, processing is done in
steps, each step represented by a DistillerNode that contains one or
more data processing processes. Every successive step receives the
output of the previous step as its input, each successive step may also
be optionally sampled through the use of a separate sampling process.

A Distillery is configured through a text file with the names (and
options, if any) of the desired processes (all processes must have
been previously registered).

    Typical usage example:

    d = Distillery()
    data_process = DataProcess()
    d.register_process(data_process.name, data_process)
    sampling_process = SamplingProcess()
    d.register_sampling_process(sampling_process)
    d.config_distilllery("cfg.txt")

    while d.next:
        d.distill(input)

    An example configuration file can be created using:
    d.generate_empty_config_file()
"""


import copy as cp

from distillernode import DistillerNode


class Distillery:
    """Class that represents a 'distillery' data processing structure.

    A distillery object mainly consists of nodes (objects of the
    DistillerNode class), a registry of data processing objects (aka
    processes) and an optional sampling process.

    The distillery object acts as a manager while the nodes, the data
    processing objects and the optional sampling process perform the
    actual data processing.

    A PrimeNode class receives the name of a configuration file and
    all the configuration options. Acting as a factory, the PrimeNode
    reads the configuration file and creates the DistillerNode objects
    that will form the data processing structure and its processes.
    Besides the processing object names (and their options if that is
    the case) the configuration file also indicates wheter the results
    of each process object should or should not be sampled.

    Data processing objects (processes) are expected to contain a 'distill'
    method that receives a single (input) parameter and returns the input
    parameter of the next process (possibly on another node), and a
    sample of the processed data (alternatively 'None' is expected).
    If a data processing object is configurable then it must also
    contain a 'config_process' method that receives a list of str
    containing its options.

    Sampling processes are a somewhat generic form of storing,
    printing and/or logging the results of data processing objects.
    Sampling and data processing objects should be compatible as the
    sample data from the data processing objects is the input for the
    sampling process 'store_sample' method.

    Attributes:
        next (bool): Indicates wheter another iteration of the
            distilling chain is possible.
        nodes (:obj:`list` of :obj: 'DistillerNode'): List of
            DistillerNode objects.
        process_registry(:obj:'dict' of :obj: ?): Dictionary mapping
            a process name to an instance of that process.
        samplig_process(:obj: ?): Sampling process object.
        comment_token(str): Token that identifies that a line in the
            configuration file is a comment.
        separator_token(str): Token that separates the name of
            processes of a node in the configuration file.
        process_opt_start_token(str): Token that identifies the start of
            the optional parameters of a process.
        process_opt_end_token(str): Token that identifies the end of the
            optional paramters of a process.
        process_opt_separator(str): Token that separates the optional
            parameters of a process.
        sampling_token(str): Token that identifies that a process should
        be sampled.
    """
    def __init__(self):
        """Initializes the Distillery object with the default
        configuration
        """
        self.next = False
        self.nodes = []
        self.process_registry = dict()
        self.sampling_process = None

        self.comment_token = "#"
        self.separator_token = ">"
        self.process_opt_start_token = "("
        self.process_opt_end_token = ")"
        self.process_opt_separator = ","
        self.sampling_token = "s"
        self._prime_config()

    def _prime_config(self):
        """Minimal setup before config_distillery method."""
        printer = PrinterProcess()
        self.register_process(printer.name, printer)

    def register_process(self, process_name, process_obj):
        """Registers a data processing object.

        Args:
            process_name(str): Name that identifies a data
                data processing object.
            process_obj(:obj: ?): Data processing object.

        Raises:

            ValueError: Duplicate name in process registry.
        """
        if process_name in self.process_registry:
            raise ValueError("The process registry already has a '" +
                             process_name + "' process")
        else:
            self.process_registry[process_name] = process_obj

    def register_sampling_process(self, sampling_process):
        """Registers a data sampling process.

        Args:
            samplig_process(:obj: ?): Sampling process object.
        """
        self.sampling_process = sampling_process

    def config_distillery(self, config_file):
        """Creates and configures data processing structure

        Creates a PrimeNode object that will read a configuration file
        and create a data processing structure with DistillerNodes and
        is processes. Afterwards the 'reset_distillery' method is called
        so that the Distillery object state is set to be ready to start
        processing data.

        Args:
            config_file(str): String containing the configuration file
                name.
        """
        prime_node = PrimeNode(config_file,
                               self.process_registry,
                               self.sampling_process,
                               self.comment_token,
                               self.separator_token,
                               self.process_opt_start_token,
                               self.process_opt_end_token,
                               self.process_opt_separator,
                               self.sampling_token)
        self.nodes = prime_node.distill()

        self.reset_distillery()

    def reset_distillery(self):
        """Resets the Distillery object state.

        Resets the distillery state so that it is ready to start a new
        batch of data processing. The objects configuration remains
        unchanged.
        """
        if self.sampling_process:
            self.sampling_process.start_sampling_process()
        self.next = True

    def generate_empty_config_file(self):
        """Generates an empty configuration file with helpful comments.

        Generates an empty configuration file with comments on what
        processes are currently known and examples on how to create
        a new configuration file.
        """
        cfg_file = open("cfg.txt", "w+")
        cfg_file.write(self.comment_token + "Example Config file\n")
        cfg_file.write(self.comment_token + " <-- This identifies a comment,"
                       " comments and empty lines will be ignored\n")
        cfg_file.write(self.comment_token + "Registered processes:\n")

        if self.process_registry:
            for process in self.process_registry.values():
                cfg_file.write(self.comment_token + process.name + ": " +
                               process.help + " \n")

        cfg_file.write(self.comment_token + "\n")
        cfg_file.write(self.comment_token + "Recognized separator '" +
                       self.separator_token + "'\n")

        cfg_file.write(self.comment_token + "\n")
        cfg_file.write(self.comment_token + "\n")

        cfg_file.write(self.comment_token + "Example (replace text below with"
                       " registered processes):\n\n")
        process_list = list(self.process_registry.values())
        example_process = process_list[0]

        cfg_file.write(example_process.name + "\n")
        cfg_file.write(example_process.name + "\n")
        cfg_file.write(example_process.name + self.separator_token +
                       example_process.name + "\n")  # process>process
        cfg_file.write(example_process.name + "\n")

        cfg_file.close()

    def _print_config(self):
        """Prints a representation of the data processing structure."""
        if self.nodes:
            print("   O")
            self.nodes[0].print_node()

    def distill(self, input_obj):
        """Starts a single data processing iteration.

        Starts a single data processing iteration, afterwards it
        signals to the sampling process (if one is registered) if the
        data processing can possibly continue or if no more data is
        available at the moment.

        Args:
            input_obj(:obj: ?): Object compatible with the registered
                data processing objects.

        Returns:
            Returns the output of the last processed node.
        """
        if self.nodes and self.next:
            results = self.nodes[0].distill_node(input_obj)
            if results is None:
                self.next = False
            else:
                self.next = True
            if self.sampling_process:
                if self.next:
                    self.sampling_process.continue_sampling_process()
                else:
                    self.sampling_process.stop_sampling_process()
        return results # Este método deveria retornar self.next?


#Incluir isso como método da classe Distillery.
class PrimeNode:
    def __init__(self,
                 config_file,
                 process_registry,
                 sampling_process,
                 comment_token,
                 separator_token,
                 process_opt_start_token,
                 process_opt_end_token,
                 process_opt_separator,
                 sampling_token):
        self.nodes = []
        self.cfg_file = open(config_file, "r")
        self.process_registry = process_registry
        self.sampling_process = sampling_process
        self.eof = False

        self.comment_token = comment_token
        self.separator_token = separator_token
        self.opt_start_token = process_opt_start_token
        self.opt_end_token = process_opt_end_token
        self.opt_separator = process_opt_separator
        self.sampling_token = sampling_token
        self.repeat_exec_token = "r"
        self.once_exec_token = "o"

    def _node_opt_sanity_check(self, node_opt):
        repeat_count = node_opt.count(self.repeat_exec_token)
        once_count = node_opt.count(self.once_exec_token)
        sample_count = node_opt.count(self.sampling_token)

        node_cfg_ok = True
        if repeat_count > 1:
            node_cfg_ok = False
        if once_count > 1:
            node_cfg_ok = False
        if sample_count > 1:
            node_cfg_ok = False
        if (repeat_count != 1) and (once_count != 1):
            node_cfg_ok = False

        return node_cfg_ok

    def _split_proc_cfg(self, proc_cfg):
        index = 0
        char = proc_cfg[index]
        while not char.isupper():
            index += 1
            char = proc_cfg[index]

        node_opt = proc_cfg[:index]
        process = proc_cfg[index:].rstrip() # Removes '\n'
        return node_opt, process

    def _read_node_opt(self, node_opt):
        digit_index = 0
        char = node_opt[digit_index]
        while char.isdigit():
            digit_index += 1
            char = node_opt[digit_index]

        opt_num = 1 # If no number is given then 1 is assumed.
        if digit_index > 0:
            opt_num = int(node_opt[0:digit_index])

        exec_opt = node_opt[digit_index:]

        limited = None
        if self.repeat_exec_token in exec_opt:
            limited = False
        elif self.once_exec_token in exec_opt:
            limited = True

        sample_flag = False
        if self.sampling_token in exec_opt:
            sample_flag = True

        return opt_num, limited, sample_flag

    def _setup_process(self, node, proc_cfg):
        node_opt, process = self._split_proc_cfg(proc_cfg)

        if not self._node_opt_sanity_check(node_opt):
            raise ValueError("Invalid node configuration: " + proc_cfg)

        opt_num, limited, sample_flag = self._read_node_opt(node_opt)

        process_opt = None
        if self.opt_start_token in process:
            temp = process.split(self.opt_start_token, 1)
            proc_name = temp[0]
            temp = temp[1].replace(self.opt_end_token, "")

            process_opt = temp.split(self.opt_separator)
        else:
            proc_name = process

        proc = self.process_registry.get(proc_name)
        if proc is not None:

            if process_opt:
                proc1 = cp.copy(proc)
                proc1.config_process(process_opt)
                proc = proc1

            node.add_process(proc, opt_num, limited, sample_flag)
        else:
            raise ValueError("Process (" + proc_cfg + ") not found in registry")

    def distill(self):
        line = self.cfg_file.readline()
        eof = not bool(line)
        prev_node = None

        while not eof:
            if line.startswith(self.comment_token):
                continue

            processes = line.split(self.separator_token)
            if processes:
                node = DistillerNode()
                for proc_cfg in processes:
                    self._setup_process(node, proc_cfg)

                node.add_sampling_process(self.sampling_process)
                if prev_node:
                    prev_node.add_node(node)

                prev_node = node
                self.nodes.append(node)

            line = self.cfg_file.readline()
            eof = not bool(line)

        return self.nodes


class PrinterProcess:
    def __init__(self):
        self.help = "Prints extracted data"
        self.name = "dPRINT"

    def distill(self, process_input):
        print(process_input)
        return input, None
 
