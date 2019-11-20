#
# Data sampling processes compatible with the 'DistillerNode' class.
# Copyright (C) 2019  Claudio Romero
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#

"""Data sampling processes compatible with the 'DistillerNode' class.

This module defines classes that represent sampling processes as used
by the 'Distillery' and 'DistillerNode' classes, all classes contain
the necessary 'start_sampling_process', 'continue_sampling_process',
'stop_sampling_process' and 'store_sample' methods.

The 'start_sampling_process', 'continue_sampling_process',
'stop_sampling_process' methods are used to signal to the sampling
process the current stage it should executing:

    'start_sampling_process': Initial configuration.
    'continue_sampling_process': Batch complete, more data available.
    'stop_sampling_process': Batch complete. no more data available.

Sampling processes receive 'samples' objects of undefined type and
thus should be carefully chosen to ensure compatibility with the
processes that generate them.

    Typical usage example:

    d = Distillery()
    sampling_process = Sample2Text("filename")
    d.register_sampling_process(sampling_process)

    # Distillery will manage start, stop and contine.
    # DistillerNode will call the 'store_sample' method.
"""

import sqlite3


class Sample2Text:
    """Class that stores data in a text file.

    A Sample2Text object is sampling process compatible with the
    'DistillerNode' and 'Distillery' classes, it simply receives data
    in 'str', 'list' or 'dict' objects and writes them do a text file.

    Attributes:
        help: String that contains a short help text.
        name: String with the name that identifies this process.
        file_name: String containing the output text file name.
        text_file: File handle object.
        write_count: Flag that indicates wheter to print the current
            sample count.
        sample_count: Number of sampled objects.
        distill_count: Number of times the sampling process was
            signaled to continue, this correlates to the number of
            times the distill process was executed by a 'Distillery'
            object.
        last_distill_count: Auxiliary variable used to detect that a
            the sampling process is now sampling another distill
            batch from a 'Distillery' object.
    """
    def __init__(self, file_name, write_count=True):
        self.help = "Prints sample data to text file"
        self.name = "Sample2Text"
        self.file_name = file_name
        self.text_file = None
        self.write_count = write_count
        self.sample_count = 0
        self.distill_count = 0
        self.last_distill_count = 0

    def start_sampling_process(self):
        """Initial configuration of the sampling process.

        This method simply sets the output file handle, the file is
        opened so that data is appended to it.
        """
        self.text_file = open(self.file_name, "w+")

    def continue_sampling_process(self):
        """Signals a 'pause' to the sampling process.

        This method signals a pause to the sampling process, this
        indicates that a 'distill' batch by a 'Distillery' object is
        completed and that more sampling is possible. The
        'last_distill_count' is set so that the 'store_sample' method
        can detect that a new batch has started.
        """
        self.last_distill_count = self.distill_count
        self.text_file.write("\n")

    def stop_sampling_process(self):
        """Post-sampling cleanup of the sampling process.

        This method simply closes the output file handle.
        """
        self.text_file.close()

    def store_sample(self, samples):
        """Receives samples and stores them to the output text file.

        This method receives a 'samples' object and writes its
        contents to the output text file, each 'samples' object os
        treated according to its type ('str', 'list', 'dict' and
        evenrythig else).

        If the 'write_count' flag is set then between each batch
        a separator is printed along with the number o batches and
        the number of sampled objects.

        Args:
            samples: Object containing the samples.
        """
        if self.text_file and samples:

            if self.last_distill_count == self.distill_count:
                self.distill_count += 1
                if self.write_count:
                    self.text_file.write("Registro " + str(self.distill_count) +
                                         " (" + str(self.sample_count) +
                                         " amostras acumuladas)\n")
                    self.text_file.write("--------------------------------------------------------\n")

            self.sample_count += 1
            if isinstance(samples, str):
                self.text_file.write(samples)
            elif isinstance(samples, list):
                for i in samples:
                    self.text_file.write(i)
                    self.text_file.write("\t")
            elif isinstance(samples, dict):
                for key, value in samples.items():
                    self.text_file.write(key)
                    self.text_file.write(": ")
                    self.text_file.write(value)
                    self.text_file.write("\t")
            else:
                self.text_file.write(samples)

            self.text_file.write("\n")


class Sample2ObjectList:
    """Class that stores data into an list of objects.

    A Sample2ObjectList object is sampling process compatible with the
    'DistillerNode' and 'Distillery' classes, it simply receives a
    'samples' object and stores it to a list.

    Attributes:
        help: String that contains a short help text.
        name: String with the name that identifies this process.
        object_list: List of samples objects.
    """

    def __init__(self):
        self.help = "Stores sampled data in a list of python objects."
        self.name = "Sample2ObjectList"
        self.object_list = list()

    def start_sampling_process(self):
        """Initial configuration of the sampling process.

        No initial configuration needed.
        """
        pass

    def continue_sampling_process(self):
        """Signals a 'pause' to the sampling process.

        This method signals a pause to the sampling process, this
        indicates that a 'distill' batch by a 'Distillery' object is
        completed and that more sampling is possible. No special
        behaviour is implemented.
        """
        pass

    def stop_sampling_process(self):
        """Post-sampling cleanup of the sampling process.

        No post-sampling cleanup required.
        """
        pass

    def store_sample(self, samples):
        """Receives samples and stores in the object_list.

        This method receives a 'samples' object and stores its
        contents in the 'object_list attribute'.

        Args:
            samples: Object containing the samples.
        """
        if samples:
            self.object_list.append(samples)

    def get_list(self):
        return self.object_list

    def clear_list(self):
        self.object_list.clear()

class Sample2Table:
    """Class that stores data in table format without persistance.

    A Sample2Table object is sampling process compatible with the
    'DistillerNode' and 'Distillery' classes. This class is only
    compatible with samples as dictionary objects. Data is stored
    as a dictionary of 'names-to-lists'.

    As an option this class can treat repeated column names by
    appending a number after each repeated name, this only works
    if the repeated columns are always presented in the same order
    in each batch (and never in the same sample).

    Attributes:
        help: String that contains a short help text.
        name: String with the name that identifies this process.
        table: Dictionary containing the data in 'table' format.
        current_index: Indicates current batch number.
        previous_index: Auxiliary variable used to determine if
            the current sample is the first one of a batch.
        sampled_names: Set object that stores columns (keys) have
            already been sampled in this batch.
        fix_repeated_names: Flag that indicates wheter the sampling
            process should attempt to fix/replace repeated column
            names.
    """
    def __init__(self, fix_repeated_names=False):
        self.help = "Stores data in table format."
        self.name = "Sample2Table"
        self.table = dict()
        self.current_index = 0
        self.previous_index = None
        self.sampled_names = set()
        self.fix_repeated_names = fix_repeated_names

    def start_sampling_process(self):
        self.table.clear()
        self.current_index = 0
        self.previous_index = None
        self.sampled_names.clear()

    def continue_sampling_process(self):
        self.previous_index = self.current_index

    def stop_sampling_process(self):
        pass

    def store_sample(self, samples):
        if samples and isinstance(samples, dict):

            if self.previous_index == self.current_index:
                self.current_index += 1
                self.sampled_names.clear()

            for key, value in samples.items():
                if key in self.table:
                    self._append_sample(key, value)
                else:
                    self.table[key] = [(self.current_index, value)]
                    self.sampled_names.add(key)
        else:
            if not isinstance(samples, dict):
                raise ValueError(self.name + " process accepts only dictionaries as samples")

    def _append_sample(self, key, value):
        temp_key = key
        if self.fix_repeated_names and temp_key in self.sampled_names:
            fix_count = 1
            temp_key = key + "-" + str(fix_count)

            # Check if temp_key already was sampled and propose a new name
            while temp_key in self.sampled_names:
                temp_key = key + "-" + str(fix_count)
                fix_count += 1

            if temp_key not in self.table:
                self.table[temp_key] = []  # Initializes with fixed name.

        indexed_list = self.table[temp_key]
        indexed_list.append((self.current_index, value))
        self.sampled_names.add(temp_key)


class Sample2XSV(Sample2Table):
    def __init__(self, file_name, data_separator, fix_repeated_names=False):
        super().__init__(fix_repeated_names)
        self.help = "Prints data in \"X\" separated value text file"
        self.name = "Sample2XSV"
        self.file_name = file_name
        self.text_file = None
        self.data_separator = data_separator
        self.header_already_written = False

    def stop_sampling_process(self):
        self.text_file = open(self.file_name, "a")

        if not self.header_already_written:
            header = self._create_header()
            self.text_file.write(header + "\n")
            self.header_already_written = True

        for i in range(0, self.current_index):
            table_line = ""
            for key, indexed_list in self.table.items():
                del key
                index, data = indexed_list[0]

                if index == i:
                    table_line += str(data) + self.data_separator
                else:
                    table_line += "-" + self.data_separator
                indexed_list.pop(0)

            self.text_file.write(table_line + "\n")

        self.text_file.close()

    def _create_header(self):
        header = ""
        for key in self.table:
            if header:
                header += "\t" + key
            else:
                header = key
        return header


class Sample2SQLite(Sample2Table):
    def __init__(self, db_name, table_name, fix_repeated_names=False):
        super().__init__(fix_repeated_names)
        self.db_name = db_name
        self.table_name = table_name
        self.connection = None

    def start_sampling_process(self):
        super().start_sampling_process()
        self.connection = sqlite3.connect(self.db_name)
        if not self.connection:
            raise ValueError(self.db_name + " connection failed")
        # self.connection.set_trace_callback(print)

    def continue_sampling_process(self):
        self.previous_index = self.current_index

    def stop_sampling_process(self):
        try:
            with self.connection:
                self._create_table(self.table_name)
                self._insert_data(self.table_name,
                                  self.table,
                                  self.current_index)

        except sqlite3.OperationalError as exception:
            print("OperationalError")
            print(exception)
        except sqlite3.IntegrityError as exception:
            print("IntegrityError")
            print(exception)
        except sqlite3.ProgrammingError as exception:
            print("ProgrammingError")
            print(exception)
        finally:
            self.connection.close()

    def _create_table(self, table_name):
        if not self._table_exists():
            column_names = "','".join(self.table.keys())
            query = "CREATE TABLE '{}' ('{}')".format(table_name, column_names)
            # print(query)
            self.connection.execute(query)

    def _table_exists(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=:t_name", {"t_name": self.table_name})
        table_exists = bool(cursor.fetchone())
        return table_exists

    def _insert_data(self, table_name, data_dict, index_count):
        self.connection.execute("BEGIN")
        for i in range(0, index_count):
            key_list = []
            value_list = []

            for key, indexed_list in data_dict.items():
                index, data = indexed_list[0]
                if index == i:
                    key_list.append(key)
                    value_list.append(data)

                indexed_list.pop(0)

            placeholder = "?"
            placeholder_num = len(value_list)
            placeholder += ",?" * (placeholder_num-1)  # ?,?,?,?, ... ,?

            columns = "','".join(key_list)

            if not columns:
                print(data_dict)

            query = "INSERT INTO '{}' ('{}') VALUES ({})".format(table_name,
                                                                 columns,
                                                                 placeholder)
            # print(query)
            self.connection.execute(query, value_list)
        self.connection.execute("COMMIT")
