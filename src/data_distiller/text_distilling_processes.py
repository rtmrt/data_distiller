#
# Text processing classes compatible with the 'DistillerNode' class.
# Copyright (C) 2019  Claudio Romero
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#

"""Text processing classes compatible with the 'DistillerNode' class.

This module contains text processing classes and methods. All data
processing classes contain the 'distill' method and also the 'help' and
'name' attributes (making them compatible with the 'Distillery' and
'DistillerNode' classes). Some classes also represent configurable
processes and as such also contain the optional 'config_process'
method.

"""


import re

#from data_distiller.options import OptType, OptManager
from liboptions import OptType, OptManager

def read_text(input_file):
    """Simple method that reads whitespace separated text fragment.

    This method receives and opened input file and reads it character
    by character until it finds a whitespaces or fails to read a new
    character.

    Args:
        inputFile: Alreadey opened text file.

    Returns
        A string of read characters (word).
    """
    word = ''

    while True:
        char = input_file.read(1)
        # pylint: disable=no-else-return
        if (char.isspace()) or (char == ''):
            return word
        else:
            word += char
    return word


class PrintText:
    """Reads and prints to console whitespace separeted text.

    The PrintText class simply reads text a whitespace separated
    text fragment and prints it to the console.

    Attributes:
        help: String that contains a short help text.
        name: String with the name that identifies this process.
    """
    def __init__(self):
        self.help = "Prints extracted data"
        self.name = "PRINTTEXT"
        self.usage = self.name

    def distill(self, process_input):
        """Reads and prints whitespace separated text.

        Args:
            input: File handle for a text file.
        Returns:
            The input file handle and a 'None' as a sample.
        """
        text = read_text(process_input)
        print(text)
        return process_input, None

class PrintLine:
    """Reads and prints to console a complete line of text.

    The PrintLine class simply reads a text line and prints it to the
    console.

     Attributes:
        help: String that contains a short help text.
        name: String with the name that identifies this process.
    """
    def __init__(self):
        self.help = "Reads and prints a line of text"
        self.name = "PRINTLINE"
        self.usage = self.name

    def distill(self, process_input):
        """Reads and prints to the console a complete line of text.

        Args:
            input: File handle for a text file.
        Returns:
            The input file handle and a 'None' as a sample.
        """
        line = process_input.readline()
        if line:
            print(line)
        else:
            process_input = None
        return process_input, None


class ReadLine:
    """Reads and samples a complete line of text.

    The ReadLine class reads a text line, strips it of any trailing
    whitespaces and samples it. The resulting sample depends on
    'sample_name' (False: str, True: dict).

    Attributes:
        help: String that contains a short help text.
        name: String with the name that identifies this process.
        sample_name: Optional name for the lines read by this process.
        strip_new_line_only: Optional flag that determines if all
            trailing whitespaces are removed or only '\n'.
        _opt_mngr: Object of the OptManager class, stores optional
            argument names and their types, aswell as providing methods
            to process and check these optional arguments.
    """

    _SAMPLE_NAME = "sample_name"
    _STRIP_NEW_LINE_ONLY = "strip_new_line_only"

    def __init__(self, sample_name=None, strip_new_line_only=False):
        super().__init__()
        self.name = "READLINE"
        self.help = "Reads a line of text"
        self.sample_name = sample_name
        self.strip_new_line_only = strip_new_line_only

        self._opt_mngr = OptManager()
        self._opt_mngr.register_opt(self._SAMPLE_NAME, OptType.STRING, False)
        self._opt_mngr.register_opt(self._STRIP_NEW_LINE_ONLY,
                                   OptType.BOOL,
                                   False)

        self.usage = self._opt_mngr.usage()

    def config_process(self, opt_dict):
        """Configures the 'sample_name' and sample format.

        This method uses the 'self._opt_mngr' object to process the
        received 'opt_dict', the resulting 'processed_opt' dictionary
        contains the options in an already compatible format. With the
        options already processed the 'sample_name' and/or
        'strip_new_line_only' attributes are set.
        if 'sample_name' is set then each line read will be sampled using a
        dictionary using the 'sample_name' attribute as key.

        Args:
            opt_dict: Dict with at least one of the possible options.
        """
        processed_opt = self._opt_mngr.process_dict(opt_dict)

        if self._SAMPLE_NAME in processed_opt:
            self.sample_name = processed_opt[self._SAMPLE_NAME]

        if self._STRIP_NEW_LINE_ONLY in processed_opt:
            self.strip_new_line_only = processed_opt[self._STRIP_NEW_LINE_ONLY]

    def distill(self, process_input):
        """Reads a text line from file handle.

        Args:
            input: File handle for a text file.

        Returns:
            The input file handle or 'None' if no data has been read.
            The read line stripped of all trailing whitespaces or
            only new line ('\n'). If a
            'sample_name' is provided then the line is returned inside
            a dictionary.
        """
        line_data = process_input.readline()

        if not line_data:
            process_input = None
        else:
            if self.strip_new_line_only:
                line_data = line_data.rstrip('\n')
            else:
                line_data = line_data.rstrip()
            if self.sample_name:
                line_data = {self.sample_name: line_data}

        return process_input, line_data


class SkipLine:
    """Reads a complete line of text from a file handle, no sampling.

    The SkipLine class reads a text line simply reads a text line, no
    sampling is performed. This is used to literally skip an undesired
    line in a text fiel.

    Attributes:
        help: String that contains a short help text.
        name: String with the name that identifies this process.
    """
    def __init__(self):
        self.help = "Skips a single line"
        self.name = "SKIPLINE"
        self.usage = self.name

    def distill(self, process_input):
        """Reads a text line from file handle.

        Args:
            input: File handle for a text file.

        Returns:
            The input file handle or 'None' if no data has been read.
            Sample is always 'None'.
        """
        if not process_input.readline():
            process_input = None
        return process_input, None

class SkipUntilToken:
    """Reads lines until it finds one containing a specific token.

    The SkipUntilToken class reads text lines until it finds one that
    contains a specific token, no sampling is performed.

    Attributes:
        help: String containing a short help text.
        name: String containing the name that identifies this process.
        token: String token that identifies last line to be skipped.
        _opt_mngr: Object of the OptManager class, stores optional
            argument names and their types, aswell as providing methods
            to process and check these optional arguments.
    """

    _TOKEN = "token"

    def __init__(self, token=None):
        super().__init__()
        self.name = "SKIPUNTIL"
        self.help = "Skips lines until it finds a specific token"
        self.token = token

        self._opt_mngr = OptManager()
        self._opt_mngr.register_opt(self._TOKEN, OptType.STRING, True)

    def config_process(self, opt_dict):
        """Configures the skip until 'token'.

        This method uses the 'self._opt_mngr' object to process the
        received 'opt_dict', the resulting 'processed_opt' dictionary
        contains the options in an already compatible format. With the
        options already processed the 'token' attribute is set.
        The 'token' attribute is used to identify the last line to be
        read by this process.

        Args:
            opt_Dict: Dict that contains the 'token' option.
        """
        processed_opt = self._opt_mngr.process_dict(opt_dict)
        self.token = processed_opt[self._TOKEN]

    def distill(self, process_input):
        """Reads lines until it finds one containing a specific token.

        Args:
            input: File handle for a text file.

        Returns:
            The input file handle or 'None' if no data has been read.
            Sample is always 'None'.
        """
        token_found = False
        line = process_input.readline()
        eof = not bool(line)

        while (not eof) and (not token_found):
            if self.token in line:
                token_found = True
            else:
                line = process_input.readline()
                eof = not bool(line)

        if eof:
            process_input = None

        return process_input, None

class RegexRead:
    """Reads a line of text and extract text using regular expressions.

    The RegexRead class reads a text line, strips it of all
    whitespaces then extracts text using. Optionally this 
    class will continue reading lines until a stop condition
    is provided.

    Attributes:
        help: String containing a short help text.
        name: String containing the name that identifies this process.
        regex: String containing the regular expression.
        stop_token: Optional string containing a stop condition.
        _opt_mngr: Object of the OptManager class, stores optional
            argument names and their types, aswell as providing methods
            to process and check these optional arguments.
    """

    _REGEX = "regex"
    _STOP_ON_MATCH = "stop_on_match"
    _STOP_TOKEN = "stop_token"

    def __init__(self, regex=None, stop_on_match=False, stop_token=None):
        super().__init__()
        self.name = "REGEXREAD"
        self.help = "Reads text using Regular Expressions"

        self._opt_mngr = OptManager()
        self._usage = self._register_options()

        self.regex = regex
        self.stop_on_match = stop_on_match
        self.stop_token = stop_token

        self.result_data = None

    def _register_options(self):
        """Registers the expected options used by this process.

        This method registers the expected names and types for options available
        for this process.
        """
        self._opt_mngr.register_opt(self._REGEX, OptType.STRING, False)
        self._opt_mngr.register_opt(self._STOP_ON_MATCH, OptType.BOOL, False)
        self._opt_mngr.register_opt(self._STOP_TOKEN, OptType.STRING, False)

        return self._opt_mngr.usage()

    def config_process(self, opt_dict):
        """Configures the 'regex' variable.

        This method uses the 'self._opt_mngr' object to process the
        received 'opt_dict', the resulting 'processed_opt' dictionary
        contains the options in an already compatible format. With the
        options already processed the 'regex', 'stop_on_match' and
        'stop_token' attributes are set.

        Args:
            opt_dict: Dictionary at least one option.
        """
        processed_opt = self._opt_mngr.process_dict(opt_dict)

        if self._REGEX in processed_opt:
            self.regex = processed_opt[self._REGEX]

        if self._STOP_ON_MATCH in processed_opt:
            self.stop_on_match = processed_opt[self._STOP_ON_MATCH]

        if self._STOP_TOKEN in processed_opt:
            self.stop_token = processed_opt[self._STOP_TOKEN]

    def _reset_result_data(self):
        """Clears result_data list."""
        self.result_data = []

    def _match_regex(self, text_line):
        """Applies regular expression from 'regex' attribute.

        Args:
            text_line: Line of text to be processed.

        Returns:
            All instances that match the desired regular expression.
        """
        return re.findall(self.regex, text_line)

    def _match_and_store(self, text_line):
        """Applies regular expression and store results to result_data..

        Args:
            text_line: Line of text to be processed.

        Returns:
            True if 1 or more matches were found.
        """
        match_found = False
        result_list = self._match_regex(text_line)
        if result_list:
            match_found = True
            self.result_data.extend(result_list)

        return match_found

    def _cleanup_whitespaces(self, line):
        """Removes all whitespaces from a string.

        Args:
            line: Line of text, possibly with whitespaces..

        Returns:
            The received text string with whitespaces removed.
        """
        return re.sub(r"\s+", "", line)

    def distill(self, process_input):
        """Reads a line and extract data between delimiting tokens.

        Args:
            input: File handle for a text file.

        Returns:
            The input file handle or 'None' if no data has been read.
            Sample contains the extracted data either as a list or a
            dictionary depending on wheter the 'store_names' flag is
            set.
        """
        execute_once = (self.stop_token is None) and not self.stop_on_match
        continue_exec = True

        self._reset_result_data()

        while continue_exec:
            line = process_input.readline()
            #print(f"1 - {line}")
            if execute_once:
                continue_exec = False

            if line:
                if self.stop_token is not None and self.stop_token in line:
                    #print(f"2 - stop_token is not None and stop_token in line: {self.stop_token}")
                    continue_exec = False

                clean_line = self._cleanup_whitespaces(line)
                match_found = self._match_and_store(clean_line)

                if match_found and self.stop_on_match:
                    #print("2 - match found and stop_on_match")
                    continue_exec = False

            else:
                #print("3 - else")
                process_input = None
                data = None
                continue_exec = False
        return process_input, self.result_data


class ReadBetweenTokens(RegexRead):
    """Reads a line of text and extract text between tokens.

    The ReadBetweenTokens class reads a text line, strips it of all
    whitespaces then extracts text between tokens. Optionally it may
    attempt to extract field names by extracting everything except
    the text between tokens.

    Attributes:
        help: String containing a short help text.
        name: String containing the name that identifies this process.
        token1: String containing the first delimiter token.
        token2: String containing the second delimiter token.
        store_names: Flag the indicates if names should be extracted.
        _opt_mngr: Object of the OptManager class, stores optional
            argument names and their types, aswell as providing methods
            to process and check these optional arguments.
    """

    _TOKEN1 = "token1"
    _TOKEN2 = "token2"
    _STORE_NAMES = "store_names"

    def __init__(self, token1=None, token2=None, store_names=False):
        super().__init__()
        self.name = "READBTWTOKENS"
        self.help = "Reads text between token1 and token2, optionally reads field names"
        self.token1 = token1
        self.token2 = token2
        self._set_regex(token1, token2)
        self.store_names = store_names

    def _register_options(self):
        """Registers the expected options used by this process.

        This method registers the expected names and types for options available
        for this process.
        """
        self._opt_mngr.register_opt(self._TOKEN1, OptType.STRING, False)
        self._opt_mngr.register_opt(self._TOKEN2, OptType.STRING, False)
        self._opt_mngr.register_opt(self._STORE_NAMES, OptType.BOOL, False)

    def _set_regex(self, token1, token2):
        tk1 = None
        tk2 = None
        if token1 is not None:
            tk1 = self.token1 = token1
        if token2 is not None:
            tk2 = self.token2 = token2
        else:
            tk2 = self.token2 = tk1

        if (tk1 is not None) and (tk2 is not None):
            self.regex = "\\" + tk1 + "(.*?)\\" + tk2

    def config_process(self, opt_dict):
        """Configures the delimiter tokens and the store name option.

        This method uses the 'self._opt_mngr' object to process the
        received 'opt_dict', the resulting 'processed_opt' dictionary
        contains the options in an already compatible format. With the
        options already processed the 'store_names' may be set and/or the
        '_set_regex' method may be called to properly set the 'regex'
        attribute of the RegexRead baseclass.

        Args:
            opt_dict: Dictionary at least one option.
        """
        #self._check_options(opt_dict)
        processed_opt = self._opt_mngr.process_dict(opt_dict)

        token1_present = self._TOKEN1 in processed_opt
        token2_present = self._TOKEN2 in processed_opt
        if token1_present and token2_present:
            self._set_regex(processed_opt[self._TOKEN1],
                            processed_opt[self._TOKEN2])
        elif token1_present:
            token = processed_opt[self._TOKEN1]
            self._set_regex(token, token)
        elif token2_present:
            token = processed_opt[self._TOKEN1]
            self._set_regex(token, token)

        if self._STORE_NAMES in processed_opt:
            self.store_names = processed_opt[self._STORE_NAMES]

    def distill(self, process_input):
        """Reads a line and extract data between delimiting tokens.

        Args:
            input: File handle for a text file.

        Returns:
            The input file handle or 'None' if no data has been read.
            Sample contains the extracted data either as a list or a
            dictionary depending on wheter the 'store_names' flag is
            set.
        """
        line = process_input.readline()
        if line:
            clean_line = self._cleanup_whitespaces(line)
            data = re.findall(self.regex, clean_line)

            if self.store_names:
                names = re.sub(self.regex, " ", clean_line)
                names = names.split()
                dict_data = dict()
                for i, name in enumerate(names):
                    dict_data[name] = data[i]
                data = dict_data
        else:
            process_input = None
            data = None

        return process_input, data

    def _apply_regex(self, text_line):
        """Applies regular expression from 'regex' variable.

        Args:
            text_line: Line of text to be processed.

        Returns:
            All instances that match the desired regular expression.
        """
        return re.findall(self.regex, text_line)


class ReadBetweenWhitespaces(RegexRead):
    """Reads a line of text and extract text between whitespaces.

    The ReadBetweenWhitespaces class is a special case of the
    ReadBetweenTokens class where whitespaces are significant. A text
    line is read, optionally stripped of repeated whitespaces (as well as
    'end of lines'), then it extracts text between the remaining
    whitespaces. This class explicitly does not attempt to extract field
    names (option is set to False).

    Attributes:
        help: String containing a short help text.
        name: String containing the name that identifies this process.
        clean_whitespaces: flag that indicates wheter repeated
            whitespaces shoould be removed.
        _opt_mngr: Object of the OptManager class, stores optional
            argument names and their types, aswell as providing methods
            to process and check these optional arguments.
    """

    _CLEAN_WHITESPACES = "clean_whitespaces"

    def __init__(self, clean_whitespaces=True):
        whitespace_token = "(.*?)\\s"
        super().__init__(whitespace_token)
        self.name = "READBTWSPACES"
        self.help = "Reads text between whitespaces."
        self.clean_whitespaces = clean_whitespaces

    def _register_options(self):
        """Registers the expected options used by this process.

        This method registers the expected names and types for options available
        for this process.
        """
        self._opt_mngr.register_opt(self._CLEAN_WHITESPACES,
                                   OptType.BOOL,
                                   True)

    def config_process(self, opt_dict):
        """Replaces base class implementation, should not be called.

        This method uses the 'self._opt_mngr' object to process the
        received 'opt_dict', the resulting 'processed_opt' dictionary
        contains the options in an already compatible format. With the
        options already processed the 'clean_whitespaces' attribute is
        set.

        Args:
            opt_dict: Dict containing the '_CLEAN_WHITESPACES' options.
        """
        processed_opt = self._opt_mngr.process_dict(opt_dict)
        self.clean_whitespaces = processed_opt[self._CLEAN_WHITESPACES]

    def _cleanup_whitespaces(self, line):
        """Replaces repeated whitespaces with a single whitespace.

        Args:
            line: Line of text, possibly with whitespaces..

        Returns:
            The received text string with repeated whitespaces removed.
        """
        if self.clean_whitespaces:
            line = re.sub('\s+', ' ', line)
        line = line.rstrip()
        
        return line + " "

class MultipleRegexRead(RegexRead):
    """Reads and matches a line of text to multiple Regular expressions.

    The MultipleRegexRead class is very similar to the RegexRead class
    but instead of looking for matches to a single regular expression
    it check multiple regular expressions.

    Attributes:
        help: String containing a short help text.
        name: String containing the name that identifies this process.
        regex_dict: Dict of regular regular expressions and their identifiers.
        exclusive_match: flag that indicates wheter or not regular
            expression matching continues once a match is found.
        clean_whitespaces: flag that indicates if whitespaces should
            be cleaned or not.
        _opt_mngr: Object of the OptManager class, stores optional
            argument names and their types, aswell as providing methods
            to process and check these optional arguments.
    """

    _CLEAN_WHITESPACES = "clean_whitespaces"
    _EXCLUSIVE_MATCH = "exclusive_match"
    _REGEX_LIST = "regex_list"
    _STOP_ON_MATCH = "stop_on_match"
    _STOP_TOKEN = "stop_token"

    def __init__(self,
                 exclusive_match,
                 clean_whitespaces=True,
                 stop_on_match=True,
                 stop_token=None,
                 regex_dict = dict()):
        super().__init__(None, stop_on_match, stop_token)
        self.name = "MULTIREGEXREAD"
        self.help = "Reads text using multiple Regular Expressions"
        self.exclusive_match = exclusive_match
        self.clean_whitespaces = clean_whitespaces
        self.regex_dict = regex_dict

    def _register_options(self):
        """Registers the expected options used by this process.

        This method registers the expected names and types for options available
        for this process.
        """
        self._opt_mngr.register_opt(self._CLEAN_WHITESPACES,
                                   OptType.BOOL,
                                   False)
        self._opt_mngr.register_opt(self._EXCLUSIVE_MATCH, OptType.BOOL, False)
        self._opt_mngr.register_opt(self._REGEX_LIST,
                                   OptType.LIST_OF_TUPLES,
                                   False)
        self._opt_mngr.register_opt(self._STOP_ON_MATCH, OptType.BOOL, False)
        self._opt_mngr.register_opt(self._STOP_TOKEN, OptType.STRING, False)

    def config_process(self, opt_dict):
        """Configures option flags, 'stop_token' and regex dictionary.

        This method uses the 'self._opt_mngr' object to process the
        received 'opt_dict', the resulting 'processed_opt' dictionary
        contains the options in an already compatible format. With the
        options already processed some (or all) of the following options
        are set:
            - clean_whitespaces: Removes extra whitespaces,
            - exclusive_match: Indicates that, for each text line, regex
            matching stops after the first match is found.
            - stop_on_match: Stops reading new lines once a match is found.
            - stop_token: If this token (string) is set then each read line is
                checked for this token, if it is found then no other line will
                be read.
            - regex_dict: Dictionary mapping ids (regex_id) to a list of regular
                expressions.

        Args:
            opt_dict: Dictionary that contains the data for at least one of
            the following attributes:
                - clean_whitespaces:    bool
                - exclusive_match:      bool
                - regex_dict:           dictionary
                - stop_on_match:        bool
                - stop_token:           string

        """
        processed_opt = self._opt_mngr.process_dict(opt_dict)

        if self._CLEAN_WHITESPACES in processed_opt:
            self.clean_whitespaces = processed_opt[self._CLEAN_WHITESPACES]

        if self._EXCLUSIVE_MATCH in opt_dict:
            self.exclusive_match = processed_opt[self._EXCLUSIVE_MATCH]

        if self._REGEX_LIST in processed_opt:
            self.regex_dict = dict()
            list_of_tuples = processed_opt[self._REGEX_LIST]
            for regex_id, regex in list_of_tuples:
                #print(regex_id)
                #print(regex)
                if regex_id in self.regex_dict:
                    self.regex_dict[regex_id].append(regex)
                else:
                    self.regex_dict[regex_id] = [regex]

        if self._STOP_ON_MATCH in processed_opt:
            self.stop_on_match = processed_opt[self._STOP_ON_MATCH]

        if self._STOP_TOKEN in processed_opt:
            self.stop_token = processed_opt[self._STOP_TOKEN]

    def _reset_result_data(self):
        """Clears result_data dictionary."""
        self.result_data = dict()

    def _cleanup_whitespaces(self, line):
        """Clean_up as RegexRead or no clean_up.

        Args:
            line: Line of text, possibly with whitespaces..

        Returns:
            The received text string with whitespaces removed or unchanged.
        """
        if self.clean_whitespaces:
            line = super()._cleanup_whitespaces(line)

        return line

    def _match_and_store(self, text_line):
        """Applies multiple regular expressions to the text_line variable.

        This method applies all regular expressions present in the regex_dict
        attribute to the text_line variable. Regular expressions are tied to
        identifiers (regex_id), matches are stored in the result_data variable
        as a dictionary of lists (for each identifier a list of results).

        Args:
            text_line: Line of text to be processed.

        Returns:
            If 'exclusive_match' is True then return a list with all
            instances that match the desired regular expression.
            If 'exclusive_match' is False then returns a list of tuples,
            each tuple contains the regular expression and all instances
            that matched it.
        """
        match_found = False

        #print(text_line)
        for regex_id, regex_list in self.regex_dict.items():
            for regex in regex_list:
                #print(regex, self.exclusive_match)
                if not (match_found and self.exclusive_match):
                    self.regex = regex

                    temp = self._match_regex(text_line)

                    if temp:
                        match_found = True
                        if regex_id in self.result_data:
                            old_data = self.result_data[regex_id]
                            old_data.extend(temp)
                        else:
                            self.result_data.update({regex_id : temp})

        return match_found

class BlockRegexRead(RegexRead):
    """Reads and matches a line of text to multiple Regular expressions.

    The BlockRegexRead class is very similar to the MultiRegexRead class,
    also working with the concept of recurring blocks of text. As with
    MultiRegexRead, BlockRegexRead attempts to match a line of text to
    1 or more regular expressions but also attempting to detect a
    "block end token". Each detected block is identified with a incresing
    index number.

    Unlike MultipleRegexRead BlockRegexRead assumes that matches are
    exclusive (i.e. a line matches only a single regular expression, the
    first one tested), and that matches only stop when the "stop_token"
    is found (i.e stop_on_match is false),

    Attributes:
        help: String containing a short help text.
        name: String containing the name that identifies this process.
        block_end_token: String identifying the end of a block of text.
        regex_dict: Dict of regular regular expressions and their identifiers.
        clean_whitespaces: flag that indicates if whitespaces should
            be cleaned or not.
        _opt_mngr: Object of the OptManager class, stores optional
            argument names and their types, aswell as providing methods
            to process and check these optional arguments.
    """

    _BLOCK_END_TOKEN = "block_end_token"
    _CLEAN_WHITESPACES = "clean_whitespaces"
    _REGEX_LIST = "regex_list"
    _STOP_TOKEN = "stop_token"

    def __init__(self,
                 block_end_token=None,
                 stop_token=None,
                 clean_whitespaces=True,
                 regex_dict = {}):
        super().__init__(None, False, stop_token)
        self.name = "BLOCKREGEXREAD"
        self.help = "Reads blocks of text using multiple Regular Expressions"
        self.block_end_token = block_end_token
        self._block_count = 0
        self.clean_whitespaces = clean_whitespaces
        self.regex_dict = regex_dict

    def _register_options(self):
        """Registers the expected options used by this process.

        This method registers the expected names and types for options available
        for this process.
        """
        self._opt_mngr.register_opt(self._BLOCK_END_TOKEN,
                                   OptType.STRING,
                                   False)
        self._opt_mngr.register_opt(self._CLEAN_WHITESPACES,
                                   OptType.BOOL,
                                   False)
        self._opt_mngr.register_opt(self._REGEX_LIST,
                                   OptType.LIST_OF_TUPLES,
                                   False)
        self._opt_mngr.register_opt(self._STOP_TOKEN, OptType.STRING, False)

    def config_process(self, opt_dict):
        """Configures option flags, 'stop_token' and regex dictionary.

        This method uses the 'self._opt_mngr' object to process the
        received 'opt_dict', the resulting 'processed_opt' dictionary
        contains the options in an already compatible format. With the
        options already processed some (or all) of the following options
        are set:
            - block_end_token: This token identifies that the end of a block
                was found, all results include a 'token_id' that indicates
                to which block a result corresponds to.
            - clean_whitespaces: Removes extra whitespaces,
            - stop_token: If this token (string) is set then each read line is
                checked for this token, if it is found then no other line will
                be read.
            - regex_dict: Dictionary mapping ids (regex_id) to a list of regular
                expressions.

        Args:
            opt_dict: Dictionary that contains the data for at least one of
            the following attributes:
                - block_end_token       string
                - clean_whitespaces:    bool
                - regex_dict:           dictionary
                - stop_token:           string

        """
        processed_opt = self._opt_mngr.process_dict(opt_dict)

        if self._BLOCK_END_TOKEN in processed_opt:
            self.block_end_token = processed_opt[self._BLOCK_END_TOKEN]

        if self._CLEAN_WHITESPACES in opt_dict:
            self.clean_whitespaces = processed_opt[self._CLEAN_WHITESPACES]

        if self._REGEX_LIST in processed_opt:
            self.regex_dict = dict()
            list_of_tuples = processed_opt[self._REGEX_LIST]
            for regex_id, regex in list_of_tuples:
                if regex_id in self.regex_dict:
                    self.regex_dict[regex_id].append(regex)
                else:
                    self.regex_dict[regex_id] = [regex]

        if self._STOP_TOKEN in processed_opt:
            self.stop_token = processed_opt[self._STOP_TOKEN]

    def _reset_result_data(self):
        """Clears result_data dictionary."""
        self.result_data = {}
        self._block_count = 0

    def _cleanup_whitespaces(self, line):
        """Clean_up as RegexRead or no clean_up.

        Args:
            line: Line of text, possibly with whitespaces..

        Returns:
            The received text string with whitespaces removed or unchanged.
        """
        if self.clean_whitespaces:
            line = super()._cleanup_whitespaces(line)

        return line

    def _match_and_store(self, text_line):
        """Applies multiple regular expressions to the text_line variable.

        This method applies all regular expressions present in the regex_dict
        attribute to the text_line variable. Regular expressions are tied to
        identifiers (regex_id), matches are stored in the result_data variable
        as a dictionary of lists (for each identifier a list of results).

        Args:
            text_line: Line of text to be processed.

        Returns:
            If 'exclusive_match' is True then return a list with all
            instances that match the desired regular expression.
        """
        if self.block_end_token in text_line:
            self._block_count += 1

        match_found = False
        for regex_id, regex_list in self.regex_dict.items():
            for regex in regex_list:
                if not match_found:
                    self.regex = regex

                    temp = self._match_regex(text_line)

                    if temp:
                        match_found = True
                        if regex_id in self.result_data:
                            old_data = self.result_data[regex_id]
                            block_id, prev_matches = old_data[-1]
                            if block_id == self._block_count:
                                #old_data[-1][1].extend(temp)
                                prev_matches.extend(temp)
                            else:
                                old_data.append((self._block_count, temp))
                        else:
                            temp_tuple = [(self._block_count, temp)]
                            self.result_data.update({regex_id : temp_tuple})
                        break

            if match_found:
                break

        return match_found
