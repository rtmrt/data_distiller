#
# Module used to handle string-based options.
# Copyright (C) 2020  Claudio Romero
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#

"""Module used to handle string-based options.

The options module is a comprised of the 'OptType' class (which defines
the supported/known options types), the 'OptManager' class (which stores
options by their name and type, aswell as providing a method to
process/validate received options and another method that generates the
expected usage of the stored options), and the 'opt_generator' method
that parses strings yielding options tuples (names and data).

    Typical usage example:

    opt_mngr = OptManager()
    opt_mngr.register_opt(<opt-name>, <OptType>, <True/False>)
    opt_string = "<opt-name>=example-data"
    opt_name, opt_data = opt_generator(opt_string)
    
    opt_dict = {opt_name: opt_data}
    
    processed_opt = opt_mngr.process_dict(opt_dict)

'opt_generator' is a generic parser of string to tuples. Strings are parsed
with the following rules:
    
    - \ is an escape character, characters following an escape char are stored
        directly to the 'buffer' without any processing.

    - " is used to identify the start/end of a string, all other characters
        following it are stored directly to the 'buffer' without processing,
        including the escape character (\).

    - Whitespaces are ignored unles they follow one of the previous characters.

    - [ Indicates the start of a list, this can be a simple list or a list of
        tuples.

    - ] Indicates the end of a list.
    
    - = Indicates the end of a option name and that the next character will be
        part of the option data.
    
    - , Indicates the end of an option pair (name, data).
"""

from enum import IntEnum, unique, auto

class OptType(IntEnum):
    """Class that enumerates different options types.

    An 'OptType' object simply represents one of the following option
    types:
    
        - STRING:           Any string of characters.
        - BOOL:             Boolean (OptManager expectes 'yes' or no'.
        - LIST:             List of strings.
        - LIST_OF_TUPLES:   List of pairs 'name=value'
    """
    STRING = auto()
    BOOL = auto()
    LIST = auto()
    LIST_OF_TUPLES = auto()

class OptManager():
    """Class that represents a manager for text-based options.
    
    A OptManager object stores required and not required option
    prototypes. Each prototype is simply a key-value pair with the
    option name as key an 'OptType' object as value.
    
    OptManager validates and processes options received as a Dictionary
    in which the keys are option names (key for the stored options) and
    the values are strings (STRING and BOOL types), list of strings (LIST
    type) or tuples (LIST_OF_TUPLES type).
    
    Options are stored in two separate dictionaries, one for the required
    options (a ValueError exception is raise if these options are not present)
    and another for the not required ones.

    Attributes:
        known_types (set of :obj: 'OptType'): Set that contains the
            types that OptManager supports.
        
        _required_opt_dict (dict): Dictionary containing the key-value
            mapping for all the required options.
            
        _required_opt_dict (dict): Dictionary containing the key-value
            mapping for all the required options.
    """
    def __init__(self):
        self.known_types = {OptType.STRING,
                            OptType.BOOL,
                            OptType.LIST,
                            OptType.LIST_OF_TUPLES}

        self._required_opt_dict = dict()
        self._not_required_opt_dict = dict()
        
    def register_opt(self, opt_name, opt_type, is_required):
        """Registers a new option.

        This method stores the supplied option on the appropriate dictionary,
        this options must be of a known OptType (must exist in the known_types
        set) and 'opt_name' must be unique (not be present in either of the
        option dictionaries).

        Args:
            opt_name: String containing the option name/identifier.
            opt_type: OptType object that represents the option type.
            is_required: Bool that indicates wheter this options is required.
        """
        if (opt_name in self._not_required_opt_dict or
        opt_name in self._required_opt_dict):
            raise ValueError(f"{opt_name} option already registered.")

        if opt_type not in self.known_types:
            msg = f"Unable to register '{opt_name}', unknown type: {opt_type}"
            raise ValueError(msg)

        if is_required:
            self._required_opt_dict[opt_name] = opt_type
        else:
            self._not_required_opt_dict[opt_name] = opt_type

    def usage(self):
        """Creates usage example string for the registered options.

        Returns:
            String containing all the options and their usage.
        """
        usage = ""
        opt_str = "<required>"
        for opt_name, opt_type in self._required_opt_dict.items():            
            if usage:
                usage += ","

            usage += self._usage_str(opt_name, opt_type, True)
            
        for opt_name, opt_type in self._not_required_opt_dict.items():
            if usage:
                usage += ","
            usage += self._usage_str(opt_name, opt_type, False)

    def _usage_str(cls, opt_name, opt_type, is_required):
        """Creates the usage example string for a single option.

        Args:
            opt_name: String containing the option name/identifier.
            opt_type: OptType object that represents the option type.
            is_required: Bool that indicates wheter this options is required.

        Returns:
            String containing the usage for a single option.
        """
        if is_required:
            opt_str = "<required>"
        else:
            opt_str = "<optional>"

        usage_str = ""
        if opt_type == OptType.STRING:
            usage_str += opt_name + "=" + opt_str
        elif opt_type == OptType.LIST:
            usage_str += f"{opt_name}=[{opt_str}, .... {opt_str}]"
        elif opt_type == OptType.LIST_OF_TUPLES:
            item_name = "<item_name>"
            usage_str += f"{opt_name}=[{item_name}={opt_str}, ..., {item_name}={opt_str}"
        elif opt_type == OptType.BOOL:
            usage_str += f"{opt_name}=yes/no"
        else:
            usage_str = "this is wrong"

        return usage_str

    def _process_opt(self, opt_name, opt_value, opt_type):
        """Validates and processes a single option input.

        Validates 'opt_value' with respect to 'opt_type' and processes
        opt_value so the returned value is of the correct type.
        
        If 'opt_type' is of unknown type or 'opt_value' is not compatible
        with 'opt_type' then a ValueError exception is raised.

        Args:
            opt_name: String containing the option name/identifier.
            opt_value: Object containing the option data.
            opt_type: OptType object that represents the option type.

        Returns:
            Processed option:
             - STRING:  str.
             - BOOL:    boolean.
             - LIST:    list.
             - LIST_OF_TUPLES: list of tuples.
        """
        opt_type_ok = False
        opt = None
        if opt_type == OptType.STRING:
            opt_type_ok = isinstance(opt_value, str)
            if opt_type_ok and opt_value:
                opt = opt_value

        elif opt_type == OptType.LIST:
            opt_type_ok = isinstance(opt_value,list)

            opt = opt_value if opt_type_ok and len(opt_value) > 0 else None

        elif opt_type == OptType.LIST_OF_TUPLES:
            opt_type_ok = all(isinstance(x,tuple) for x in opt_value)

            opt = opt_value if opt_type_ok and len(opt_value) > 0 else None

        elif opt_type == OptType.BOOL:
            opt_type_ok = (opt_value == "yes") or (opt_value == "no")
            if opt_type_ok:
                opt = True if opt_value == "yes" else False
        else:
            raise ValueError(f"{opt_name} is of unknown option type: {opt_name}")

        if not opt_type_ok:
            msg = (f"Value ({opt_value}) for '{opt_name}' is not compatible "
                   f"with its type {opt_type}")
            raise ValueError(msg)
        
        return opt

    def process_dict(self, opt_dict):
        """Validates and processes all option in input dictionary.

        All required and not-required options are read and checked
        against the input 'opt_dict', if a required options is not
        found then a ValueError exception is raised.

        Args:
            opt_name: String containing the option name/identifier.
            opt_value: Object containing the option data.
            opt_type: OptType object that represents the option type.

        Returns:
            Processed option:
             - STRING:  str.
             - BOOL:    boolean.
             - LIST:    list.
             - LIST_OF_TUPLES: list of tuples.
        """
        options = dict()
        for opt_name, opt_type in self._required_opt_dict.items():
            if opt_name in opt_dict:
                
                opt_ok = opt_name in opt_dict
                if opt_ok:
                    opt_value = opt_dict[opt_name]
                    
                    processed_opt = self._process_opt(opt_name,
                                                      opt_value,
                                                      opt_type)
                    if processed_opt is not None:
                        options[opt_name] = processed_opt
                    else:
                        opt_ok = False

                if not opt_ok:
                    raise ValueError(f"Required option not found: {opt_name}")
                    
        for opt_name, opt_type in self._not_required_opt_dict.items():
            if opt_name in opt_dict:
                opt_value = opt_dict[opt_name]
                
                processed_opt = self._process_opt(opt_name,
                                                  opt_value,
                                                  opt_type)
                if processed_opt is not None:
                    options[opt_name] = processed_opt

        return options
    
def opt_generator(opt_str):
    """Generic option parser from text strings to name-value tuples.

    'opt_generator' is a generic parser of string to tuples. Strings are read
    character by character which are parsed with the following rules:
    
        - \ is an escape character, characters following an escape char are stored
            directly to the 'buffer' without any processing.

        - " is used to identify the start/end of a string, all other characters
            following it are stored directly to the 'buffer' without processing,
            including the escape character (\).

        - Whitespaces are ignored unles they follow one of the previous characters.

        - [ Indicates the start of a list, this can be a simple list or a list of
            tuples.

        - ] Indicates the end of a list.
        
        - = Indicates the end of a option name and that the next character will be
            part of the option data.
        
        - , Indicates the end of an option pair (name, data).

    Args:
        opt_str:    Option string.

    Returns:
        Yields a name-value tuple
    """

    buffer = ""
    escape_detected = False
    string_detected = False
    is_list = False
    name = None
    tuple_name = None
    data = None
    
    for char in opt_str:
        if escape_detected:
            buffer += char
            escape_detected = False
            continue
        
        char_is_double_quotes = char == "\""
        if string_detected or char_is_double_quotes:

            if char_is_double_quotes and string_detected:
                string_detected = False
                continue
            elif char_is_double_quotes: #string_detected = False
                string_detected = True
                continue
            else: #string_detected == True and char_is_double_quotes == False
                buffer += char
                continue
                
            string_detected = True
            continue

        if char == "\\":
            escape_detected = True
            continue
        
        if char == " ":
            continue

        if char == "[": #Início de uma lista
#TODO check if name is None?
            if is_list:
                raise ValueError("List of list are not currently supported")
            is_list = True
            data = []
            continue

        if char == "]": #Fim de uma lista
            is_list = False
            if tuple_name is not None:
                data.append((tuple_name, buffer))
                tuple_name = None
            else:
                data.append(buffer)
            yield (name, data)
            name = None
            data = None
            buffer = ""
            continue

#TODO Futuramente talvez permitir listas de listas.

        if char == "=":
            if name is None:
                if buffer:
                    name = buffer
                    buffer = ""
                else:
                    raise ValueError(f"Option wihtout name found: {buffer}")
                continue
            elif tuple_name is None and is_list:
                if buffer:
                    tuple_name = buffer
                    buffer = ""
                else:
                    raise ValueError("Empty name found in list")
                continue
            else:
                name_data_separator = "="
                raise ValueError(f"Incorrect usage, two '{name_data_separator}' where found")

        if char == ",":
            if is_list:
                if tuple_name is not None:
                    data.append((tuple_name, buffer))
                    tuple_name = None
                else:
                    data.append(buffer)
            else:
                data = buffer
                yield (name, data)
                name = None
                data = None
            buffer = ""
            continue

        buffer += char

    if name is None and buffer != "":
        raise ValueError(f"Option wihtout name found: {buffer}")
    elif name is not None:
        data = buffer
        yield (name, data)
        
