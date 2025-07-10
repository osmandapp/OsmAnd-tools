import sys

def parse_command_line_into_dict() -> dict[str, bool | str]:
    args = {}
    for arg in sys.argv[1:]:
        if arg.startswith("--"):
            if "=" in arg:
                key, value = arg.split("=", 1)
                args[f"{key}="] = value
                args[key] = value
            else:
                args[arg] = True
    return args


