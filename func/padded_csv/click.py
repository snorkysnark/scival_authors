import click
from typing import Any, Optional


class HeaderLengthArg(click.ParamType):
    name = "header_length"

    def convert(
        self, value: Any, param: Optional[click.Parameter], ctx: Optional[click.Context]
    ) -> Any:
        if isinstance(value, int):
            return value
        elif isinstance(value, str):
            return "auto" if value == "auto" else int(value)
        else:
            self.fail('Must be an integer value or "auto"')


header_length_arg = HeaderLengthArg()
