from harlequin.options import (
    TextOption,
    SelectOption, # noqa
    FlagOption, # noqa
    ListOption, # noqa
    PathOption, # noqa
)  

foo = TextOption(
    name="foo",
    description="Help text goes here",
    short_decls=["-f"],
)

MYADAPTER_OPTIONS = [foo]
