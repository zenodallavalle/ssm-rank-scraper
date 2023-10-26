class UnrecognizedYearError(TypeError):
    """
    Error raised when year cannot be parsed correctly. Remember to pass 4 digits full year or last 2 digits ("20"-leading will be added automatically).
    It is better to pass string rather than other data_types as they will be casted to string with str(year).
    """

    def __init__(self, year, *args: object) -> None:
        super().__init__(
            f'Unrecognized type of year "{year}" (must be 4 digits or last 2 digits).',
            *args,
        )


def parse_year_long(year):
    if not isinstance(year, str):
        year = str(year)
    if len(year) == 2:
        return f"20{year}"
    elif len(year) == 4:
        return year
    else:
        raise UnrecognizedYearError(year)


def parse_year_short(year):
    if not isinstance(year, str):
        year = str(year)
    if len(year) == 2:
        return year
    elif len(year) == 4:
        return year[2:4]
    else:
        raise UnrecognizedYearError(year)


def parse_year_int(year):
    return int(parse_year_long(year))
