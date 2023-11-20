from __future__ import annotations

from harlequin.options import (
    FlagOption,  # noqa
    ListOption,  # noqa
    PathOption,  # noqa
    SelectOption,  # noqa
    TextOption,
)

host = TextOption(
    name="host",
    description=(
        "Specifies the host name of the machine on which the server is running. "
        "If the value begins with a slash, it is used as the directory for the "
        "Unix-domain socket."
    ),
    short_decls=["-h"],
    default="localhost",
)


port = TextOption(
    name="port",
    description=(
        "Port number to connect to at the server host, or socket file name extension "
        "for Unix-domain connections."
    ),
    short_decls=["-p"],
    default="5432",
)


dbname = TextOption(
    name="dbname",
    description=(
        "Port number to connect to at the server host, or socket file name extension "
        "for Unix-domain connections."
    ),
    short_decls=["-d"],
    default="postgres",
)


user = TextOption(
    name="user",
    description=("PostgreSQL user name to connect as."),
    short_decls=["-u", "--username", "-U"],
)


password = TextOption(
    name="password",
    description=("Password to be used if the server demands password authentication."),
)


passfile = PathOption(
    name="passfile",
    description=(
        "Specifies the name of the file used to store passwords. Defaults to "
        "~/.pgpass, or %APPDATA%\postgresql\pgpass.conf on Windows. (No error is "
        "reported if this file does not exist.)"
    ),
    resolve_path=True,
    exists=False,
    file_okay=True,
    dir_okay=False,
)

require_auth = SelectOption(
    name="require_auth",
    description=(
        "Specifies the authentication method that the client requires from the server. "
        "If the server does not use the required method to authenticate the client, or "
        "if the authentication handshake is not fully completed by the server, the "
        "connection will fail."
    ),
    choices=["password", "md5", "gss", "sspi", "scram-sha-256", "none"],
)

channel_binding = SelectOption(
    name="channel_binding",
    description=(
        "This option controls the client's use of channel binding. A setting of "
        "require means that the connection must employ channel binding, prefer "
        "means that the client will choose channel binding if available, and "
        "disable prevents the use of channel binding. The default is prefer if "
        "PostgreSQL is compiled with SSL support; otherwise the default is disable."
    ),
    choices=["require", "prefer", "disable"],
)


def _int_validator(s: str | None) -> tuple[bool, str]:
    if s is None:
        return True, ""
    try:
        _ = int(s)
    except ValueError:
        return False, f"Cannot convert {s} to an int!"
    else:
        return True, ""


connect_timeout = TextOption(
    name="connect_timeout",
    description=(
        "Maximum time to wait while connecting, in seconds (write as an integer, "
        "e.g., 10)."
    ),
    validator=_int_validator,
)

sslmode = SelectOption(
    name="sslmode",
    description=(
        "Determines whether or with what priority a secure SSL TCP/IP connection will "
        "be negotiated with the server."
    ),
    choices=["disable", "allow", "prefer", "require", "verify-ca", "verify-full"],
    default="prefer",
)

sslcert = PathOption(
    name="sslcert",
    description=(
        "Specifies the file name of the client SSL certificate. "
        "Ignored if an SSL connection is not made."
    ),
    default="~/.postgresql/postgresql.crt",
)

sslkey = TextOption(
    name="sslkey",
    description=(
        "Specifies the location for the secret key used for the client certificate. "
        "It can either specify a file name that will be used instead of the default "
        "~/.postgresql/postgresql.key, or it can specify a key obtained from an "
        "external engine. An external engine specification should consist of a "
        "colon-separated engine name and an engine-specific key identifier. This "
        "parameter is ignored if an SSL connection is not made."
    ),
)


POSTGRES_OPTIONS = [
    host,
    port,
    dbname,
    user,
    password,
    passfile,
    require_auth,
    channel_binding,
    connect_timeout,
    sslmode,
    sslcert,
    sslkey,
]
