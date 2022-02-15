# Configuration

Configuration is supported by @link:[Typesafe Config](https://github.com/lightbend/config){ open=new }, enabling
multiple ways to pass in options. Most commonly, configuration is provided via either Java system
properties (passed as command-line options) or via a
@link:[HOCON](https://github.com/lightbend/config/blob/main/HOCON.md){ open=new } config file. HOCON is a JSON-like
format that is very flexible and human-readable. The reference config below is in HOCON format.

```bash
# Example of overriding configuration via system properties
java \
  -Dthatdot.connect.webserver.port=9000 \
  -Dthatdot.connect.id.type=uuid-3 \
  -jar connect.jar

# Example of overriding configuration via configuration file
java \
  -Dconfig.file=thatdot-connect.conf \
  -jar connect.jar
```

Uncommented values are the defaults, unless otherwise noted. Unexpected configuration keys or
values in the `thatdot.connect` block will report an error at startup.

An individual underscore `_` is used to indicate a required property with no default value. There are none of
these in the default configuration.

@@snip [reference.conf]($connect$/src/test/resources/documented_config.conf)
