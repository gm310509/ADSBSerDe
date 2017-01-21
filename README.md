# Bootcamp_Dec16_Utah


The ADSBSerDe folder contains the Java Project for a Hive SerDe.
This SerDe allows the ADSB data file to be read natively by hive.

The SerDe leverages the column names in the table definition to determine
which fields to return. The column names in the table definition must match
the attribute names in the raw ADSB data file.

The SerDe supports String, Int, float and timestamp data types onle.
Note: timestamps in the ADSB file will be interpreted as Unix epoch times.
That is, they will be interpreted as *seconds* since 1970-01-01 00:00:00

Refer to the sql/create_serde.hql for an example of how to use the compiled JAR
and a sample table definition.

