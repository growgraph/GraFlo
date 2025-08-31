### Table Config  

Table part of the config specifies how input sources will be transformed and mapped to vertex collections.

```yaml
table:
-   tabletype: ibes
    encoding: ISO-8859-1
    transforms:
    -   foo: parse_date_ibes
        module: graflo.util.transform
        input:
        -   ANNDATS
        -   ANNTIMS
        output:
        -   datetime_announce
```

