SELECT
    CAST(booleanDatum AS STRING) AS booleanDatum,
    CAST(dateDatum AS STRING) AS dateDatum,
    CAST(decimalDatum AS STRING) AS decimalDatum,
    CAST(doubleDatum AS STRING) AS doubleDatum,
    CAST(integerDatum AS STRING) AS integerDatum,
    CAST(longDatum AS STRING) AS longDatum,
    CAST(stringDatum AS STRING) AS stringDatum,
    CAST(timeDatum AS STRING) AS timeDatum,
    CAST(timestampDatum AS STRING) AS timestampDatum
FROM
    dataset