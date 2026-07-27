{% macro haversine_km(lat1, lon1, lat2, lon2) %}
    (
        2 * 6371 * ASIN(
            SQRT(
                POWER(SIN(RADIANS({{ lat2 }} - {{ lat1 }}) / 2), 2)
                + COS(RADIANS({{ lat1 }}))
                  * COS(RADIANS({{ lat2 }}))
                  * POWER(SIN(RADIANS({{ lon2 }} - {{ lon1 }}) / 2), 2)
            )
        )
    )
{% endmacro %}