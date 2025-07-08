{% macro get_pk_column(model_name) %}
    {% set schema_file_path = 'models/silver/' ~ model_name ~ '.yml' %}
    {% set yml = load_yaml(schema_file_path) %}
    {% for model in yml.models %}
        {% if model.name == model_name %}
            {% for col in model.columns %}
                {% if col.meta is defined and col.meta.pk is defined and col.meta.pk == true %}
                    {{ return(col.name) }}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
    {{ exceptions.raise_compiler_error("❌ No PK column found in schema.yml for model: " ~ model_name) }}
{% endmacro %}
