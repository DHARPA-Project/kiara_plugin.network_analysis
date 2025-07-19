# [**kiara**](https://dharpa.org/kiara) plugin:

This package contains a set of commonly used/useful modules, pipelines, types and metadata schemas for [*kiara*](https://github.com/DHARPA-project/kiara).

## Description

Network-related data-types, modules and pipelines for kiara.

## Package content

{% for item_type, item_group in get_context_info().get_all_info().items() %}

### {{ item_type }}
{% for item, details in item_group.item_infos.items() %}
- [`{{ item }}`][kiara_info.{{ item_type }}.{{ item }}]: {{ details.documentation.description }}
{% endfor %}
{% endfor %}

## Links

 - Documentation: [https://DHARPA-Project.github.io/kiara_plugin.network_analysis](https://DHARPA-Project.github.io/kiara_plugin.network_analysis)
 - Code: [https://github.com/DHARPA-Project/kiara_plugin.network_analysis](https://github.com/DHARPA-Project/kiara_plugin.network_analysis)
