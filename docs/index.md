# kiara modules for: network-analysis

This package contains a set of commonly used/useful modules, pipelines, types and metadata schemas for [*Kiara*](https://github.com/DHARPA-project/kiara).


## Description

TODO

## Package content

{% for info_category, details in get_info_for_categories('metadata.value_type','metadata.module', 'metadata.pipeline','metadata.operation_type', limit_to_package='kiara_modules.network_analysis').items() %}
### {{ details['title'] }}
{% for item, desc in details['items'].items() %}- [{{ item }}][]: {{ desc }}
{% endfor %}
{% endfor %}


## Links

 - Documentation: [https://dharpa.org/kiara_modules.network_analysis](https://dharpa.org/kiara_modules.network_analysis)
 - Code: [https://github.com/DHARPA-Project/kiara_modules.network_analysis](https://github.com/DHARPA-Project/kiara_modules.network_analysis)
