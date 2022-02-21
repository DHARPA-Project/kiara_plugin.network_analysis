# Usage

The central data type of the *network_analysis* modules repo is called [``network_data``][kiara_modules.network_analysis.value_types.NetworkDataType], which uses a sqlite database to store and access the node and edges data.

Internally, it uses a wrapper class for convenient access to the network data and underlying database called [NetworkData][kiara_modules.network_analysis.metadata_models.NetworkData].
