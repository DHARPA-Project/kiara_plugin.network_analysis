# Usage

## Introduction

## The `network_data` type

If you access the `.data` attribute of a value of the `network_data` type, you will get a Python instance of the class [`NetworkData`](https://github.com/DHARPA-Project/kiara_plugin.network_analysis/blob/develop/src/kiara_plugin/network_analysis/models/__init__.py).

In Python, this would look something like:

```
from kiara.api import KiaraAPI
from kiara_plugin.network_analysis.models import NetworkData

kiara = KiaraAPI.instance()
network_data_value = api.get_value("my_network_data_alias_or_id")

network_data: NetworkData = network_data_value.data
```

or, from within a module `process` method:

```
from kiara.api import ValueMap, Value
from kiara_plugin.network_analysis.models import NetworkData

def process(self, inputs: ValueMap, outputs: ValueMap):

    network_data_obj = inputs.get_value_obj("network_data_input_field_name")
    network_data: NetworkData = network_data_obj.data
```

This is a wrapper class that stores all the data related to the nodes and edges of the network data in two separate tables (inheriting from [`KiaraTables`](https://github.com/DHARPA-Project/kiara_plugin.tabular/blob/develop/src/kiara_plugin/tabular/models/tables.py), which in turn uses [`KiaraTable`](https://github.com/DHARPA-Project/kiara_plugin.tabular/blob/develop/src/kiara_plugin/tabular/models/table.py) to store the actual per-table data).

The only two tables that are available in a `NetworkData` instance are called `nodes` and `edges`. You can access them via the `.nodes` and `.edges` attributes of the `NetworkData` instance. As mentioned above, Both of these attributes are instances of `KiaraTable`, so you can use all the methods of that class to access the data. The most important ones are:

- `.arrow_table`: to get the data as an [Apache Arrow](https://arrow.apache.org/) table
- `.to_pandas_dataframe()`: to get the data as a [pandas](https://pandas.pydata.org/) dataframe -- please try to always use the arrow table, as it is much more efficient and avoides loading the whole data into memory in some cases

As a convention, *kiara* will add columns prefixed with an underscore if the values in it have internal 'meaning', normal/original attributes are stored in columns without that prefix.

Both node and edge tables contain a unique `id` column (`_node_id`, `_edge_id`) that is generated for eacch specific network_data instance. You can not rely on this id being consistent across network_data values (e.g. if you create a filtered `network_data` instance from another one, the same node_id will most likely not refer to the original node row).

### The 'edges' table

The `edges` table contains the data about the edges of the network. The most important columns are:

- `_source`: the source node ids of the edge
- `_target`: the target node ids of the edge

In addition, this table contains a number of pre-processed, static metadata concerning this specific `network_data` instance. You can get information about those using the cli command:

```
kiara data-type explain network_data
```

The `nodes' table contains the data about node attributes of the network. The `_node_id` column contains node ids that reference the `_source`/`_target` columns of the `edges` table.

The table also contains additional pre-processed, static metadata for this specific `network_data` instance, which can be accessed using the same cli command as above.

## `network_data`-specific metadata

Along the pre-processed edge- and node- metadata, a `network_data` value also comes with some more general, pre-processed metadata:

```
kiara data explain -p journals_network

...
...
properties:
    "metadata.network_data": {
      "number_of_nodes": 276,
      "properties_by_graph_type": {
        "directed": {
          "number_of_edges": 321,
          "parallel_edges": 0
        },
        "directed_multi": {
          "number_of_edges": 321,
          "parallel_edges": 0
        },
        "undirected": {
          "number_of_edges": 313,
          "parallel_edges": 0
        },
        "undirected_multi": {
          "number_of_edges": 321,
          "parallel_edges": 8
        }
      },
      "number_of_self_loops": 1
    }
...
...

```

In a *kiara* module you'd access this information like:

```python

def process(self, inputs: ValueMap, outputs: ValueMap):

    network_data_obj: Value = inputs.get_value_obj("network_data_input_field_name")
    network_props = network_data_obj.get_property_data('metadata.network_data')
```

This gives you information about the number of edges (and parallel edges), depending as which graph type you interpret the data itself. For example, the 'undirected' graph type would merge all the edges that have the same source/target and target/source combinations into a single edge, whereas the 'directed' graph type would keep them separate.

In addition, you can also retrieve the more generic table column metadata for the `nodes` and `edges` tables:

```python

table_props = network_data_obj.get_property_data('metadata.tables')
```

This can be useful for non-auto-pre-processed node/edge attributes that where copied over from the original data, or just to get
an idea about the general shape of the data.


## Creating a `NetworkData` instance in a *kiara* module

*kiara* tries to make assembling `network_data` as easy as possible for a module developer (this should only ever happen within the context of a module).

The default way to assemble a `network_data` value is to use the `create_network_data` class method of the [`NetworkData`](https://github.com/DHARPA-Project/kiara_plugin.network_analysis/blob/develop/src/kiara_plugin/network_analysis/models/__init__.py) class:

This method is the most flexible and powerful, which means it also requires some preparation of the data, and the data to be in a specific format. To make this easier, there exists a convenience method to create a `network_data` value from an existing `networkx` graph:

```python
def create_from_networkx_graph(
    cls,
    graph: "nx.Graph",
    label_attr_name: Union[str, None] = None,
    ignore_node_attributes: Union[Iterable[str], None] = None,
    ) -> "NetworkData":
```

In addition, there exists a helper function that lets you create a `network_data` instance from an existing one, in addition to a list of node_ids the new graph should contain (nodes/edges containing ids not in that list will be not included in the new graph)

```python
def from_filtered_nodes(
    cls, network_data: "NetworkData", nodes_list: List[int]
) -> "NetworkData":
```


## Assembling a `network_data` value in a workflow

The central operation that is used to assemble a `network_data` value is called `assemble.network_data`:

```
❯ kiara operation explain assemble.network_data

╭─ Operation: assemble.network_data ───────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│                                                                                                                                                  │
│   Documentation   Create a 'network_data' instance from one or two tables.                                                                       │
│                                                                                                                                                  │
│                   This module needs at least one table as input, providing the edges of the resulting network data set.                          │
│                   If no further table is created, basic node information will be automatically created by using unique values from the edges     │
│                   source and target columns.                                                                                                     │
│                                                                                                                                                  │
│                   If no `source_column_name` (and/or `target_column_name`) is provided, *kiara* will try to auto-detect the most likely of the   │
│                   existing columns to use. If that is not possible, an error will be raised.                                                     │
│                                                                                                                                                  │
│   Inputs                                                                                                                                         │
│                     field name         type     description                                                      Required   Default              │
│                    ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────    │
│                     edges              table    A table that contains the edges data.                            yes        -- no default --     │
│                     source_column      string   The name of the source column name in the edges table.           no         -- no default --     │
│                     target_column      string   The name of the target column name in the edges table.           no         -- no default --     │
│                     edges_column_map   dict     An optional map of original column name to desired.              no         -- no default --     │
│                     nodes              table    A table that contains the nodes data.                            no         -- no default --     │
│                     id_column          string   The name (before any potential column mapping) of the            no         -- no default --     │
│                                                 node-table column that contains the node identifier (used in                                     │
│                                                 the edges table).                                                                                │
│                     label_column       string   The name of a column that contains the node label (before any    no         -- no default --     │
│                                                 potential column name mapping). If not specified, the value of                                   │
│                                                 the id value will be used as label.                                                              │
│                     nodes_column_map   dict     An optional map of original column name to desired.              no         -- no default --     │
│                                                                                                                                                  │
│                                                                                                                                                  │
│   Outputs                                                                                                                                        │
│                     field name     type           description                                                                                    │
│                    ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────    │
│                     network_data   network_data   The network/graph data.                                                                        │
│                                                                                                                                                  │
│                                                                                                                                                  │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

```

This assumes the user has already imported at least a table containing edge data, which in turn is used in the `edges` input field. Providing a 'nodes' information table is optional.

The second option of creating a `network_data` value is to use the `create.network_data.from.file` operation, which takes a (raw) `file` as input. This file needs to contain network data in one of the supported formats (e.g. 'gml, 'gexf', 'graphml', ... -- use 'explain' on the operation to get the latest list of supported formats).


## Other perations for `network_data` values

The following operations are available for `network_data` values. Use the `operation explain` command to get more information about them.

### `export.network_data.*`

Those operations take an existing `network_data` instance and export it as afile (or files) to the local filesystem, optionally including *kiara* specific metadata.

### `network_data.calculate_components`

Add a `_component_id` column to the nodes table indicating which (separate) component it belongs to, for single component networks thie value will be '0' for every node.

### `network_data_filter.component`

Filter a `network_data` instance by extracting a single component.
