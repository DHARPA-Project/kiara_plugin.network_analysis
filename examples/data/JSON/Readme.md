# A collection of sample JSON files for testing


### peacetreaties
This file was created by [Lena Jaskov](https://github.com/yaslena). It is is based on data that is available online in a database focused on European peace treaties from 1450 to 1789 (https://www.ieg-friedensvertraege.de/vertraege).

The database was created as part of a project that was funded by the DFG (German Research Foundation)
and contains a selection of some 1800 treaties. One extra treaty between Tsarist Russia and the Qing Empire (1689) was manually added by Lena Jaskov.

The data sample here only shows a selection of that data, i.e. filterd on treaties that involve Russia.
There are only edges between treaties and countries. If a country was involved in a treaty, there will be an edge between that country and the respective treaty.

The data was first scraped into a table in a first step. The original scraped table (CSV) can be found in this [repository](https://github.com/DHARPA-Project/kiara_plugin.network_analysis/blob/develop/examples/data/treaties). The scraping code can be found [here](https://github.com/yaslena/WebScraping).
In a second step the data from the scraping process was restructured to fit the requirements of creating a dynamic bipartite graph with networkX.
Code for restructuring the table data and for generating a bipartite graph with python networkX can be found [here](https://github.com/yaslena/NetworkAnalysis).
The restructured data was converted into JSON with networkX for visualization in JavaScript on [Observable](https://observablehq.com/@yaslena/dynamic-network-graph).

The data represents a **two-mode** graph that is **dynamic** ('time-specific'), **bipartite**, **undirected** and **unweighted**. The bipartite graph makes it possible to create a **projected** **one-mode** graph that would consist of only countries as nodes and have connections between them when they have a teaty in common.

- 120 nodes (two types, countries and treaties)
- 203 edges (undirected)

### radicaltranslations

This file is too large, it is therefore located in a different [repository](https://github.com/DHARPA-Project/kiara.examples/tree/main/examples/data/network_analysis/JSON).

This network is based on the work of The Radical Translations Project (https://radicaltranslations.org/).

An interactive visualisation can be found here:

https://observablehq.com/@jmiguelv/radical-translations-agents-network-visualisation

The visualisation shows a network of – *person (f), person (m), person (u), organisation, place, serial publication* – nodes, and how they are connected to one another via the relationships *based in (place), edited, knows, member of, published, published in (place), translated*. The size of the nodes corresponds to the number of connections the node has.

This network represents a **multi-mode network** with **several node-types** (person, organisation, place, serial publication) and **several edge (or relationship) types** (based_in, edited, knows, member of, published, translated..)

The dataset is a **json file** that seems to be the output of the the project's online database (https://radicaltranslations.org/about/database/). There is also **zip file** available for download on the website. The zip file contains several CSV files, one for each of the main data types (Agents, Events, Places and Resources) in the project.

- 1495 nodes (multiple types)
- 6675 edges (multiple types)
