# A collection of sample JSON files for testing


### peacetreaties
This file was created by [Lena Jaskov](https://github.com/yaslena). It is is based on data contained in a database focused on 
European peace treaties from 1450 to 1789 (https://www.ieg-friedensvertraege.de/vertraege).

The database was created as part of a project that was funded by the DFG (German Research Foundation) 
and contains a selection of some 1800 treaties. One extra treaty between Tsarist Russia and the Qing Empire (1689) was manually added by Lena Jaskov.

The data sample here only shows a selection of that data, i.e. fileterd on treaties that involve Russia. 
There are only edges between treaties and countries. If a country was involved in a treaty, there will be an edge between them.

The data was first scraped into a table in a first step. The original scraped table (CSV) can be found in this [repository](https://github.com/DHARPA-Project/kiara_plugin.network_analysis/blob/develop/examples/data/treaties). The scraping code can be found [here](https://github.com/yaslena/WebScraping). 
In a second step the data from the scraping process was restructured to fit the requirements of creating a dynamic bipartite graph with networkX.
Code for restructuring the table data and for generating a bipartite graph with python networkX can be found [here](https://github.com/yaslena/NetworkAnalysis).
The restructured data was converted into JSON with networkX for visualization in JavaScript on [Observable](https://observablehq.com/@yaslena/dynamic-network-graph)

The data represents a **two-mode** graph that is **dynamic** ('time-specific'), **bipartite**, **undirected** and **unweighted**.

- 120 (two types, countries and treaties)
- 203 edges (undirected)
