# A collection of sample gexf files for testing

Most datasets come from here https://github.com/gephi/gephi/wiki/Datasets if not otherwise stated.

### lesmis
The lesmis gephi file represents the same data as the one in gml format found in 
Marc Newmans GML dataset collection: http://www-personal.umich.edu/~mejn/netdata/
It also has some additional graph data, like node positions, node colors (by modularity group), node size etc because the gexf format can also store that kind of data.

This gefx file is included as sample data with the installation of the Gephi software.
It seems to be a converted version of Marc Newmans GML file. The reference is the same as for the GML file:

"Les Miserables: coappearance weighted network of characters in the novel Les Miserables. 
D. E. Knuth, The Stanford GraphBase: A Platform for Combinatorial Computing, Addison-Wesley, Reading, MA (1993)."

As the title reveals this data represents a coappearance network. This makes it an **undirected** network. 
It is furthermore a **weighted** network, edge weights are named "value" in the gml file. 
This is a **one-mode** network because there is only one type of nodes (book characters).
- 77 nodes
- 254 edges (undirected)
