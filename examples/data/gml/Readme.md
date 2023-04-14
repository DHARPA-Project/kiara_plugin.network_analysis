# A collection of sample gml files for testing

Most gml files will be from Marc Newmans GML dataset collection: http://www-personal.umich.edu/~mejn/netdata/

### lesmis
"The file lesmis.gml contains the weighted network of coappearances of
characters in Victor Hugo's novel "Les Miserables".  Nodes represent
characters as indicated by the labels and edges connect any pair of
characters that appear in the same chapter of the book.  The values on the
edges are the number of such coappearances.  The data on coappearances were
taken from D. E. Knuth, The Stanford GraphBase: A Platform for
Combinatorial Computing, Addison-Wesley, Reading, MA (1993)."

As explained in the file description this data represents a coappearance network. This makes it an **undirected** network. It is furthermore a **weighted** network, edge weights are named "value" in the gml file. This is a **one-mode** network because there is only one type of nodes (book characters).
- 77 nodes
- 254 edges (undirected)

### karate
"The file karate.gml contains the network of friendships between the 34
members of a karate club at a US university, as described by Wayne Zachary
in 1977.  If you use these data in your work, please cite W. W. Zachary, An
information flow model for conflict and fission in small groups, Journal of
Anthropological Research 33, 452-473 (1977)."

See also: https://en.wikipedia.org/wiki/Zachary%27s_karate_club#cite_note-Data-3

This is a **one-mode** network where nodes represent friends. It is **undirected** and **unweighted**. 
- 34 nodes
- 78 edges (undirected)

### celegansneural
*Note: The multigraph tag has been manually added to the original gml file so that it can be parsed by the networkX 3.0 code.*

"Neural network of the nematode C. Elegans

Compiled by Duncan Watts and Steven Strogatz from original experimental
data by White et al.

The file celegansneural.gml describes a weighted, directed network
representing the neural network of C. Elegans.  The data were taken from
the web site of Prof. Duncan Watts at Columbia University,
http://cdg.columbia.edu/cdg/datasets.  The nodes in the original data were
not consecutively numbered, so they have been renumbered to be consecutive.
The original node numbers from Watts' data file are retained as the labels
of the nodes.  Edge weights are the weights given by Watts.

These data can be cited as:
J. G. White, E. Southgate, J. N. Thompson, and S. Brenner, "The structure
of the nervous system of the nematode C. Elegans", Phil. Trans. R. Soc.
London 314, 1-340 (1986).

D. J. Watts and S. H. Strogatz, "Collective dynamics of `small-world'
networks", Nature 393, 440-442 (1998)."

A **one-mode** network where nodes represent neurons. It is a **directed** **multigraph** (14 parallel edges) that is **weighted**.
- nodes 297
- edges 2359 (directed, multi)
