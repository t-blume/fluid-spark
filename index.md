# FLUID: FLexible graph sUmmarIes for Data graphs

## A Common Model for Semantic Structural Graph Summariesbased on Equivalence Relations

The task of structural graph summarization is to compute a concise while at the same time meaningful synopsis of the key structural information of the graph.
Graph summaries are condensed representations of graphs such that a chosen set of (structural) subgraph features in the graph summary are equivalent to the input graph. 

As there are many tasks regarding what information is to be summarized from a graph, there is no single concept or model of graph summaries.
We have studied existing structural graph summaries for large (semantic) graphs.
Despite the different purposes and concepts followed by the existing graph summaries, we found common patterns in the captured graph structures.
We abstract from these patterns and provide for the first time a formally defined common model FLUID (FLexible graph sUmmarIes for Data graphs) that allows to flexibly define structural graph summaries.
FLUID allows to quickly define, adapt, and compare different graph summaries for different purposes and datasets. 
To this end, FLUID provides features of structural summarization based on equivalence relations such as distinction of types and properties, direction of edges, bisimulation, and inferencing.

## Indexing Data on the Web: A Comparison of Schema-level Indices for Data Search
Indexing the Web of Data offers many opportunities, in particular, to find and explore data sources. 
One major design decision when indexing the Web of Data is to find a suitable index model, i.e., how to index and summarize data. 
We implemented the FLUID model into a single, stream-based framework. 
We implemented the schema computation in a pipeline architecture following the basic principles of the SchemEX approach [2]. 
However, we redesigned the approach in a way that allows to abstract from the stream-based computation approach.

The figure below outlines the basic concept of the FLUID approach. All modules, e.g., the schema computation, can be changed and implemented differently.

![framework-architecture](documents/images/fluid-framework-concept.png)
In our research, we evaluate variations of six index models considering sub-graphs of size 0, 1, and 2 hops on two large, real-world datasets [DEXA].




## Incremental and Parallel Computation of Structural Graph Summaries for Evolving Graphs
Existing graph summarization algorithms are tailored to specific graph summary models, only support one-time batch computation, are designed and implemented for a specific task, or evaluated using static graphs. 
Our novel, incremental, parallel algorithm addresses all these shortcomings. 
Our algorithm solves the set union problem following the signal and collect programming model.
We support all structural graph summary models that are defined in FLUID. 
All graph summaries can be updated in $\mathcal{O}(\Delta \cdot d^k)$, where $\Delta$ is the number of additions, deletions, 
and modifications in the input graph, $d$ is the maximum degree of the input graph, and $k$ is the maximum distance in the subgraphs considered. 
We empirically evaluate the performance of our algorithm on benchmark and real-world datasets. 
Our experiments show that for commonly used summary models and datasets, the incremental summarization algorithm almost always outperforms their batch counterpart, 
even when about $50\%$ of the graph database changes. 
The source code and the experimental results are openly available for reproducibility and extensibility.

### References

1. Blume, T., Scherp, A.: Towards flexible indices for distributed graph data: The formal schema-level index model FLuID. In: 30th GI-Workshop on Foundations of Databases (Grundlagen von Datenbanken). CEUR Workshop Proceedings (2018), http://ceur-ws.org/Vol-2126/paper3.pdf.
2. Konrath, M., Gottron, T., Staab, S., Scherp, A.: SchemEX - efficient construction of a data catalogue by stream-based indexing of Linked Data. J. Web Sem. 16, 52–58 (2012)
3. Blume, T., Scherp, A.: FLuID: A Meta Model to Flexibly Define Schema-level Indices for the Web of Data. CoRR abs/1908.01528 (2019)

### Acknowledgments
This research was co-financed by the EU H2020 project [MOVING](http://www.moving-project.eu/) under contract no 693092.
