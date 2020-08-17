---
layout: page
title: Proof
permalink: /proof/
---
Below, you find the formal proof to show that the results of the incremental and the batch computation are the same.
It references the two pseudo code algorithms presented in the paper.
A link to the paper will follow shortly. 

To prove that the incremental algorithm is correct, we need to show that the batch algorithm applied on the graph database at time $$t+1$$ ($$GDB_{t+1}$$) returns the same graph summary $$SG$$ as the incremental algorithm first applied on $$GDB_{t}$$ and then applied on $$GDB_{t+1}$$, i.e.,

$$ ParallelSummarize (GDB_{t+1}, \emptyset, \sim) =\\ ParallelSummarize (GDB_{t+1}, ParallelSummarize (GDB_{t}, \emptyset, \sim), \sim) $$



We denote by $$\emptyset$$ the empty (summary) graph, by $$V_t$$ the vertices in GDB at time $$t$$, by $$SG_{batch}$$ the result of the batch computation $$ParallelSummarize(GDB_{t+1}, \emptyset, \sim)$$, and by $$SG_{incr}$$ the result of the incremental computation $$ParallelSummarize(GDB_{t+1}, ParallelSummarize(GDB_t, \emptyset, \sim), \sim)$$.
It can be shown by induction for all $$t \in \mathbb{N}$$ that $$SG_{batch} \subseteq SG_{incr}$$ and $$SG_{incr} \subseteq SG_{batch}$$, with $$\subseteq$$ denoting the subgraph relation.




#### Proof (by induction)
__Time $$t = 0$$:__
In Algorithm 2 Line 11, we guarantee that all vertices $$v \in V_{t}$$ are in the _VertexUpdateHashIndex_ after _ParallelSummarize_$$(GDB_t, \emptyset, \sim)$$ is completed.
Furthermore, there can be no vertex in _VertexUpdateHashIndex_ that is not in $$V_t$$.
Since we start with an empty graph summary $$SG = \emptyset$$, the result of _ParallelSummarize_$$(GDB_t, \emptyset, \sim)$$ is the result of a batch computation.

__Time $$t + 1$$:__
In Algorithm 2 starting in Line 6, we guarantee that modifications are handled correctly.
We update the _VertexUpdateHashIndex_ by deleting the old link between $$v$$ and $$vs_{prev}$$ and optionally delete $$vs_{prev}$$ if no link to $$vs_{prev}$$ exists anymore, \ie there exists no $$v \in V_{t+1}$$ that is summarized by $$vs_{prev}$$.
Therefore, $$vs_{prev}$$ is not in _ParallelSummarize_$$(GDB_{t+1}, \emptyset, \sim)$$.
Furthermore, since $$v$$ is not in the _VertexUpdateHashIndex_, we add a link to the new vertex summary $$vs$$ in Line 11.
Note that we do not explicitly have to follow all incoming edges of $$v$$ to update the vertex summaries that depend on the schema of vertex $$v$$ since the new schema information is already messaged in Algorithm 1 Line 6.
In Algorithm 2 Line 11, we guarantee that all vertices $$v \in V_{t+1}\setminus V_t$$ are added to the _VertexUpdateHashIndex_.
In Algorithm 2 Line 15, we guarantee that for all vertices $$v \in V_{t+1}\setminus V_t$$ the corresponding vertex summaries $$vs$$ are added to the graph summary $$SG_{incr} $$, if $$vs$$ is not already in $$SG_{incr} $$.
Thus, for all vertex summaries $$vs \subseteq SG_{batch} $$, there exists a vertex summary $$vs' \in SG_{incr} $$ with $$vs = vs'$$.
In Algorithm 2 Line 13, we guarantee that for all vertices $$v \in V_{t+1}\setminus V_t$$ the corresponding payload element $$pe$$ is added to the vertex summary $$vs$$.
Thus, for all $$se$$ in $$SG_{batch} $$ there is a $$se' \in SG_{incr} $$ with $$se = se'$$ and all information in $$pe$$ is also in $$pe'$$.
This means, \eg for data search that all label $$l \in pe$$ are also in $$pe'$$ and for cardinality computation that the counter $$c$$ in $$pe$$ is less or equal to the counter $$c'$$ in $$pe'$$.
Furthermore, we guarantee that all vertices $$v \in V_{t}\setminus V_{t+1}$$ are removed from the _VertexUpdateHashIndex_ and optionally the vertex summary $$vs$$ with its payload element $$pe$$ (the for loop is not shown in the pseudo code).
Thus, for all vertex summaries $$vs \subseteq SG_{incr} $$ and their payload elements $$pe$$, there exists a vertex summary $$vs' \in SG_{batch} $$ with a payload element $$pe'$$ with $$vs = vs'$$ and $$pe = pe'$$.
Thus, $$SG_{batch}  \subseteq SG_{incr} $$ and $$SG_{incr}  \subseteq SG_{batch} $$, \ie _ParallelSummarize_$$(GDB_{t+1}, \emptyset, \sim) =$$ _ParallelSummarize_$$(GDB_{t+1},$$ _ParallelSummarize_$$(GDB_t, \emptyset, \sim), \sim)$$ holds true for $$t=1$$.
Consequently, this holds true for all $$t \in \mathbb{N}\setminus 0$$.
