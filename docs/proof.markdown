---
layout: page
title: Proof
permalink: /proof/
---


To prove that the incremental algorithm is correct, we need to show that the batch algorithm applied on the graph database at time $$t+1$$ ($$GDB_{t+1}$$) returns the same graph summary $$SG$$ as the incremental algorithm first applied on $$GDB_{t}$$ and then applied on $$GDB_{t+1}$$, i.e.,
\$$ ParallelSummarize (GDB_{t+1}, \emptyset, \sim) = ParallelSummarize (GDB_{t+1}, ParallelSummarize (GDB_{t}, \emptyset, \sim), \sim) $$



We denote by $$\emptyset$$ the empty (summary) graph, by $$V_t$$ the vertices in GDB at time $$t$$, by $$SG_{batch}$$ the result of the batch computation $$ParallelSummarize(GDB_{t+1}, \emptyset, \sim)$$, and by $$SG_{incr}$$ the result of the incremental computation $$ParallelSummarize(GDB_{t+1}, ParallelSummarize(GDB_t, \emptyset, \sim), \sim)$$.
It can be shown by induction for all $$t \in \mathbb{N}$$ that $$SG_{batch} \subseteq SG_{incr}$$ and $$SG_{incr} \subseteq SG_{batch}$$, with $$\subseteq$$ denoting the subgraph relation.
