multicore-mergesort
===================

Multicore Merge Sort using MPI to distribute the work across multiple cores.

This was a way to learn MPI and explore the performance limits of my machine.

All development/testing done on a late 2011 MacBook Pro i7 Quad core w/
hyperthreading.

This project investigates 2 approaches to parallel processing:

    Split/reduction (Scatter & Gather) - split up the problem evenly across n
cores, perform the the sort on each part in parallel, gather the results, and
n-way merge to get the final sorted array.

    Distributed Queue - add jobs to a queue that each core will query for things
to do.  This is more of a general approach to parallel work distribution, though
it is less efficient than Scatter & Gather for this particular task.
