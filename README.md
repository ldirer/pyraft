A Raft implementation in Python, written as part of [David Beazley's Raft course](https://dabeaz.com/raft.html).  
There's more to the course (smaller exercises to highlight key concepts, a key-value store layer), this is only my implementation of the core Raft algorithm following [the paper](https://raft.github.io/raft.pdf).

I really enjoyed the week-long course. I recommend it and would probably take it again some time.

Writing Raft is challenging! Debugging and testing distributed systems is notoriously difficult, this is a good introduction as to why.  

You can go in depth on many aspects of the implementation (networking, persistence, interfacing with clients...). 
My focus was on setting up simulation-based tests to spot typical concurrency bugs between Raft servers.

------------------------

I wrote this to experiment and learn. If you really want to run it:

- To test this manually, adjust configuration in `raftconfig.py` and launch:

      # in different terminals
      python console.py 1 leader
      python console.py 2 follower
      python console.py 3 follower

See console.py for available commands.

- Run tests with `pytest`.  
  The [test_simulated.py](test_simulated.py) file uses simulation testing.  
  I find it interesting to introduce subtle bugs and see if the tests can find them.
  Example: comment out this line (`raftlogic.py:254`): 
 
      and (self.state.voted_for is None or self.state.voted_for == msg.candidate_id)
 
  This allows servers to vote for multiple candidates and hopefully causes the simulation tests (at least) to fail.
