import time
from collections import deque
from collections import namedtuple
from queue import Queue
from threading import Thread

queue = Queue()


def producer():
    queue.put("job")
    time.sleep(5)
    producer()


def do_job(op: str):
    print(f"doing the {op}")


def process_queue():
    """
    Favoring recursive calls instead of
    while True:
        op = queue.get()
        do_job(op)

    It's a minor change that might make things a little simpler.
    --> Except it does not work great with the recursion limit!
    Cannot use that in a thread that is meant to run indefinitely.
    """

    op = queue.get()
    do_job(op)
    process_queue()


def run():
    Thread(target=producer).start()
    Thread(target=process_queue).start()


# Goal: simulate the code above in a single process.
# The simulation should be deterministic: running twice with the same inputs
# should produce *exactly* the same outputs, with the same timings, etc.


# thinking: how should the code run?
# we see we have two threads. producer, process_queue.
# we schedule producer and process_queue to run at time 0
# process queue:
# while True: queue.get()...
# That means every time something is added to the queue we want to run the
# code after queue.get.
# ...as soon as the thread is not busy. Do we need to keep track of that too?

# at t=0, we run producer:
# 1. queue.put("job"): ok
# 2. time.sleep(5): we schedule **everything after this time.sleep**
# for execution at current time + 5.
# Maybe step 1 is overly simplistic and queue.put("job") should also schedule a callback for execution.
# The callback is the `queue.get` stuff.
# hard to see how we can connect the two though.

"""
Thinking about what a simulation would do

there's a clock
events are registered on a timeline
Creating a node causes many events to be registered?
Like heartbeats if it's a leader/election timeout if it's not?


RaftNode(node_config)

simulation calls actual code to send/receive messages. Only it's mocked.
sending a message is adding an event to the timeline where the destination node receives the message (success case).

Simulation runs more than just RaftLogic.

"""
TimeSegment = namedtuple("TimeSegment", ["time", "events"])


class Simulation:
    def __init__(self):
        # a sorted collection of (time, deque(callbacks))
        self.agenda = deque()
        self.current_time = 0

    def run(self):
        while self.agenda:
            next_time_segment = self.agenda[0]
            # if next_time_segment.time != self.current_time:
            #     print(f"advancing time! making progress! {next_time_segment.time}")

            self.current_time = next_time_segment.time

            callback = next_time_segment.events.popleft()
            # call the thing. It may add new events to the simulation.
            callback()

            # remove time segment if empty
            if not next_time_segment.events:
                self.agenda.popleft()

        print("simulation ended")

    def _add_to_timeline(self, t, callback):
        import bisect

        times = [item.time for item in self.agenda]
        right_index = bisect.bisect_right(times, t)
        if right_index > 0 and times[right_index - 1] == t:
            # add the callback to the list in the existing time segment
            self.agenda[right_index - 1].events.append(callback)
        else:
            # create a new time segment
            self.agenda.insert(right_index, TimeSegment(t, deque([callback])))

    def after_delay(self, delay, callback):
        self._add_to_timeline(t=self.current_time + delay, callback=callback)

    def __str__(self):
        return f"Simulation({self.current_time}, {self.agenda})"

    def run_until(self, t_end):
        def stop_simulation():
            raise StopSimulation()

        self._add_to_timeline(t_end, stop_simulation)

        try:
            self.run()
        except StopSimulation:
            return


class StopSimulation(Exception):
    pass


def test_simulation():
    sim = Simulation()

    def assert_time(t):
        assert sim.current_time == t

    sim.after_delay(1, lambda: assert_time(1))
    sim.after_delay(2, lambda: sim.after_delay(1, lambda: assert_time(3)) or assert_time(2))

    sim.run()
