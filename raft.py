class Raft:
    # thinking about the concepts
    # server state
    # persisted / volatile
    # need to be able to persist and load state on node startup
    # RPCs
    # need to be able to send and receive them: serialize, deserialize
    # Nodes: can be in different states

    # big picture:
    # - leader receiving commands.
    # Maybe followers receiving commands and redirecting but for now probably ignore that
    # - Nodes can execute commands (when committed) Feels like this just comes from the application?
    # passing some 'execute_command' callback?

    # Hmmm I'm not really reproducing figure 1.

    def __init__(self):
        self.state_machine = ...  # the application
        self.consensus_module = ...  # ? the raft algorithm?
        # I don't really understand how the log could live outside the raft algorithm object
        self.log = []  # replicated log

    def execute_command(self, command):
        self.state_machine.execute_command(command)
