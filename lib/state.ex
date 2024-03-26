
# Pengfei Xiao (px23) and Qiancheng Fan (qf23)


# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule State do
  # state variables and setters for clients and the database are within their own module

# _________________________________________________________ State.initialise()
def initialise(config, server_num, servers, databaseP) do
  # initialise state variables for server
  %{
    # _____________________constants _______________________

    config:       config,             # system configuration parameters (from Helper module)
    server_num:   server_num,         # server num (for debugging)
    selfP:        self(),             # server's process id
    servers:      servers,            # list of process id's of servers
    num_servers:  length(servers),    # no. of servers
    majority:     div(length(servers),2) + 1,  # cluster membership changes are not supported in this implementation

    databaseP:    databaseP,          # local database - used to send committed entries for execution
    applied:      MapSet.new,         # cids of cmd already applied - used to filter duplicate requests

    # ______________ elections ____________________________
    election_timer:  nil,             # one timer for all peers
    curr_election:   0,               # used to drop old electionTimeout messages and votereplies
    voted_for:       nil,             # num of candidate that been granted vote incl self
    voted_by:        MapSet.new,      # set of processes that have voted for candidate incl. candidate

    append_entries_timers: Map.new,   # one timer for each follower

    leaderP:        nil,              # included in reply to client request

    # _______________raft paper state variables___________________

    curr_term:    0,                  # current term incremented when starting election
    log:          Log.new(),          # log of entries, indexed from 1
    role:         :FOLLOWER,          # one of :FOLLOWER, :LEADER, :CANDIDATE
    commit_index: 0,                  # index of highest committed entry in server's log
    last_applied: 0,                  # index of last entry applied to state machine of server

    next_index:   Map.new,            # foreach follower, index of follower's last known entry+1 - used by leader
    match_index:  Map.new,            # index of highest entry known to be replicated at a follower
  }
end # initialise

# ______________setters for mutable variables_______________

# log is implemented in Log module
# 's' is an abbreviation for 'server' below

def leaderP(s, v)         do Map.put(s, :leaderP, v) end
def election_timer(s, v)  do Map.put(s, :election_timer, v) end
def curr_election(s, v)   do Map.put(s, :curr_election, v) end
def inc_election(s)       do Map.put(s, :curr_election, s.curr_election + 1) end
def voted_for(s, v)       do Map.put(s, :voted_for, v) end
def new_voted_by(s)       do Map.put(s, :voted_by, MapSet.new) end
def add_to_voted_by(s, v) do Map.put(s, :voted_by, MapSet.put(s.voted_by, v)) end
def vote_tally(s)         do MapSet.size(s.voted_by) end

def append_entries_timers(s)
                          do Map.put(s, :append_entries_timers, Map.new) end
def append_entries_timer(s, k, v)
                          do Map.put(s, :append_entries_timers, Map.put(s.append_entries_timers, k, v)) end

def curr_term(s, v)       do Map.put(s, :curr_term, v) end
def inc_term(s)           do Map.put(s, :curr_term, s.curr_term + 1) end

def role(s, v)            do Map.put(s, :role, v) end
def commit_index(s, v)    do Map.put(s, :commit_index, v) end
def last_applied(s, v)    do Map.put(s, :last_applied, v) end

def next_index(s, v)      do Map.put(s, :next_index, v) end
def next_index(s, k, v)   do Map.put(s, :next_index, Map.put(s.next_index, k, v)) end
def match_index(s, v)     do Map.put(s, :match_index, v) end
def match_index(s, k, v)  do Map.put(s, :match_index, Map.put(s.match_index, k, v)) end

def applied(s, v)         do Map.put(s, :applied, MapSet.put(s.applied, v)) end

def init_next_index(s) do    # initialise when server becomes leader
  new_next_index = for server <- s.servers, into: Map.new do
    {server, Log.last_index(s)+1}
  end # for
  s |> State.next_index(new_next_index)
end # init_next_index

def init_match_index(s) do    # initialise when server becomes leader
  new_match_index = for server <- s.servers, into: Map.new do {server, 0} end
  s |> State.match_index(new_match_index)
end # init_match_index


def update_state_term(s, new_term) do    # update when out of date
  if s.curr_term < new_term do
    s
    |> curr_term(new_term)
    |> role(:FOLLOWER)
    |> voted_for(nil)
    |> Timer.cancel_all_append_entries_timers()
    |> Timer.restart_election_timer()
    |> Debug.message("update_state_term", {s, new_term})
  else
    s
  end



end # init_state

end # State
