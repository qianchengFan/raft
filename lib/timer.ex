
# Pengfei Xiao (px23) and Qiancheng Fan (qf23)


# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Timer do

# _________________________________________________________ restart_election_timer()
def restart_election_timer(server) do  # election will be restarted if no VOTE or AE requests
  server = server |> Timer.cancel_election_timer()

  election_timeout = Enum.random(server.config.election_timeout_range)
  timeout_msg = { :ELECTION_TIMEOUT, %{term: server.curr_term, election: server.curr_election} }
  election_timer = Process.send_after(server.selfP, timeout_msg, election_timeout)

  server
  |> State.election_timer(election_timer)
  |> Debug.message("+etim", {"started", timeout_msg, election_timeout})
end # restart_election_timer

# _________________________________________________________ cancel_election_timer()
def cancel_election_timer(server) do
  if server.election_timer do
    Process.cancel_timer(server.election_timer)
  end # if
  server |> State.election_timer(nil)
end # cancel_election_timer



# _________________________________________________________ restart_append_entries_timer()
def restart_append_entries_timer(server, followerP) do
  server = Timer.cancel_append_entries_timer(server, followerP)

  timeout_msg = { :APPEND_ENTRIES_TIMEOUT, %{term: server.curr_term, followerP: followerP }}
  append_entries_timer = Process.send_after(server.selfP, timeout_msg, server.config.append_entries_timeout)

  server
  |> State.append_entries_timer(followerP, append_entries_timer)
  |> Debug.message("+atim", {"started", timeout_msg, server.config.append_entries_timeout})
end # restart_append_entries_timer

# _________________________________________________________ cancel_append_entries_timer()
def cancel_append_entries_timer(server, followerP) do
  if server.append_entries_timers[followerP] do
    Process.cancel_timer(server.append_entries_timers[followerP])
  end # if
  server |> State.append_entries_timer(followerP, nil)
end # cancel_append_entries_timer

# _________________________________________________________ cancel_all_append_entries_timers()
def cancel_all_append_entries_timers(server) do
  for followerP <- server.append_entries_timers do
    Timer.cancel_append_entries_timer(server, followerP)         # mutated result ignored
  end
  server |> State.append_entries_timers()                        # now reset to Map.new
end # cancel_all_append_entries_timers

end # Timer
