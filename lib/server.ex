
# Pengfei Xiao (px23) and Qiancheng Fan (qf23)


# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Server do

  # _________________________________________________________ Server.start()
  def start(config, server_num) do

    config = config
      |> Configuration.node_info("Server", server_num)
      |> Debug.node_starting()
    # Task.start_link(fn -> periodic_sleep(15000, 2000) end)
    receive do
    { :BIND, servers, databaseP } ->
      config
      |> State.initialise(server_num, servers, databaseP)
      |> Debug.info(servers, 4)
      |> State.init_next_index()
      |> State.init_match_index()
      |> Timer.restart_election_timer()
      |> Server.next()
    end # receive

  end # start

  # _________________________________________________________ next()
  def next(server) do

    # invokes functions in AppendEntries, Vote, ServerLib etc

    server = receive do


    # { :APPEND_ENTRIES_REQUEST, {term, lastLogIndex, termAtQ, entries, commitedIndex}} ->
    { :APPEND_ENTRIES_REQUEST, s, data} ->
      server  |> Debug.received({ :APPEND_ENTRIES_REQUEST }, 3)
              |> AppendEntries.append_request_execution(s, data)

    # { :APPEND_ENTRIES_REPLY, ...
    { :APPEND_ENTRIES_REPLY, follower_server, currentTerm, success, index } ->
      server  |> Debug.received({ :APPEND_ENTRIES_REPLY }, 3)
              |> AppendEntries.append_reply_execution(follower_server, currentTerm, success, index)

    # { :APPEND_ENTRIES_TIMEOUT, ...
    { :APPEND_ENTRIES_TIMEOUT, message } ->
      server |> Debug.received({ :APPEND_ENTRIES_TIMEOUT, {message.term, message.followerP} }, 3)
             |> AppendEntries.append_entry_timeout_execution(message.followerP)
    # { :VOTE_REQUEST, ...
    # server.role is not sure
    { :VOTE_REQUEST, {candidate, termC, lastLogTermC, lastLogIndexC} } ->
      server  |> Debug.received({ :VOTE_REQUEST, {candidate, termC, lastLogTermC, lastLogIndexC} }, 3)
              |> Vote.request(candidate, termC, lastLogTermC, lastLogIndexC)

    # { :VOTE_REPLY, ...
    # server.role == :CANDIDATE
    { :VOTE_REPLY, {voter, termV, voted_for} } ->
      server  |> Debug.received({ :VOTE_REPLY, {voter, termV, voted_for} }, 3)
              |> Vote.reply(voter, termV, voted_for)

    # { :ELECTION_TIMEOUT, ...
    { :ELECTION_TIMEOUT, mp } ->
      #IO.puts("look here term:#{mp.term}, election:#{mp.election}")
      server  |> Debug.received({ :ELECTION_TIMEOUT, {mp.term, mp.election} }, 3)
              |> Vote.election_timeout()

    # { :CLIENT_REQUEST, ...
    { :CLIENT_REQUEST, request } ->
      server  |> Debug.received({ :CLIENT_REQUEST, request }, 3)
              |> ClientRequest.get_request(request)

    # { :DB_REPLY, ...
    { :DB_REPLY, db_result } ->
      server |> Debug.received({ :DB_REPLY, db_result }, 3)

     unexpected ->
        Helper.node_halt("***** Server: unexpected message #{inspect unexpected}")
    end # receive

    server |> Server.next()

  end # next
  # defp periodic_sleep(sleep_interval, sleep_duration) do
  #   :timer.sleep(sleep_interval)
  #   IO.puts("Server sleeping for #{sleep_duration} ms...")
  #   :timer.sleep(sleep_duration)
  #   IO.puts("Server woke up from sleep.")
  #   periodic_sleep(sleep_interval, sleep_duration)
  # end
  end # Server
