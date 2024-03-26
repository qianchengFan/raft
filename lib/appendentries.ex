
# Pengfei Xiao (px23) and Qiancheng Fan (qf23)

# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule AppendEntries do

# .. omitted

  def send_append_entries(s,q) do
    s=Timer.restart_append_entries_timer(s,q)
    lastLogIndex_q = s.next_index[q]
    lastLogIndex = min(lastLogIndex_q,map_size(s.log))

    IO.inspect(lastLogIndex_q, label: "lastLogIndex_q value")
    #lastLogIndex_q_1 = lastLogIndex_q - 1
    #IO.puts("lastLogIndex_q-1 : #{lastLogIndex_q_1}")

    #Log.term_at(s, lastLogIndex_q - 1)


    s |> Debug.sent({ :APPEND_ENTRIES_REQUEST, {s.curr_term,
    lastLogIndex-1,
    Log.term_at(s, lastLogIndex_q - 1),
    Log.get_entries(s,lastLogIndex..Log.last_index(s)),
    s.commit_index} }, 3)



    send q, {
      :APPEND_ENTRIES_REQUEST, s.selfP,
        {s.curr_term,
        lastLogIndex-1,
        Log.term_at(s, lastLogIndex_q - 1),
        Log.get_entries(s,lastLogIndex..Log.last_index(s)),
        s.commit_index}}
    s
  end # send_append_entries

  def append_request_execution(
    sender,
    s,
    {currTerm,
    prevIndex,
    logTermPrev,
    entries,
    commitIndex}) do
    sender = if currTerm >= sender.curr_term do
      sender
      |> State.update_state_term(currTerm)
      |> State.leaderP(s)
    else
      Timer.restart_election_timer(sender)
    end

    sender |> Debug.info("new leaderP: #{inspect sender.leaderP}", 4)

    new_sender = if currTerm < sender.curr_term do
      send s, {:APPEND_ENTRIES_REPLY, sender.selfP, sender.curr_term, false, 0}
      sender
    else
      success = prevIndex==0 || (prevIndex < map_size(sender.log) && Log.term_at(sender,prevIndex) == logTermPrev)
      {sender_2, index} = if success do
        # follower update commit_index
        {sender_1, index_1} = store_entries(sender,prevIndex,entries,commitIndex)
        {apply_commits(sender_1), index_1}
      else
        {sender, 0}
      end
      send s, {:APPEND_ENTRIES_REPLY, sender_2.selfP, sender.curr_term, true, index}
      sender_2
    end
    new_sender
  end # append_request_execution

  def store_entries(sender,prevIndex,entries,c) do
    sender = Log.delete_entries(sender, prevIndex+1..map_size(sender.log)+1)
      |> Log.merge_entries(entries)
      |> State.commit_index(min(c,map_size(sender.log)))
      {sender, map_size(sender.log)}
  end # store_entries

  # 1 leader --> n followers
  def append_entry_timeout_execution(server, followerP) do
    # only follower valid, heartbeat(empty package) to keepalive
    if server.role == :LEADER do
      server
        |> Debug.info("Leader sending append entries request to #{inspect followerP}", 3)
        |> AppendEntries.send_append_entries(followerP)
    else
      server
    end
  end # append_entry_timeout_execution

  # follower --> leader, Repairing follower log
  def append_reply_execution(leader_server, follower_server, currentTerm, success, index) do
    leader_server = State.update_state_term(leader_server, currentTerm)
    new_leader_server = if leader_server.role == :LEADER && leader_server.curr_term == currentTerm do
      leader_server_1 = if success do
        leader_server
        |> State.next_index(follower_server, index + 1)
        |> State.match_index(follower_server, index) # commonly match_index[i] + 1 = next_index[i]
      else
        leader_server
        |> State.next_index(follower_server, max(1, leader_server.next_index[follower_server]-1))
      end

      leader_server_2 = if leader_server_1.next_index[follower_server] <= Log.last_index(leader_server_1) do
        send_append_entries(leader_server_1, follower_server)
      else
        leader_server_1
      end
      leader_server_2
    else
      leader_server
    end
    commit_to_db(new_leader_server)
  end # append_reply_execution

  # Safely commit: with majority match index more than specific index
  # only leader
  def commit_to_db(server) do
    left_edge = server.commit_index + 1
    right_edge = Log.last_index(server)
    server |> Debug.info("left_edge: #{inspect left_edge}, right_edge: #{inspect right_edge}", 3)
    server |> Debug.info("last_index: #{inspect Log.last_index(server)}, last_term: #{inspect Log.last_term(server)}", 3)

    commited_server = if left_edge >= right_edge do
      server
    else
      checklist = Log.get_entries(server, left_edge..right_edge)
      new_commit_index_loop = Enum.reduce_while(checklist, left_edge, fn _, now_index ->
        # get sum of matched index (including itself)
        match_sum = Enum.reduce(server.servers, 0, fn element, acc ->
          new_acc = if element == server || server.match_index[element] >= now_index do
            acc + 1
          else
            acc
          end
          new_acc
        end)
        res = if match_sum >= server.majority do
          {:cont, now_index + 1}
        else
          {:halt, now_index}
        end
        res
      end)
      new_commit_index = new_commit_index_loop - 1
      new_server = State.commit_index(server, new_commit_index)
      applied_server = apply_commits(new_server)

      committed_list = Log.get_entries(applied_server, left_edge..new_commit_index)
      for {_, entry} <- committed_list do
        send entry.request_info.clientP, { :CLIENT_REPLY, %{cid: entry.request_info.cid, reply: :SUCCESS, leaderP: applied_server.selfP} }
      end
      applied_server
    end
    commited_server
  end # commit_to_db

  defp apply_commits(new_server) do
    # commit new server state to database
    applied_server = if new_server.last_applied == new_server.commit_index do
      new_server
    else
      applylist = Log.get_entries(new_server, (new_server.last_applied+1)..(new_server.commit_index))
      new_applied_server = Enum.reduce(applylist, new_server, fn {_, entry}, server_i ->
        # find existed cids in applied, avoid dupulicate request
        new_server_i = if MapSet.member?(server_i.applied, entry.request_info.cid) do
          server_i
        else
          server_i |> Debug.sent({ :DB_REQUEST, entry.request_info }, 3)
          send server_i.databaseP, {:DB_REQUEST, entry.request_info}
          State.applied(server_i, entry.request_info.cid)
        end
        new_server_i
      end)
      updated_server = State.last_applied(new_applied_server, new_applied_server.commit_index)
      updated_server
    end
    applied_server |> Debug.info("server.commit_index: #{inspect applied_server.commit_index}", 3)
  end #apply_commits

end # AppendEntries
