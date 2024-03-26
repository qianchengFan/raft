
# Pengfei Xiao (px23) and Qiancheng Fan (qf23)


# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Vote do

  # .. omitted

  # maybe redefine in other files

  # timeout = 500 # heartbeat

  # candidate -> voter
  def request(voter, candidate, termC, lastLogTermC, lastLogIndexC) do
    # candidate information is latest, update server term
    voter = voter |> State.update_state_term(termC) |> Debug.info("candidate #{inspect candidate} -> test", 1)

    voter_1 = if termC == voter.curr_term && (voter.voted_for == candidate or voter.voted_for == nil) do
      new_voter = State.curr_term(voter, termC)
      lastLogTermV = Log.last_term(voter)
      lastLogIndexV = Log.last_index(voter)

      if(lastLogTermC < lastLogTermV || (lastLogTermC == lastLogTermV && lastLogIndexC <= lastLogIndexV)) do
        new_voter |> Debug.sent({ :VOTE_REPLY, {self(), termC, candidate} }, 3)
        new_voter_to = new_voter |> State.voted_for(candidate) |> Timer.restart_election_timer() |> Debug.sent({ :VOTE_REPLY, {self(), termC, candidate} }, 3)
        send candidate, { :VOTE_REPLY, { self(), termC, candidate}}
        new_voter_to
      else
        new_voter
      end

    else # candidate out-of-date
      voter
    end
    voter_1
  end # request

  # voter -> candidate
  def reply(candidate, voter, termV, voted_for) do
    candidate = candidate |> State.update_state_term(termV) |> Debug.info("VOTE_REPLY, voter #{inspect voter}", 1)
    candidate |> Debug.info("reply, candidate curr_term: #{candidate.curr_term}, termV: #{termV}, candidate.role: #{candidate.role}", 3)

    updated_candidate_final = if  candidate.curr_term == termV &&
                            candidate.role == :CANDIDATE &&
                            candidate.selfP == voted_for &&
                            not Map.has_key?(candidate.voted_by, voter) do
      updated_candidate = candidate |> Debug.info("add_to_voted_by, voter_num: #{State.vote_tally(candidate)+1}", 3)
      |> State.add_to_voted_by(voter) |> Timer.cancel_append_entries_timer(voter)

      updated_candidate_2 = if State.vote_tally(updated_candidate) >= updated_candidate.majority do
        updated_candidate |> Debug.info("leader get selected", 4)
        |> State.role(:LEADER) |> State.leaderP(candidate.selfP)
      else
        updated_candidate
      end
      updated_candidate_2

    else
      candidate
    end
    updated_candidate_final
  end # reply

  def election_timeout(server) do
    server |> Debug.info("Election timer... starting election, role: #{server.role}", 4)
    res_server = if server.role == :FOLLOWER || server.role == :CANDIDATE do
      new_server = Timer.restart_election_timer(server)
        |>State.leaderP(nil)
        |>State.inc_term()
        |>State.inc_election()
        |>State.role(:CANDIDATE)
        |>State.new_voted_by()
        |>State.voted_for(server.selfP)
        |>State.add_to_voted_by(server.selfP)
        |>Timer.cancel_all_append_entries_timers()

      new_server |> Debug.info("Election stage 1, role: #{new_server.role}", 4)
      new_server |> Debug.info(new_server.servers, 4)

      for neighbour <- new_server.servers do
        if neighbour != self() do
          new_server |> Debug.info("Election stage 2, role: #{new_server.role}", 4)

          #self() |> Debug.info("Election neighbour", 4)
          send neighbour, {:VOTE_REQUEST, {self(),new_server.curr_term, Log.last_term(new_server), Log.last_index(new_server)} }
        end
      end
      new_server
    else
      server
    end
    res_server
  end # election_timeout

end # Vote
