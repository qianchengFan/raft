
# Pengfei Xiao (px23) and Qiancheng Fan (qf23)

# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule ClientRequest do
  def get_request(server, request) do
    server |> Debug.info("get request from client", 4)
    val = Enum.filter(server.log, fn {_, entry} -> entry.request_info.cid == request.cid end)
    if val == [] do
      new_server = if server.role == :LEADER do
        # random_int = Enum.random(1..100)
        # if random_int == 100 do
        #   Debug.info(server, "server crash",3)
        #   exit(:normal)
        # end
        server |> Monitor.send_msg({ :CLIENT_REQUEST, server.server_num } )
               |> Debug.sent({ :CLIENT_REPLY, %{cid: request.cid, reply: :LEADER, leaderP: server.leaderP} }, 3)
        entry = %{term: server.curr_term, request_info: request}
        server_1 = Log.append_entry(server,entry)
        server_2 = Enum.reduce(server_1.servers, server_1, fn q, res_server ->
          if q != res_server.selfP do
            AppendEntries.send_append_entries(res_server,q)
          else
            res_server
          end
        end)
        #send request.clientP, { :CLIENT_REPLY, %{cid: request.cid, reply: "test client request.clientP", leaderP: server_2.leaderP} }
        server_2
      else
        if server.leaderP != nil do
          send request.clientP, { :CLIENT_REPLY, %{cid: request.cid, reply: :NOT_LEADER, leaderP: server.leaderP} }
          server |> Debug.sent({ :CLIENT_REPLY, %{cid: request.cid, reply: :NOT_LEADER, leaderP: server.leaderP} }, 3)
        end
        server
      end
      new_server
    else
      server
    end
  end # get_request

end # ClientRequest
