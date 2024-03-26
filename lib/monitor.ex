
# Pengfei Xiao (px23) and Qiancheng Fan (qf23)


# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Monitor do

# _________________________________________________________ Monitor setters()
def clock(monitor, v)       do Map.put(monitor, :clock, v) end
def requests(monitor, k, v) do Map.put(monitor, :requests, Map.put(monitor.requests, k, v)) end
def updates(monitor, k, v)  do Map.put(monitor, :updates, Map.put(monitor.updates, k, v)) end
def moves(monitor, v)       do Map.put(monitor, :moves, v) end
def moves(monitor, k, v)    do Map.put(monitor, :moves, Map.put(monitor.moves, k, v)) end

# _________________________________________________________ Monitor.start()
def start(config) do
  Process.send_after(self(), { :PRINT }, config.monitor_interval)
  monitor = %{
    config:    config,
    clock:     0,
    requests:  Map.new,
    updates:   Map.new,
    moves:     Map.new,
  }
  monitor |> Monitor.next()
end # start

# _________________________________________________________ Monitor next()
def next(monitor) do
  receive do
  { :DB_MOVE, db, seqnum, command } ->

    { :MOVE, amount, from, to } = command

    done = Map.get(monitor.updates, db, 0)

    if seqnum != done + 1 do
      Monitor.halt "  ** error db #{db}: seq #{seqnum} expecting #{done+1}"
    end # if

    monitor =
      case Map.get(monitor.moves, seqnum) do
      nil ->
        # IO.puts "db #{db} seq #{seqnum} = #{done+1}"
        monitor |> Monitor.moves(seqnum, %{ amount: amount, from: from, to: to })

      t -> # already logged - check command
        if amount != t.amount or from != t.from or to != t.to do
            Monitor.halt " ** error db #{db}.#{done} [#{amount},#{from},#{to}] " <>
              "= log #{done}/#{map_size(monitor.moves)} [#{t.amount},#{t.from},#{t.to}]"
        end # if
        monitor
      end # case

    monitor
    |> Monitor.updates(db, seqnum)
    |> Monitor.next()

  { :CLIENT_REQUEST, server_num } ->    # client requests seen by leaders
    value = Map.get(monitor.requests, server_num, 0)
    IO.puts "get request!!!!!!!!!!!!!!!!!!!"
    monitor
    |> Monitor.requests(server_num, value + 1)
    |> Monitor.next()

  { :PRINT, term, msg } ->
    IO.puts "term = #{term} #{msg}"
    monitor |> Monitor.next()

  { :PRINT } ->
    clock  = monitor.clock + monitor.config.monitor_interval

    monitor = monitor |> Monitor.clock(clock)

    sorted = monitor.requests |> Map.to_list |> List.keysort(0)
    IO.puts "  time = #{clock} client requests seen = #{inspect sorted}"
    sorted = monitor.updates  |> Map.to_list |> List.keysort(0)
    IO.puts "  time = #{clock}      db updates done = #{inspect sorted}"

    # if m.config.debug_level == 0 do
    #   min_done   = m.updates  |> Map.values |> Enum.min(fn -> 0 end)
    #   n_requests = m.requests |> Map.values |> Enum.sum
    #   IO.puts "  time = #{clock}           total seen = #{n_requests} max lag = #{n_requests-min_done}"
    # end

    IO.puts ""
    Process.send_after(self(), { :PRINT }, monitor.config.monitor_interval)
    monitor |> Monitor.next()

  # ** ADD ADDITIONAL MESSAGES HERE

  unexpected ->
     Monitor.halt "monitor: unexpected message #{inspect unexpected}"

  end # receive
end # next

# _________________________________________________________ Monitor.notify()
def send_msg(server, msg) do
  send server.config.monitorP, msg
  server
end # send_msg

# _________________________________________________________ Monitor.halt()
def halt(string) do
  Helper.node_halt("monitor: #{string}")
end # halt


end # Monitor
