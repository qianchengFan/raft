
# Pengfei Xiao (px23) and Qiancheng Fan (qf23)

# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Debug do

# Options N (node starting), M (message), S (State)
#         a (print server or config map)

def kpad(key)      do String.pad_trailing("#{key}", 15) end
def lpad(line_num) do String.pad_leading("#{line_num}", 4, "0") end
def rpad(role)     do String.pad_trailing("#{role}", 9) end
def tpad(term)     do String.pad_leading("#{term}", 3, "0") end
def map(m)         do (for {k, v} <- m, into: "" do "\n\t#{kpad(k)}\t#{inspect v}" end) end

def option?(config, option, level) do
  String.contains?(config.debug_options, option) and config.debug_level >= level
end

def mapstr(config, mapname, mapvalue, level) do
  (if Debug.option?(config, "a", level) do "#{mapname} = #{map(mapvalue)}" else "" end)
end

def node_prefix(config) do
  "#{config.node_name}@#{config.node_location}"
end

def server_prefix(server) do
  "server#{server.server_num}-#{lpad(server.config.line_num)} role=#{rpad(server.role)} term=#{tpad(server.curr_term)}"
end

def inc_line_num(server) do
  Map.put(server, :config, Map.put(server.config, :line_num, server.config.line_num+1))
end

# _________________________________________________________ Debug.message()
def message(server, option, message, level \\ 1) do
  unless Debug.option?(server.config, option, level) do server else
    server = server |> Debug.inc_line_num()
    IO.puts "#{server_prefix(server)} #{option} #{inspect message}"
    server
  end # unless
end # message

# _________________________________________________________ Debug.received()
def received(server, message, level \\ 1) do
  server |> Debug.message("?rec", message, level)
end # received

# _________________________________________________________ Debug.sent()
def sent(server, message, level \\ 1) do
  server |> Debug.message("!snd",  message, level)
end # sent

# _________________________________________________________ Debug.received()
def info(server, message, level \\ 1) do
  server |> Debug.message("!inf", message, level)
end # received

# _________________________________________________________ Debug.state()
def state(server, msg, level \\ 2) do
  unless Debug.option?(server.config, "+state", level) do server else
    server = server |> Debug.inc_line_num()
    smap = Map.put(server, :config, "... OMITTED")
#   smap = Map.put(smap, :log, "... OMITTED")
    IO.puts "#{server_prefix(server)} #{msg} #{mapstr(server.config, "STATE", smap, level)}"
    server
  end # unless
end # state

# _________________________________________________________ Debug.node_starting()
def node_starting(config, level \\ 1) do
  if Debug.option?(config, "+node", level) do
    IO.puts("  Node #{node_prefix(config)} starting #{mapstr(config, "CONFIG", config, level)}")
  end # if
  config
end # node_starting

# _________________________________________________________ Debug.role()
def role(server, level \\ 3) do   # paint role each iteration of server
  if Debug.option?(server.config, "R", level) do
    IO.write %{FOLLOWER: "F", LEADER: "L", CANDIDATE: "C"}[server.role]
  end # if
  server
end # role

# _________________________________________________________ Debug.assert()
def assert(server, asserted, message) do
  unless asserted do
    Helper.node_halt("!!!! server #{server.server_num} assert failed #{message}")
  end # unless
  server
end # assert

end # Debug
