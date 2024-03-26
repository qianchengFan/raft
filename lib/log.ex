
# Pengfei Xiao (px23) and Qiancheng Fan (qf23)


# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Log do

# implemented as a Map indexed from 1.

def new()                     do Map.new() end                      # used when process state is initialised
def new(server)               do Map.put(server, :log, Map.new) end # not currently used
def new(server, log)          do Map.put(server, :log, log) end     # only used below

def last_index(server)        do map_size(server.log) end
def entry_at(server, index)   do server.log[index] end
def request_at(server, index) do server.log[index].request end
def term_at(_server, 0)       do 0 end
def term_at(server, index)    do server.log[index].term end
def last_term(server)         do Log.term_at(server, Log.last_index(server)) end

def get_entries(server, range) do                 # e.g return server.log[3..5]
  Map.take(server.log, Enum.to_list(range))
  # equivalent to
  #   for k <- range.first .. range.last // 1, into: Map.new do {k, Log.entry_at(server, k)} end
end

def get_entries_from(server, from) do               # e.g return server.log[3..]
  for k <- from .. Log.last_index(server) // 1, into: Map.new do
    {k, Log.entry_at(server, k)}
  end
end

def append_entry(server, entry) do
  Log.new(server, Map.put(server.log, Log.last_index(server)+1, entry))
end

def merge_entries(server, entries) do               # entries should be disjoint
  Log.new(server, Map.merge(server.log, entries))
end

def delete_entries(server, range) do                 # e.g. delete server.log[3..5] keep rest
  Log.new(server, Map.drop(server.log, Enum.to_list(range)))
end

def delete_entries_from(server, from) do             # delete server.log[from..last] keep rest
  Log.delete_entries(server, from .. Log.last_index(server) // 1 )
end

end # Log
