-- Run from the repository root: luajit util/test_failover.lua
package.path = './lib/?.lua;./lib/?/init.lua;' .. package.path

local data, locks, fault = {}, {}, nil
local clock, expirations, flags = 1, {}, {}
local reads = 0
local unlock_failure, unlocks, connections, prepare_queries
local shm = {
  get = function(_, key)
    if key:find('prepared:id:', 1, true) == 1 then
      reads = reads + 1
      if fault == 'read' and reads == 2 then return nil, 'injected read failure' end
    end
    if expirations[key] and expirations[key] <= clock then data[key], expirations[key] = nil, nil end
    return data[key], flags[key]
  end,
  safe_set = function(_, key, value)
    if (fault == 'write' or fault == 'no memory') and key:find('prepared:id:', 1, true) == 1 then
      return nil, fault == 'no memory' and fault or 'injected write failure'
    end
    data[key] = value
    return true
  end,
  safe_add = function(self, key, value, ttl)
    if fault == 'throttle' then return nil, 'no memory' end
    if self:get(key) ~= nil then return nil, 'exists' end
    data[key] = value
    if ttl then expirations[key] = clock + ttl end
    return true
  end,
  set = function(_, key, value, _, flag) data[key], flags[key] = value, flag; return true end,
  delete = function(_, key) data[key], expirations[key], flags[key] = nil, nil, nil end,
}
ngx = {
  shared = { cassandra = shm },
  now = function() return clock end,
  update_time = function() end,
  log = function() end,
}
package.loaded['resty.lock'] = {
  new = function()
    return {
      lock = function(self, key)
        if fault == 'lock' then return nil, 'injected lock failure' end
        assert(not locks[key], 'preparation lock leaked')
        self.key = key
        locks[key] = true
        return 0
      end,
      unlock = function(self)
        unlocks = unlocks + 1
        if unlock_failure then return nil, 'injected unlock failure' end
        locks[self.key] = nil
        return true
      end,
    }
  end,
}
-- Keep cache values in memory; wire encoding and JSON are outside this check.
package.loaded.cjson = { encode = function(v) return v end, decode = function(v) return v end }
local errors = {
  UNPREPARED = 0x2500, OVERLOADED = 0x1001, INVALID = 0x2200,
  READ_TIMEOUT = 0x1200, WRITE_TIMEOUT = 0x1100, UNAVAILABLE_EXCEPTION = 0x1000,
}
package.loaded['cassandra.cql'] = {
  errors = errors,
  requests = {
    query = { new = function(query) return { retries = 0, query = query } end },
    batch = { new = function(queries) return { retries = 0, queries = queries } end },
    execute_prepared = { new = function(_, _, _, query) return { retries = 0, query = query } end },
  },
}

local expected_keyspace, closed, pooled, attempts, preparations, scenario
local discovered_peers
package.loaded.cassandra = {
  get_request_opts = function(opts) return opts or {} end,
  new = function(opts)
    local host, keyspace = opts.host, opts.keyspace
    return {
      host = host,
      protocol_version = 4,
      settimeout = function() end,
      connect = function()
        connections[#connections + 1] = host
        if scenario == 'background_connect' and host == 'stopping' then return nil, 'closed', true end
        if scenario == 'dns_connect' and #connections == 1 then return nil, 'closed', true end
        if (scenario == 'connect_stale' or scenario == 'initial_refresh_closed') and host ~= 'replacement' then
          return nil, 'connection refused', true
        end
        return true
      end,
      close = function() closed[host] = true end,
      setkeepalive = function() pooled[host] = true end,
      execute = function(_, query)
        assert(scenario == 'background_move')
        if query:find('system.local', 1, true) then
          return { { rpc_address = '10.0.1.1', data_center = 'dc1' } }
        end
        return discovered_peers
      end,
      prepare = function(_, query)
        preparations[#preparations + 1] = host
        prepare_queries[#prepare_queries + 1] = host .. ':' .. query
        assert(keyspace == expected_keyspace)
        if scenario == 'prepare_timeout' then return nil, 'timeout' end
        if scenario == 'prepare_stale' and host ~= 'replacement' then return nil, 'closed' end
        if scenario == 'invalid' then return nil, 'invalid query', errors.INVALID end
        if scenario == 'batch_partial' and host == 'stopping' and query == 'second' then
          return nil, 'closed'
        end
        if host == 'stopping' and (scenario == 'prepare_closed' or scenario == 'reprepare_closed') then
          return nil, 'closed'
        end
        if scenario == 'batch_partial' then return { query_id = query .. '-id' } end
        return { query_id = 'new-id', meta = { metadata_id = 'new-metadata' } }
      end,
      send = function(_, request)
        attempts[#attempts + 1] = host
        assert(#attempts < 10, 'unbounded retries')
        if scenario == 'overloaded' then return nil, 'overloaded', errors.OVERLOADED end
        if scenario == 'timeout' then return nil, 'timeout' end
        if scenario == 'background_move' and host == '10.0.0.1' then return nil, 'closed' end
        if scenario == 'dns_closed' or (scenario == 'dns_recover' and #attempts == 1) then
          return nil, 'closed'
        end
        if scenario == 'transient_read' and #attempts == 1 then return nil, 'read timeout', errors.READ_TIMEOUT end
        if scenario == 'transient_write' and #attempts == 1 then return nil, 'write timeout', errors.WRITE_TIMEOUT end
        if scenario == 'transient_unavailable' and #attempts == 1 then
          return nil, 'unavailable', errors.UNAVAILABLE_EXCEPTION
        end
        if scenario == 'read_timeout' then return nil, 'read timeout', errors.READ_TIMEOUT end
        if scenario == 'closed_then_read' then
          if host == 'stopping' then return nil, 'closed' end
          return nil, 'read timeout', errors.READ_TIMEOUT
        end
        if scenario == 'all_closed' or scenario == 'initial_refresh_closed' or
           (scenario == 'stale' and host ~= 'replacement') then
          return nil, 'closed'
        end
        if scenario == 'always_unprepared' then return nil, 'unprepared', errors.UNPREPARED end
        if scenario == 'closed' and host == 'stopping' then return nil, 'closed' end
        if scenario == 'reprepare_closed' or scenario == 'reprepare' then
          local id = request.queries and request.queries[1][3].query_id or request.query_id
          if id ~= 'new-id' then return nil, 'unprepared', errors.UNPREPARED end
          if not request.queries then assert(request.result_metadata_id == 'new-metadata') end
        end
        assert(keyspace == expected_keyspace, 'retry lost coordinator keyspace options')
        if scenario == 'batch_partial' then
          assert(request.queries[1][3].query_id == 'first-id')
          assert(request.queries[2][3].query_id == 'second-id')
        end
        return { type = 'VOID' }
      end,
    }
  end,
}

local Cluster = require 'resty.cassandra.cluster'
local function new_cluster()
  data = {
    stopping = true, healthy = true,
    ['host:rec:stopping'] = '0:0:0:0:',
    ['host:rec:healthy'] = '0:0:0:0:',
  }
  closed, pooled, attempts, preparations, locks = {}, {}, {}, {}, {}
  reads, fault = 0, nil
  clock, expirations, flags = 1, {}, {}
  unlock_failure, unlocks, connections, prepare_queries = false, 0, {}, {}
  local cluster = assert(Cluster.new { keyspace = 'default', silent = false })
  cluster.topo_ver = 1
  cluster.lb_policy:init({ { host = 'stopping' }, { host = 'healthy' } })
  -- Always start at the stopping node to verify per-request exclusion as well
  -- as the shared DOWN flag, independently of round-robin's starting offset.
  cluster.lb_policy.iter = function(self) return ipairs(self.peers) end
  cluster.refresh = function() error('retry must not refresh topology') end
  return cluster
end

for _, operation in ipairs({ 'execute', 'batch' }) do
  for _, options in ipairs({ { keyspace = 'tenant' }, { no_keyspace = true } }) do
    for _, mode in ipairs({ 'closed', 'prepare_closed', 'reprepare_closed', 'reprepare' }) do
      scenario = mode
      expected_keyspace = options.keyspace
      local cluster = new_cluster()
      local query = 'SELECT * FROM items'
      if mode == 'reprepare_closed' or mode == 'reprepare' then
        cluster.prepared_ids[query] = { query_id = 'old-id' }
      end
      for i = 1, 3 do
        local result, err
        local opts = { prepared = mode ~= 'closed' }
        if operation == 'execute' then
          result, err = cluster:execute(query, nil, opts, options)
        else
          result, err = cluster:batch({ { query } }, opts, options)
        end
        assert(result, err)
        assert(next(locks) == nil, 'preparation lock leaked')
      end
      if mode ~= 'reprepare' then
        assert(data.stopping == false, 'failed node must be marked down')
        assert(closed.stopping and not pooled.stopping, 'failed connection must be closed')
        assert(pooled.healthy, 'healthy connection should be reusable')
      end
    end
  end
end

expected_keyspace = 'tenant'
for _, mode in ipairs({ 'overloaded', 'timeout', 'always_unprepared', 'invalid' }) do
  scenario = mode
  local cluster = new_cluster()
  local refreshes = 0
  cluster.refresh = function()
    refreshes = refreshes + 1
    assert(refreshes == 1, 'more than one recovery refresh')
    return true
  end
  local result, err = cluster:execute('SELECT * FROM items', nil,
    { prepared = mode == 'invalid' or mode == 'always_unprepared' }, { keyspace = 'tenant' })
  assert(not result and err)
  if mode == 'invalid' then
    assert(#attempts == 0 and #preparations == 1, 'invalid CQL must not fail over')
  elseif mode == 'always_unprepared' then
    assert(#attempts == 2, 're-preparation must be bounded')
  elseif mode == 'timeout' then
    assert(#attempts == 4, 'timeout retries must honor the policy budget')
  else
    assert(table.concat(attempts, ',') == 'stopping,healthy', 'each host must be tried once')
  end
  assert(refreshes == 0, 'reachable hosts must not trigger topology refresh')
  assert(next(locks) == nil)
end

-- Refresh after exhaustion can recover replacement addresses, but cannot
-- retry old addresses or refresh again if the replacement also fails.
for _, operation in ipairs({ 'execute', 'batch' }) do
  for _, mode in ipairs({ 'stale', 'prepare_stale', 'connect_stale', 'initial_refresh_closed',
                         'all_closed', 'refresh_failed', 'unchanged' }) do
    scenario = (mode == 'refresh_failed' or mode == 'unchanged') and 'all_closed' or mode
    local cluster = new_cluster()
    local refreshes = 0
    cluster.refresh = function(self)
      refreshes = refreshes + 1
      assert(refreshes == 1, 'more than one recovery refresh')
      if mode == 'refresh_failed' then return nil, 'contact points unavailable' end
      -- Even if refresh marks old hosts UP, this request must skip them.
      data.stopping, data.healthy = true, true
      local peers = { { host = 'stopping' }, { host = 'healthy' } }
      if mode ~= 'unchanged' then
        data.replacement = true
        data['host:rec:replacement'] = '0:0:0:0:'
        peers[#peers + 1] = { host = 'replacement' }
      end
      self.lb_policy:init(peers)
      return true
    end
    local opts = { prepared = mode == 'prepare_stale' }
    local result, err
    if operation == 'execute' then
      result, err = cluster:execute('SELECT * FROM items', nil, opts, { keyspace = 'tenant' })
    else
      result, err = cluster:batch({ { 'SELECT * FROM items' } }, opts, { keyspace = 'tenant' })
    end
    assert(refreshes == 1)
    if mode == 'refresh_failed' then
      assert(not result and err:find('topology refresh failed: contact points unavailable', 1, true))
    elseif mode == 'all_closed' or mode == 'unchanged' or mode == 'initial_refresh_closed' then
      assert(not result and err)
    else
      assert(result, err)
    end
    local seen = {}
    for _, host in ipairs(attempts) do
      assert(not seen[host], 'refresh retried an attempted host')
      seen[host] = true
    end
    seen = {}
    for _, host in ipairs(connections) do
      assert(not seen[host], 'refresh reconnected to an attempted address')
      seen[host] = true
    end
    if mode == 'all_closed' then assert(seen.replacement) end
    assert(next(locks) == nil)
  end
end

-- A partially prepared batch must finish preparing before any batch is sent.
for _, forced in ipairs({ false, true }) do
  local cluster = new_cluster()
  scenario = 'batch_partial'
  if forced then
    -- Exercise the UNPREPARED path directly with stale IDs for both entries.
    local request = {
      retries = 0, queries = { { 'first', nil, { query_id = 'old' } }, { 'second', nil, { query_id = 'old' } } },
      coordinator_options = { keyspace = 'tenant' },
    }
    local coordinator = assert(cluster:next_coordinator(request.coordinator_options))
    assert(cluster:handle_error('unprepared', errors.UNPREPARED, coordinator, request))
    assert(table.concat(prepare_queries, ',') == 'stopping:first,stopping:second,healthy:first,healthy:second')
  else
    assert(cluster:batch({ { 'first' }, { 'second' } }, { prepared = true }, { keyspace = 'tenant' }))
    assert(table.concat(prepare_queries, ',') == 'stopping:first,stopping:second,healthy:second')
  end
  assert(table.concat(attempts, ',') == 'healthy')
  assert(next(locks) == nil and data.stopping == false)
end

-- Acquisition failures never unlock an unowned lock; cleanup errors retain
-- the original failure and its protocol/transport classification.
for _, failure in ipairs({ 'lock', 'unlock', 'read_unlock', 'prepare_unlock', 'no memory' }) do
  local cluster = new_cluster()
  scenario = failure == 'prepare_unlock' and 'invalid' or 'reprepare'
  fault = failure == 'read_unlock' and 'read' or failure
  unlock_failure = failure == 'unlock' or failure == 'read_unlock' or failure == 'prepare_unlock'
  local coordinator = assert(cluster:next_coordinator({ keyspace = 'tenant' }))
  local result, err, code, prepare_err = cluster:get_or_prepare(coordinator, 'test')
  if failure == 'no memory' then
    assert(result and result.query_id == 'new-id' and not err)
    assert(cluster.prepared_ids.test == result and next(locks) == nil)
  else
    assert(not result and err:find('injected', 1, true))
    if failure == 'read_unlock' then assert(err:find('injected read failure', 1, true)) end
    if failure == 'prepare_unlock' then
      assert(err:find('invalid query', 1, true) and code == errors.INVALID and prepare_err == nil)
    end
  end
  assert(unlocks == (failure == 'lock' and 0 or 1))
end

-- Cover the exported selector without request tracking and the DOWN peer
-- diagnostic with no recorded connection error.
do
  local cluster = new_cluster()
  scenario = 'reprepare'
  cluster.logging = false
  data.stopping = false
  data['host:rec:stopping'] = '1000:1000:0:0:'
  assert(cluster:next_coordinator_with_refresh({ keyspace = 'tenant' }).host == 'healthy')
  assert(cluster:next_coordinator({ keyspace = 'tenant' }).host == 'healthy')
end

-- Internal state failures must still propagate without topology recovery.
do
  local cluster = new_cluster()
  data.stopping = false
  data['host:rec:stopping'] = false
  local result, err = cluster:execute('SELECT * FROM items')
  assert(not result and err == 'corrupted shm')
end

for _, failure in ipairs({ 'read', 'write' }) do
  scenario = 'reprepare'
  local cluster = new_cluster()
  fault = failure
  local result, err = cluster:execute('SELECT * FROM items', nil, { prepared = true }, { keyspace = 'tenant' })
  assert(not result and err:find('injected', 1, true))
  assert(next(locks) == nil, 'shared dictionary failure leaked preparation lock')
end

for _, mode in ipairs({ 'timeout', 'prepare_timeout' }) do
  scenario = mode
  local cluster = new_cluster()
  cluster.retry_on_timeout = false
  local result, err = cluster:execute('SELECT * FROM items', nil,
    { prepared = mode == 'prepare_timeout' }, { keyspace = 'tenant' })
  assert(not result and err == 'timeout', 'retry_on_timeout=false must be respected')
  assert(#attempts + #preparations == 1 and closed.stopping and not closed.healthy)
  assert(next(locks) == nil)
end

-- Interleaved iterators must not share mutable cursors, even across refresh.
for _, name in ipairs({ 'rr', 'dc_rr' }) do
  local policy = require('resty.cassandra.policies.lb.' .. name).new('dc1')
  policy:init({ { host = 'a', data_center = 'dc1' }, { host = 'b', data_center = 'dc1' } })
  local step, state, index = policy:iter()
  local first
  index, first = step(state, index)
  local other, other_state, other_index = policy:iter()
  other(other_state, other_index)
  policy:init({ { host = 'c', data_center = 'dc1' } })
  local second
  index, second = step(state, index)
  assert(first.host ~= second.host and second.host ~= 'c', 'iterator cursor was shared')
  assert(step(state, index) == nil)
  policy:init({})
  step, state, index = policy:iter()
  assert(step(state, index) == nil)
end

-- DC-aware cursors must isolate both local and remote progress, preserve
-- local-first ordering, and work with either tier empty.
for _, local_count in ipairs({ 0, 2 }) do
  local policy = require('resty.cassandra.policies.lb.dc_rr').new('dc1')
  local peers = { { host = 'r1', data_center = 'dc2' }, { host = 'r2', data_center = 'dc2' } }
  for i = 1, local_count do peers[#peers + 1] = { host = 'l' .. i, data_center = 'dc1' } end
  policy:init(peers)
  local step, state, index = policy:iter()
  local seen = {}
  for i = 1, #peers do
    local peer
    index, peer = step(state, index)
    assert(peer and not seen[peer.host])
    seen[peer.host] = true
    assert(peer.data_center == (i <= local_count and 'dc1' or 'dc2'))
    for _ in policy:iter() do end
  end
  assert(step(state, index) == nil)
end
-- A policy-approved transient error can recover on the only reachable host.
-- Exhausting that policy must preserve the server error and protocol code.
for _, operation in ipairs({ 'execute', 'batch' }) do
  for _, mode in ipairs({ 'transient_read', 'transient_write', 'transient_unavailable', 'read_timeout', 'overloaded' }) do
    local cluster = new_cluster()
    scenario = mode
    cluster.lb_policy:init({ { host = 'stopping' } })
    cluster.retry_policy.on_unavailable = function(_, request) return request.retries < 1 end
    local result, err, code
    if operation == 'execute' then
      result, err, code = cluster:execute('test', nil, nil, { keyspace = 'tenant' })
    else
      result, err, code = cluster:batch({ { 'test' } }, nil, { keyspace = 'tenant' })
    end
    if mode == 'read_timeout' then
      assert(not result and err == 'read timeout' and code == errors.READ_TIMEOUT)
      assert(#attempts == 4)
    elseif mode == 'overloaded' then
      assert(not result and err:find('overloaded', 1, true) and code == errors.OVERLOADED)
      assert(#attempts == 1)
    else
      assert(result, err)
      assert(table.concat(attempts, ',') == 'stopping,stopping')
    end
  end
end

-- Revisiting responsive hosts must not reintroduce a failed connection.
do
  local cluster = new_cluster()
  scenario = 'closed_then_read'
  local result, err, code = cluster:execute('test', nil, nil, { keyspace = 'tenant' })
  assert(not result and err == 'read timeout' and code == errors.READ_TIMEOUT)
  assert(table.concat(attempts, ',') == 'stopping,healthy,healthy,healthy')
end

-- A lock cleanup failure is terminal, even when the original PREPARE error
-- would normally cause failover. Exercise the public query and batch paths.
for _, operation in ipairs({ 'execute', 'batch' }) do
  for _, mode in ipairs({ 'invalid', 'prepare_closed' }) do
    local cluster = new_cluster()
    scenario, unlock_failure = mode, true
    local result, err, code
    if operation == 'execute' then
      result, err, code = cluster:execute('cleanup failure', nil, { prepared = true }, { keyspace = 'tenant' })
    else
      result, err, code = cluster:batch({ { 'cleanup failure' } }, { prepared = true }, { keyspace = 'tenant' })
    end
    assert(not result and err:find('injected unlock failure', 1, true))
    assert(err:find(mode == 'invalid' and 'invalid query' or 'closed', 1, true))
    assert(code == (mode == 'invalid' and errors.INVALID or nil))
    assert(#preparations == 1 and #attempts == 0 and closed.stopping and not pooled.stopping)
  end
end

-- A configured DNS name can be probed once after refresh even while its old
-- endpoint is in backoff. This allowance never bypasses maintenance mode.
for _, mode in ipairs({ 'dns_recover', 'dns_connect', 'dns_closed', 'maintenance' }) do
  local cluster = new_cluster()
  scenario = mode == 'maintenance' and 'dns_closed' or mode
  cluster.contact_points = { 'stopping' }
  cluster.lb_policy:init({ { host = 'stopping' } })
  local refreshes = 0
  cluster.refresh = function()
    refreshes = refreshes + 1
    assert(refreshes == 1)
    if mode == 'maintenance' then data['host:maintenance:stopping'] = true end
    return true
  end
  local result, err = cluster:execute('test', nil, nil, { keyspace = 'tenant' })
  assert(refreshes == 1)
  if mode == 'dns_recover' or mode == 'dns_connect' then
    assert(result, err)
    assert(data.stopping == true and #connections == 2)
  else
    assert(not result and err:find('closed', 1, true))
    assert(#connections == (mode == 'maintenance' and 1 or 2))
  end
end

-- Literal IPs are never eligible for the DNS retry allowance.
for _, host in ipairs({ '127.0.0.1', '::1' }) do
  local cluster = new_cluster()
  scenario = 'dns_closed'
  cluster.contact_points = { host }
  data[host], data['host:rec:' .. host] = true, '0:0:0:0:'
  cluster.lb_policy:init({ { host = host } })
  cluster.refresh = function() return true end
  local result, err = cluster:execute('test', nil, nil, { keyspace = 'tenant' })
  assert(not result and err:find('closed', 1, true) and #connections == 1)
end

-- A selection/storage failure is not exhaustion: surface it instead of
-- replacing it with the preceding server error or attempting a refresh.
for _, revisit in ipairs({ false, true }) do
  local cluster = new_cluster()
  data.stopping, data['host:rec:stopping'] = false, false
  local result, err, code = cluster:send_retry({
    retries = 0, last_error = 'previous timeout', last_cql_code = errors.READ_TIMEOUT,
    tried_hosts = revisit and { stopping = true, healthy = true } or {},
    retry_same_hosts = revisit,
  })
  assert(not result and err == 'corrupted shm' and code == nil)
end
-- Refresh a single moved node while seven peers are still healthy. Use the
-- real topology refresh against fake system-table responses and shared memory.
-- DNS/network I/O is simulated; configured CNAME aliases stay unchanged.
do
  local cluster = new_cluster()
  scenario = 'background_move'
  local peers, aliases = {}, {}
  discovered_peers = {}
  for i = 1, 8 do
    local host = '10.0.0.' .. i
    peers[i] = { host = host }
    aliases[i] = 'cassandra-' .. i .. '.lotusflare.svc.cluster.local'
    data[host], data['host:rec:' .. host] = true, '0:0:0:0:'
    if i > 1 then
      discovered_peers[#discovered_peers + 1] = { peer = host, rpc_address = host, data_center = 'dc1' }
    end
  end
  cluster.contact_points = aliases
  cluster.refresh = Cluster.refresh
  cluster.lb_policy:init(peers)
  assert(cluster:set_peers(1, peers, 4))
  shm:set('topo:latest', 1, 0, 1)
  local timers = {}
  ngx.timer = { at = function(delay, fn, client)
    assert(delay == 0)
    timers[#timers + 1] = { fn, client }
    return true
  end }
  assert(cluster:execute('test', nil, nil, { keyspace = 'tenant' }))
  assert(table.concat(attempts, ',') == '10.0.0.1,10.0.0.2')
  assert(#timers == 1 and cluster.topo_ver == 1, 'query must not wait for discovery')
  -- A different worker/client shares the same throttle.
  local other = assert(Cluster.new { silent = true })
  assert(other:set_peer_down('10.0.0.1', 'closed'))
  assert(#timers == 1)
  timers[1][1](false, timers[1][2])
  assert(cluster.topo_ver == 2 and cluster.contact_points == aliases)
  local refreshed = assert(cluster:get_peers(2))
  assert(#refreshed == 8)
  local seen = {}
  for _, peer in ipairs(refreshed) do seen[peer.host] = true end
  assert(seen['10.0.1.1'] and not seen['10.0.0.1'])
  local resolved = false
  for _, host in ipairs(connections) do if host == aliases[1] then resolved = true end end
  assert(resolved, 'discovery must connect through configured DNS aliases')
  -- The replacement is eligible for subsequent queries.
  cluster.lb_policy:init({ { host = '10.0.1.1' } })
  assert(cluster:execute('test', nil, nil, { keyspace = 'tenant' }))
  assert(attempts[#attempts] == '10.0.1.1')
  assert(other:set_peer_down('10.0.0.1', 'closed'))
  assert(#timers == 1, 'completion must not clear the throttle')
  clock = clock + 5
  assert(other:set_peer_down('10.0.0.1', 'closed'))
  assert(#timers == 2, 'throttle must expire')
  ngx.timer = nil
end

-- Timer lifecycle and errors never turn a successful query failover into an
-- application error, and callback failures must not recursively enqueue work.
for _, mode in ipairs({ 'timer_error', 'throttle_error', 'premature', 'refresh_error', 'refresh_throw',
                       'silent', 'connect_failure', 'prepare_failure' }) do
  local cluster = new_cluster()
  scenario = 'closed'
  if mode == 'connect_failure' then scenario = 'background_connect' end
  if mode == 'prepare_failure' then scenario = 'prepare_closed' end
  local queued, calls = {}, 0
  ngx.timer = { at = function(_, fn, client)
    if mode == 'timer_error' then return nil, 'too many pending timers' end
    queued[#queued + 1] = { fn, client }
    return true
  end }
  if mode == 'throttle_error' then fault = 'throttle' end
  if mode == 'silent' then cluster.logging = false end
  cluster.refresh = function(self)
    calls = calls + 1
    assert(self:set_peer_down('stopping', 'closed'))
    if mode == 'refresh_throw' then error('injected refresh exception') end
    return nil, 'injected refresh failure'
  end
  assert(cluster:execute('test', nil, { prepared = mode == 'prepare_failure' }, { keyspace = 'tenant' }))
  if mode == 'timer_error' or mode == 'throttle_error' then
    assert(#queued == 0 and not data['refresh:background'])
  else
    assert(#queued == 1)
    queued[1][1](mode == 'premature', queued[1][2])
    assert(calls == (mode == 'premature' and 0 or 1))
    assert(#queued == 1 and not cluster.refreshing_in_background)
  end
  ngx.timer = nil
end

-- Maintenance is an explicit operator action, not a connection failure.
do
  local cluster = new_cluster()
  ngx.timer = { at = function() error('maintenance must not schedule discovery') end }
  assert(cluster:set_peer_maintenance('stopping', true) == nil)
  ngx.timer = nil
end
print('failover checks passed')
