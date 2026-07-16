--- Request, datacenter and rack-aware round robin load balancing policy for OpenResty.
-- This policy will try to reuse the same node for the lifecycle of a given
-- request if possible. It is mostly designed for use in OpenResty
-- environments.
--
-- This extends the request and datacenter-aware policy with rack preference
-- (ideal for cloud AZs which map to Cassandra racks).
--
-- Priority order: same rack in local DC > other racks in local DC > remote DCs.
--
-- If local_rack is not provided, this behaves identically to req_dc_rr.
-- @module resty.cassandra.policies.lb.req_dc_rack_rr

local cluster = require "resty.cassandra.cluster"
local _M = require('resty.cassandra.policies.lb').new_policy('req_dc_rack_aware_round_robin')

local past_init

--- Create a request, DC and rack-aware round robin policy.
-- @usage
-- local Cluster = require "resty.cassandra.cluster"
-- local req_dc_rack_rr = require "resty.cassandra.policies.lb.req_dc_rack_rr"
--
-- local policy = req_dc_rack_rr.new("my_dc", "my_rack")
-- local cluster = assert(Cluster.new {
--   lb_policy = policy
-- })
--
-- @param[type=string] local_dc Name of the local/closest datacenter.
-- @param[type=string] local_rack (optional) Name of the local rack/AZ.
-- @treturn table `policy`: A DC and rack-aware round robin policy.
function _M.new(local_dc, local_rack)
  assert(type(local_dc) == 'string', 'local_dc must be a string')
  if local_rack ~= nil then
    assert(type(local_rack) == 'string', 'local_rack must be a string')
  end

  local self = _M.super.new()
  self.local_dc = local_dc
  self.local_rack = local_rack
  return self
end

function _M:init(peers)
  local same_rack, local_other_rack, remote = {}, {}, {}

  for i = 1, #peers do
    local peer = peers[i]
    if type(peer.data_center) ~= 'string' then
      ngx.log(ngx.WARN, cluster._log_prefix, 'peer ', peer.host,
              ' has no data_center field in shm, considering it remote')
      peer.data_center = nil
    end
    if type(peer.rack) ~= 'string' then
      peer.rack = nil
    end

    if self.local_dc and peer.data_center == self.local_dc then
      if self.local_rack and peer.rack == self.local_rack then
        same_rack[#same_rack + 1] = peer
      else
        local_other_rack[#local_other_rack + 1] = peer
      end
    else
      remote[#remote + 1] = peer
    end
  end

  self.start_same_idx = -2
  self.start_local_other_idx = -2
  self.start_remote_idx = -2
  self.same_rack_peers = same_rack
  self.local_other_rack_peers = local_other_rack
  self.remote_peers = remote
end

local function advance_peer(state)
  if state.same_tried < #state.same_rack_peers then
    state.same_tried = state.same_tried + 1
    state.same_idx = state.same_idx + 1
    local peer = state.same_rack_peers[(state.same_idx % #state.same_rack_peers) + 1]
    if state.ctx then
      state.ctx.cassandra_coordinator = peer
    end
    return peer

  elseif state.local_other_tried < #state.local_other_rack_peers then
    state.local_other_tried = state.local_other_tried + 1
    state.local_other_idx = state.local_other_idx + 1
    local peer = state.local_other_rack_peers[(state.local_other_idx % #state.local_other_rack_peers) + 1]
    if state.ctx then
      state.ctx.cassandra_coordinator = peer
    end
    return peer

  elseif state.remote_tried < #state.remote_peers then
    state.remote_tried = state.remote_tried + 1
    state.remote_idx = state.remote_idx + 1
    return state.remote_peers[(state.remote_idx % #state.remote_peers) + 1]
  end
end

local function next_peer(state, i)
  i = i + 1

  if i == 1 and state.initial_cassandra_coordinator then
    return i, state.initial_cassandra_coordinator
  end

  local peer = advance_peer(state)
  if not peer then
    return nil
  end

  if peer == state.initial_cassandra_coordinator then
    peer = advance_peer(state)
    if not peer then
      return nil
    end
  end

  return i + 1, peer
end

function _M:iter()
  self.same_tried = 0
  self.local_other_tried = 0
  self.remote_tried = 0

  if past_init or ngx.get_phase() ~= "init" then
    self.ctx = ngx and ngx.ctx
    past_init = true
  end

  if self.ctx then
    self.initial_cassandra_coordinator = self.ctx.cassandra_coordinator
  end

  if #self.same_rack_peers > 0 then
    self.same_idx = (self.start_same_idx % #self.same_rack_peers) + 1
  end
  if #self.local_other_rack_peers > 0 then
    self.local_other_idx = (self.start_local_other_idx % #self.local_other_rack_peers) + 1
  end
  if #self.remote_peers > 0 then
    self.remote_idx = (self.start_remote_idx % #self.remote_peers) + 1
  end

  self.start_same_idx = self.start_same_idx + 1
  self.start_local_other_idx = self.start_local_other_idx + 1
  self.start_remote_idx = self.start_remote_idx + 1

  return next_peer, self, 0
end

return _M
