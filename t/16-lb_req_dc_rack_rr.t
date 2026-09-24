# vim:set ts=4 sw=4 et fdm=marker:
use lib '.';
use Test::Nginx::Socket::Lua;
use t::Util;

no_long_string();

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: lb_req_dc_rack_rr sanity (3-tier ordering)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_mod = require 'resty.cassandra.policies.lb.req_dc_rack_rr'
            ngx.say(lb_mod.name)

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2', rack = 'rack1'},
                {host = '10.0.0.2', data_center = 'dc2', rack = 'rack2'},

                {host = '127.0.0.1', data_center = 'dc1', rack = 'rack1'},
                {host = '127.0.0.2', data_center = 'dc1', rack = 'rack1'},
                {host = '127.0.0.3', data_center = 'dc1', rack = 'rack2'},
                {host = '127.0.0.4', data_center = 'dc1', rack = 'rack3'},
            }

            local lb = lb_mod.new('dc1', 'rack1')
            ngx.say('local_dc: ', lb.local_dc)
            ngx.say('local_rack: ', lb.local_rack)

            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("1. ", peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("2. ", peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("3. ", peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
req_dc_rack_aware_round_robin
local_dc: dc1
local_rack: rack1

1. 127.0.0.1
1. 127.0.0.2
1. 127.0.0.3
1. 127.0.0.4
1. 10.0.0.1
1. 10.0.0.2

2. 127.0.0.4
2. 127.0.0.2
2. 127.0.0.1
2. 127.0.0.3
2. 10.0.0.2
2. 10.0.0.1

3. 127.0.0.3
3. 127.0.0.1
3. 127.0.0.2
3. 127.0.0.4
3. 10.0.0.1
3. 10.0.0.2
--- no_error_log
[error]



=== TEST 2: lb_req_dc_rack_rr falls back to DC-only when local_rack omitted
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_mod = require 'resty.cassandra.policies.lb.req_dc_rack_rr'

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2', rack = 'rack1'},

                {host = '127.0.0.1', data_center = 'dc1', rack = 'rack1'},
                {host = '127.0.0.2', data_center = 'dc1', rack = 'rack2'},
                {host = '127.0.0.3', data_center = 'dc1', rack = 'rack3'},
            }

            local lb = lb_mod.new('dc1')
            lb:init(peers)

            for i, peer in lb:iter() do
                ngx.say(peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.1
127.0.0.2
127.0.0.3
10.0.0.1
--- no_error_log
[error]



=== TEST 3: lb_req_dc_rack_rr with missing local_dc
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_mod = require 'resty.cassandra.policies.lb.req_dc_rack_rr'
            local lb = lb_mod.new()
        }
    }
--- request
GET /t
--- error_code: 500
--- error_log
local_dc must be a string
--- no_error_log
[crit]



=== TEST 4: lb_req_dc_rack_rr with missing data_center and rack fields
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_mod = require 'resty.cassandra.policies.lb.req_dc_rack_rr'

            local peers = {
                {host = '127.0.0.1', data_center = 'dc1', rack = 'rack1'},
                {host = '127.0.0.2', data_center = 'dc1'},
                {host = '127.0.0.3'},
                {host = '10.0.0.1', data_center = 'dc2', rack = 'rack1'},
            }

            local lb = lb_mod.new('dc1', 'rack1')
            lb:init(peers)

            for i, peer in lb:iter() do
                ngx.say(peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.1
127.0.0.2
127.0.0.3
10.0.0.1
--- error_log eval
qr/\[warn\].*?\[lua-cassandra\] peer 127\.0\.0\.3 has no data_center field in shm, considering it remote/



=== TEST 5: lb_req_dc_rack_rr returns same host first when invoked multiple times (request stickiness)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_mod = require 'resty.cassandra.policies.lb.req_dc_rack_rr'

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2', rack = 'rack1'},

                {host = '127.0.0.1', data_center = 'dc1', rack = 'rack1'},
                {host = '127.0.0.2', data_center = 'dc1', rack = 'rack2'},
                {host = '127.0.0.3', data_center = 'dc1', rack = 'rack1'},
            }

            local lb = lb_mod.new('dc1', 'rack1')
            lb:init(peers)

            for i, peer in lb:iter() do
                ngx.say("1. ", peer.host)
                break
            end

            for i, peer in lb:iter() do
                ngx.say("2. ", peer.host)
                break
            end

            for i, peer in lb:iter() do
                ngx.say("3. ", peer.host)
                break
            end
        }
    }
--- request
GET /t
--- response_body
1. 127.0.0.1
2. 127.0.0.1
3. 127.0.0.1
--- no_error_log
[error]



=== TEST 6: lb_req_dc_rack_rr is resilient when ngx.ctx is 'nil'
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            if rawget(ngx, "ctx") == nil then
                local __ngx_index = getmetatable(ngx)

                setmetatable(ngx, {
                    __index = function(t, k)
                        if k == "ctx" then
                            return
                        end

                        return __ngx_index(t, k)
                    end
                })

            else
                ngx.ctx = nil
            end

            local lb_mod = require 'resty.cassandra.policies.lb.req_dc_rack_rr'
            ngx.say(lb_mod.name)

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2', rack = 'rack1'},

                {host = '127.0.0.1', data_center = 'dc1', rack = 'rack1'},
                {host = '127.0.0.2', data_center = 'dc1', rack = 'rack1'},
                {host = '127.0.0.3', data_center = 'dc1', rack = 'rack2'},
            }

            local lb = lb_mod.new('dc1', 'rack1')
            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("1. ", peer.host)
                break
            end

            for i, peer in lb:iter() do
                ngx.say("2. ", peer.host)
                break
            end

            for i, peer in lb:iter() do
                ngx.say("3. ", peer.host)
                break
            end
        }
    }
--- request
GET /t
--- response_body
req_dc_rack_aware_round_robin

1. 127.0.0.1
2. 127.0.0.2
3. 127.0.0.1
--- no_error_log
[error]



=== TEST 7: lb_req_dc_rack_rr with all peers in same rack (single AZ)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_mod = require 'resty.cassandra.policies.lb.req_dc_rack_rr'

            local peers = {
                {host = '127.0.0.1', data_center = 'dc1', rack = 'rack1'},
                {host = '127.0.0.2', data_center = 'dc1', rack = 'rack1'},
                {host = '127.0.0.3', data_center = 'dc1', rack = 'rack1'},
            }

            local lb = lb_mod.new('dc1', 'rack1')
            lb:init(peers)

            for i, peer in lb:iter() do
                ngx.say(peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.1
127.0.0.2
127.0.0.3
--- no_error_log
[error]
