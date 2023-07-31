local fio = require('fio')

local t = require('luatest')
local g = t.group()

local helpers = require('test.helper')


local function init_remote_funcs(servers, fn_names, fn_body)
    for _, server in pairs(servers) do
        for _, fn_name in ipairs(fn_names) do
            local eval_str = ('%s = %s'):format(fn_name, fn_body)
            server:eval(eval_str)
        end
    end
end

local function cleanup_log_data()
    g.s1:eval([[
        _G.__log_warn = {}
        _G.__log_error = {}
    ]])
end

local function call_twophase(server, arg)
    return server:eval([[
        local twophase = require('cartridge.twophase')
        return twophase.twophase_commit(...)
    ]], {arg})
end

local function rewind_2pc_options(server)
    server:exec(function()
        _G.__rewind_2pc_options()
    end)
end

local function force_reapply()
    g.s1:exec(function()
        local topology = require('cartridge.confapplier').get_readonly('topology')
        local uuids = require('fun').iter(topology.servers):totable()
        require('cartridge.twophase').force_reapply(uuids)
    end)
end

local function start_high_load(server, fibers, sleep)
    server:eval([[
        local fiber = require('fiber')
        local log = require('log')
        local ffi = require('ffi')
        local fibers_count, timeout = ...
        ffi.cdef("int poll(struct pollfd *fds, unsigned long nfds, int timeout);")
        local fibers = {}
        for i = 1, fibers_count do
            local fb = fiber.create(function()
                while true do
                    ffi.C.poll(box.NULL, 0, timeout) -- blocks fiber for the specified amount of ms
                    fiber.yield()
                end
            end)
            table.insert(fibers, fb)
        end
        _G.__cancel_fibers  = function()
           for i, f in ipairs(fibers) do
               f:cancel()
           end
        end]], {fibers, sleep})
end

local REWIND_2PC_OPTIONS_FUNC_BODY = [[
        local twophase = require('cartridge.twophase')
        local t1 = twophase.get_netbox_call_timeout()
        local t2 = twophase.get_upload_config_timeout()
        local t3 = twophase.get_validate_config_timeout()
        local t4 = twophase.get_apply_config_timeout()
        local v1 = twophase.get_abort_method()
        _G.__rewind_2pc_options = function()
            twophase.set_netbox_call_timeout(t1)
            twophase.set_upload_config_timeout(t2)
            twophase.set_validate_config_timeout(t3)
            twophase.set_apply_config_timeout(t4)
            twophase.set_abort_method(v1)
        end]]

local function stop_high_load(server)
    server:exec(function() pcall(_G.__cancel_fibers) end)
end

g.before_all(function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_loghack'),
        cookie = helpers.random_cookie(),
        replicasets = {{
            alias = 'main',
            roles = {},
            servers = 2
        }}
    })
    g.cluster:start()

    g.s1 = g.cluster:server('main-1')
    g.s2 = g.cluster:server('main-2')

    g.two_phase_funcs = {'_G.__prepare', '_G.__abort', '_G.__commit'}
    g.simple_stage_func_good = [[function(data) return true end]]
    g.simple_stage_func_bad = [[function()
        return nil, require('errors').new('Err', 'Error occured')
    end]]

    g.s1:eval(REWIND_2PC_OPTIONS_FUNC_BODY)
    g.s2:eval(REWIND_2PC_OPTIONS_FUNC_BODY)
end)

g.after_all(function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end)

g.before_each(function()
    rewind_2pc_options(g.s1)
    rewind_2pc_options(g.s2)
    force_reapply()
    init_remote_funcs(g.cluster.servers, g.two_phase_funcs, g.simple_stage_func_good)
    cleanup_log_data()
end)


function g.test_errors()
    t.assert_error_msg_contains(
        'bad argument opts.fn_abort to nil' ..
        ' (string expected, got nil)',
        call_twophase, g.s1, {
            uri_list = {},
            fn_prepare = '_G.undefined',
            fn_commit = '_G.undefined',
            fn_abort = nil,
        }
    )

    t.assert_error_msg_contains(
        'bad argument opts.fn_commit to nil' ..
        ' (string expected, got nil)',
        call_twophase, g.s1, {
            uri_list = {},
            fn_prepare = '_G.undefined',
            fn_commit = nil,
            fn_abort = '_G.undefined',
        }
    )

    t.assert_error_msg_contains(
        'bad argument opts.fn_prepare to nil' ..
        ' (string expected, got nil)',
        call_twophase, g.s1, {
            uri_list = {},
            fn_prepare = nil,
            fn_commit = '_G.undefined',
            fn_abort = '_G.undefined',
        }
    )

    t.assert_error_msg_contains(
        'bad argument opts.uri_list to twophase_commit' ..
        ' (contiguous array of strings expected)',
        call_twophase, g.s1, {
            uri_list = {k = 'v'},
            fn_prepare = '_G.undefined',
            fn_commit = '_G.undefined',
            fn_abort = '_G.undefined',
        }
    )

    t.assert_error_msg_contains(
        'bad argument opts.uri_list to twophase_commit' ..
        ' (duplicates are prohibited)',
        call_twophase, g.s1, {
            uri_list = {'localhost:13301', 'localhost:13301'},
            fn_prepare = '_G.undefined',
            fn_commit = '_G.undefined',
            fn_abort = '_G.undefined',
        }
    )

    local ok, err = call_twophase(g.s1, {
        uri_list = {'localhost:13301'},
        fn_prepare = '_G.undefined',
        fn_commit = '_G.undefined',
        fn_abort = '_G.undefined',
    })
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'NetboxCallError',
        err = [["localhost:13301": Procedure '_G.undefined' is not defined]],
    })
end

function g.test_success()
    local ok, err = call_twophase(g.s1, {
        uri_list = {'localhost:13302'},
        fn_prepare = '_G.__prepare',
        fn_commit = '_G.__commit',
        fn_abort = '_G.__abort',
        upload_data = {'xyz'},
    })
    t.assert_equals({ok, err}, {true, nil})
    t.assert_equals(g.s1:eval('return _G.__log_error'), {})
    t.assert_equals(g.s1:eval('return _G.__log_warn'), {
        "(2PC) twophase_commit upload phase...",
        "(2PC) twophase_commit prepare phase...",
        "Prepared for twophase_commit at localhost:13302",
        "(2PC) twophase_commit commit phase...",
        "Committed twophase_commit at localhost:13302",
    })

    local function get_inbox()
        local upload = require('cartridge.upload')
        local _, data = next(upload.inbox)
        table.clear(upload.inbox)
        return data
    end
    t.assert_equals(helpers.run_remotely(g.s1, get_inbox), nil)
    t.assert_equals(helpers.run_remotely(g.s2, get_inbox), {'xyz'})
end

function g.test_upload_skipped()
    g.s1:eval([[
        _G.__prepare = function(data)
            assert(data == nil)
            return true
        end
    ]])

    local ok, err = call_twophase(g.s1, {
        activity_name = 'my_2pc',
        uri_list = {'localhost:13301', 'localhost:13302'},
        fn_prepare = '_G.__prepare',
        fn_commit = '_G.__commit',
        fn_abort = '_G.__abort',
    })
    t.assert_equals({ok, err}, {true, nil})
    t.assert_equals(g.s1:eval('return _G.__log_error'), {})
    t.assert_equals(g.s1:eval('return _G.__log_warn'), {
        "(2PC) my_2pc prepare phase...",
        "Prepared for my_2pc at localhost:13301",
        "Prepared for my_2pc at localhost:13302",
        "(2PC) my_2pc commit phase...",
        "Committed my_2pc at localhost:13301",
        "Committed my_2pc at localhost:13302",
    })
end

function g.test_prepare_fails()
    local twophase_args = {
        fn_prepare = '_G.__prepare',
        fn_abort = '_G.__abort',
        fn_commit = '_G.__commit',
        uri_list = {'localhost:13301', 'localhost:13302'},
        activity_name = 'simple_twophase'
    }

    init_remote_funcs({g.s2}, {'_G.__prepare'}, g.simple_stage_func_bad)
    local ok, err = call_twophase(g.s1, twophase_args)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'Err',
        err = '"localhost:13302": Error occured',
    })
    t.assert_items_include(g.s1:eval('return _G.__log_warn'), {
        'Aborted simple_twophase at localhost:13301'
    })
    local error_log = g.s1:eval('return _G.__log_error')
    t.assert_str_contains(error_log[1],
        'Error preparing for simple_twophase at localhost:13302'
    )
end

function g.test_commit_fails()
    local twophase_args = {
        fn_prepare = '_G.__prepare',
        fn_abort = '_G.__abort',
        fn_commit = '_G.__commit',
        uri_list = {'localhost:13301', 'localhost:13302'},
        activity_name = 'simple_twophase'
    }

    init_remote_funcs({g.s2}, {'_G.__commit'}, g.simple_stage_func_bad)
    local ok, err = call_twophase(g.s1, twophase_args)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'Err', err = '"localhost:13302": Error occured'
    })
    t.assert_items_include(g.s1:eval('return _G.__log_warn'),{
        'Committed simple_twophase at localhost:13301'
    })
    local error_log = g.s1:eval('return _G.__log_error')
    t.assert_str_contains(error_log[1],
        'Error committing simple_twophase at localhost:13302'
    )
end

function g.test_abort_fails()
    local twophase_args = {
        fn_prepare = '_G.__prepare',
        fn_abort = '_G.__abort',
        fn_commit = '_G.__commit',
        uri_list = {'localhost:13301', 'localhost:13302'},
        activity_name = 'simple_twophase'
    }

    init_remote_funcs({g.s2}, {'_G.__prepare'}, g.simple_stage_func_bad)
    init_remote_funcs({g.s1}, {'_G.__abort'}, g.simple_stage_func_bad)
    local ok, err = call_twophase(g.s1, twophase_args)
    t.assert_equals(ok, nil)
    t.assert_covers(err, {
        class_name = 'Err', err = '"localhost:13302": Error occured'
    })
    local error_log = g.s1:eval('return _G.__log_error')
    t.assert_str_contains(error_log[1],
        'Error preparing for simple_twophase at localhost:13302'
    )
    t.assert_str_contains(error_log[2],
        'Error aborting simple_twophase at localhost:13301'
    )
end

function g.test_timeouts()
    g.s1:exec(function()
        local t = require('luatest')
        local twophase = require('cartridge.twophase')

        twophase.set_netbox_call_timeout(222)
        t.assert_equals(twophase.get_netbox_call_timeout(), 222)

        twophase.set_upload_config_timeout(123)
        t.assert_equals(twophase.get_upload_config_timeout(), 123)

        twophase.set_validate_config_timeout(654)
        t.assert_equals(twophase.get_validate_config_timeout(), 654)

        twophase.set_apply_config_timeout(111)
        t.assert_equals(twophase.get_apply_config_timeout(), 111)
    end)
end

-- immitate the 'preparing instance' error
g.before_test('test_default_abort_method', function()
    g.s1:exec(function()
        require("cartridge.twophase").set_validate_config_timeout(0.001)
    end)
end)

-- by default, we must to keep lock after the commit abortion
function g.test_default_abort_method()
    g.s1:exec(function()
        require('cartridge.twophase').patch_clusterwide({})
    end)

    g.s1:exec(function()
        local t = require('luatest')
        local _, err = require('cartridge.twophase').patch_clusterwide({})
        t.assert(err)
        t.assert_str_icontains(err.err, 'Two-phase commit is locked')
    end)
end

g.before_test('test_join_abort_method', function()
    g.s1:eval(function()
        require('cartridge.twophase').set_validate_config_timeout(0.001)
        require('cartridge.twophase').set_abort_method('join')
    end)
end)


-- Check we don't lock a clusterwide config updates
-- after exceeding of `validate_timeout_config` timeout
-- see https://github.com/tarantool/cartridge/issues/2119
function g.test_join_abort_method()
    g.s1:exec(function()
        require('cartridge.twophase').patch_clusterwide({})
    end)

    rewind_2pc_options(g.s1)

    g.s1:exec(function()
        local t = require('luatest')
        local _, err = require('cartridge.twophase').patch_clusterwide({})
        t.assert_not(err)
    end)
end

g.before_test('test_cancel_abort_method', function()
    g.s1:exec(function()
        require('cartridge.twophase').set_validate_config_timeout(0.001)
        require('cartridge.twophase').set_abort_method('cancel')
    end)
end)

-- Check we don't lock a clusterwide config updates
-- after exceeding of `validate_timeout_config` timeout
-- see https://github.com/tarantool/cartridge/issues/2119
function g.test_cancel_abort_method()
    g.s1:exec(function()
        local t = require('luatest')
        local _, err = require('cartridge.twophase').patch_clusterwide({})
        t.assert(err)
    end)

    rewind_2pc_options(g.s1)

    g.s1:exec(function()
        local t = require('luatest')
        local _, err = require('cartridge.twophase').patch_clusterwide({})
        t.assert_not(err)
    end)
end

g.before_test('test_highloaded_abort', function()
    -- decrease timeout for speeding up test
    g.s1:exec(function()
        local twophase = require('cartridge.twophase')
        twophase.set_validate_config_timeout(0.1)
    end)

    -- wrap twophase commit phases:
    -- - preparation phase - to get the fiber's csw
    -- - abort phase - to calculate the duration
    g.s2:eval([[
        _G.__abort_dur = 0
        _G.__prep_fiber_csw = 0
        local super = _G.__cartridge_clusterwide_config_abort_2pc
        _G.__cartridge_clusterwide_config_abort_2pc = function(...)
            local clock = require('clock')
            local _start = clock.monotonic64() / 1e6
            local ok , res = super(...)
            local _end = clock.monotonic64() / 1e6
            _G.__abort_dur = _end - _start
            return ok, res
        end
        local super_prep = _G.__cartridge_clusterwide_config_prepare_2pc
        _G.__cartridge_clusterwide_config_prepare_2pc = function(...)
            local f = require('fiber').self()
            local yields = require('fiber').info()[f:id()].csw
            local ok , res = super_prep(...)
            _G.__prep_fiber_csw = require('fiber').info()[f:id()].csw - yields
            return ok, res
        end]])

    start_high_load(g.s2, 10, 1)
end)

g.after_test('test_highloaded_abort', function()
    stop_high_load(g.s2)
end)

function g.test_highloaded_abort()
    -- run the twophase apply without
    -- the interaption of the preparation fiber
    g.s1:exec(function()
        local t = require('luatest')
        require('cartridge.twophase').set_abort_method("join")
        local _, err = require('cartridge.twophase').patch_clusterwide({})
        t.assert(err)
    end)
    local join_abort_duration, join_prep_csw = g.s2:exec(function()
        return tonumber(_G.__abort_dur), _G.__prep_fiber_csw -- luacheck:ignore
    end)

    -- run the twophase apply with the interaption
    -- of the preparation fiber by `fiber.testcancel()`
    g.s1:exec(function()
        local t = require('luatest')
        require('cartridge.twophase').set_abort_method("cancel")
        local _, err = require('cartridge.twophase').patch_clusterwide({})
        t.assert(err)
    end)
    local cancel_abort_duration, cancel_prep_csw = g.s2:exec(function()
        return tonumber(_G.__abort_dur), _G.__prep_fiber_csw -- luacheck:ignore
    end)
    require('log').info('debug')
    require('log').info('debug')
    require('log').info(join_prep_csw)
    require('log').info('debug')
    require('log').info('debug')

    t.assert_lt(cancel_abort_duration, join_abort_duration)
    t.assert_lt(cancel_prep_csw, join_prep_csw)
end
