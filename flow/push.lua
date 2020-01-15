script = """
local l = redis.call('llen', KEYS[1])
if l < 1 then
    redis.call('lpush', KEYS[2])
end
return -1
"""
