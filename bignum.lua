-- Atomic big number operations for Redis
local function add(a, b)
    local result = {}
    local carry = 0
    local i = 1
    local base = 1000000000  -- 10^9, large base to minimize array size
    
    -- Convert string to array of digits in base 10^9
    local function to_digits(str)
        local digits = {}
        local len = string.len(str)
        local pos = len
        while pos > 0 do
            local take = math.min(9, pos)
            local start = pos - take + 1
            digits[#digits + 1] = tonumber(string.sub(str, start, pos))
            pos = pos - take
        end
        return digits
    end
    
    -- Convert array of base 10^9 digits back to string
    local function to_string(digits)
        if #digits == 0 then return "0" end
        local parts = {}
        parts[1] = tostring(digits[#digits])
        for i = #digits - 1, 1, -1 do
            parts[#parts + 1] = string.format("%09d", digits[i])
        end
        return table.concat(parts)
    end
    
    local num1 = to_digits(a)
    local num2 = to_digits(b)
    local max_len = math.max(#num1, #num2)
    
    for i = 1, max_len do
        local sum = (num1[i] or 0) + (num2[i] or 0) + carry
        carry = math.floor(sum / base)
        result[i] = sum % base
    end
    if carry > 0 then
        result[max_len + 1] = carry
    end
    
    return to_string(result)
end

-- Get the key and increment value from Redis KEYS and ARGV
local key = KEYS[1]
local increment = ARGV[1]

-- Get current value or initialize to 0
local current = redis.call('GET', key) or '0'

-- Add the increment to current value
local new_value = add(current, increment)

-- Store and return the result
redis.call('SET', key, new_value)
return new_value 